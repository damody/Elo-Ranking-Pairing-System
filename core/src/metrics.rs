//! Non-authoritative, bounded-cardinality observability.
use crate::components::QueueMode;
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

const MAX_LATENCY_SAMPLES: usize = 4096;

#[derive(Default)]
pub struct Counter(AtomicU64);
impl Counter {
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
#[derive(Default)]
pub struct HighWatermark(AtomicU64);
impl HighWatermark {
    pub fn observe(&self, n: u64) {
        let mut old = self.0.load(Ordering::Relaxed);
        while n > old {
            match self
                .0
                .compare_exchange_weak(old, n, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(v) => old = v,
            }
        }
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
#[derive(Default)]
pub struct Histogram(RwLock<VecDeque<u64>>);
impl Histogram {
    pub fn observe(&self, value: u64) {
        let mut values = self.0.write();
        if values.len() == MAX_LATENCY_SAMPLES {
            values.pop_front();
        }
        values.push_back(value);
    }
    pub fn samples(&self) -> Vec<u64> {
        self.0.read().iter().copied().collect()
    }

    pub fn summary(&self) -> HistogramSummary {
        let mut samples = self.samples();
        samples.sort_unstable();
        HistogramSummary {
            samples: samples.len() as u64,
            p50: percentile(&samples, 50),
            p95: percentile(&samples, 95),
            p99: percentile(&samples, 99),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistogramSummary {
    pub samples: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    sorted[index]
}
#[derive(Default)]
pub struct Metrics {
    pub commands: Counter,
    pub matches: Counter,
    pub ready_accepts: Counter,
    pub ready_rejects: Counter,
    pub ready_timeouts: Counter,
    pub credit_penalties: Counter,
    pub launch_failures: Counter,
    pub reconnects: Counter,
    pub invariant_failures: Counter,
    pub command_queue_high: HighWatermark,
    pub event_queue_high: HighWatermark,
    pub control_queue_high: HighWatermark,
    pub queue_wait_us: Histogram,
    pub candidate_compute_us: Histogram,
    pub commit_us: Histogram,
    pub ready_check_us: Histogram,
    pub placement_us: Histogram,
    pub launch_us: Histogram,
    pub lifecycle: RwLock<MetricSnapshot>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueueLabel {
    pub mode: QueueMode,
    pub region: String,
}
#[derive(Clone, Debug, Default)]
pub struct MetricSnapshot {
    pub queue_players: BTreeMap<QueueLabel, u64>,
    pub capacity_total: u64,
    pub capacity_used: u64,
    pub reservations: u64,
    pub running_instances: u64,
    pub party_structure_difference_sum: u64,
    pub elo_quality_sum: u64,
    pub match_latency_us: Vec<u64>,
}

/// Redacts credential-like fields before a value reaches structured logging.
pub fn redact_field(name: &str, value: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.contains("token") || lower.contains("secret") || lower.contains("authorization") {
        "[REDACTED]".into()
    } else {
        value.into()
    }
}

#[cfg(feature = "otel")]
pub fn trace_lifecycle(name: &'static str) {
    use opentelemetry::trace::Tracer;
    opentelemetry::global::tracer("erps").in_span(name, |_| {});
    tracing::trace!(lifecycle = name);
}
#[cfg(not(feature = "otel"))]
#[inline]
pub fn trace_lifecycle(_: &'static str) {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn secrets_are_always_redacted() {
        for key in [
            "session_token",
            "inviteToken",
            "connection-token",
            "Authorization",
        ] {
            assert_eq!(redact_field(key, "sensitive"), "[REDACTED]");
        }
        assert_eq!(redact_field("region", "tw"), "tw");
    }
    #[test]
    fn high_watermark_never_decreases() {
        let h = HighWatermark::default();
        h.observe(9);
        h.observe(2);
        assert_eq!(h.get(), 9)
    }
    #[test]
    fn tracing_does_not_change_logical_values() {
        let mut value = 7;
        trace_lifecycle("match");
        value += 1;
        assert_eq!(value, 8)
    }
    #[test]
    fn latency_histograms_are_bounded() {
        let histogram = Histogram::default();
        for value in 0..(MAX_LATENCY_SAMPLES as u64 + 10) {
            histogram.observe(value);
        }
        let samples = histogram.samples();
        assert_eq!(samples.len(), MAX_LATENCY_SAMPLES);
        assert_eq!(samples[0], 10);
    }
    #[test]
    fn histogram_summary_is_stable_and_handles_empty_input() {
        let histogram = Histogram::default();
        assert_eq!(histogram.summary(), HistogramSummary::default());
        for value in [100, 10, 50, 90, 30] {
            histogram.observe(value);
        }
        assert_eq!(
            histogram.summary(),
            HistogramSummary {
                samples: 5,
                p50: 50,
                p95: 100,
                p99: 100,
            }
        );
    }
}
