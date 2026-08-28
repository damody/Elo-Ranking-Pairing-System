use crate::components::QueueMode;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketKey {
    pub region: String,
    pub mode: QueueMode,
    pub index: i32,
}
pub fn bucket_for(region: &str, mode: QueueMode, rating: i32, width: i32) -> BucketKey {
    BucketKey {
        region: region.into(),
        mode,
        index: rating.div_euclid(width.max(1)),
    }
}
pub fn halo(key: &BucketKey, radius: i32) -> Vec<BucketKey> {
    (-radius..=radius)
        .map(|d| BucketKey {
            region: key.region.clone(),
            mode: key.mode,
            index: key.index + d,
        })
        .collect()
}
pub fn search_delta(
    initial: i32,
    step: i32,
    step_seconds: u64,
    maximum: i32,
    enqueued: u64,
    now: u64,
) -> i32 {
    let steps = now.saturating_sub(enqueued) / step_seconds.max(1);
    initial
        .saturating_add(step.saturating_mul(steps.min(i32::MAX as u64) as i32))
        .min(maximum)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expansion_clamps() {
        assert_eq!(search_delta(100, 50, 5, 600, 0, 1000), 600);
        assert_eq!(
            halo(&bucket_for("tw", QueueMode::OneVsOne, 1050, 100), 1).len(),
            3
        );
    }
}
