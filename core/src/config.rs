use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, path::Path};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ErpsConfig {
    pub deterministic_seed: u64,
    pub command_queue_capacity: usize,
    pub event_queue_capacity: usize,
    pub control_queue_capacity: usize,
    pub batch_window_ms: u64,
    pub initial_elo_delta: i32,
    pub elo_step: i32,
    pub elo_step_seconds: u64,
    pub maximum_elo_delta: i32,
    pub max_party_rating_spread: i32,
    pub party_size_rating_adjustment: i32,
    pub party_spread_rating_adjustment: i32,
    pub elo_established_k: f64,
    pub elo_provisional_k: f64,
    pub elo_provisional_matches: u32,
    pub elo_maximum_match_delta: i32,
    pub ready_timeout_seconds: u64,
    pub placement_timeout_seconds: u64,
    pub disconnect_grace_seconds: u64,
    pub reject_credit_penalty: u8,
    pub timeout_credit_penalty: u8,
    pub minimum_credit: u8,
    pub credit_suspension_base_seconds: u64,
    pub heartbeat_interval_seconds: u64,
    pub unhealthy_after_missed: u32,
    pub lost_after_seconds: u64,
    pub allow_development_plaintext: bool,
    pub drain_mode: bool,
    pub graceful_shutdown_seconds: u64,
    pub tls_certificate_path: Option<String>,
    pub tls_private_key_path: Option<String>,
    pub server_classes: BTreeMap<String, ServerClassPolicy>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServerClassPolicy {
    pub capacity_limit: u32,
    pub max_instances: u16,
    pub mode_costs: BTreeMap<String, u32>,
}

impl Default for ErpsConfig {
    fn default() -> Self {
        Self {
            deterministic_seed: 0x4552_5053,
            command_queue_capacity: 65_536,
            event_queue_capacity: 1_024,
            control_queue_capacity: 1_024,
            batch_window_ms: 3,
            initial_elo_delta: 100,
            elo_step: 50,
            elo_step_seconds: 5,
            maximum_elo_delta: 600,
            max_party_rating_spread: 600,
            party_size_rating_adjustment: 10,
            party_spread_rating_adjustment: 1,
            elo_established_k: 20.0,
            elo_provisional_k: 40.0,
            elo_provisional_matches: 10,
            elo_maximum_match_delta: 40,
            ready_timeout_seconds: 15,
            placement_timeout_seconds: 15,
            disconnect_grace_seconds: 30,
            reject_credit_penalty: 2,
            timeout_credit_penalty: 5,
            minimum_credit: 60,
            credit_suspension_base_seconds: 60,
            heartbeat_interval_seconds: 2,
            unhealthy_after_missed: 3,
            lost_after_seconds: 10,
            allow_development_plaintext: false,
            drain_mode: false,
            graceful_shutdown_seconds: 10,
            tls_certificate_path: None,
            tls_private_key_path: None,
            server_classes: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("failed to read ERPS config: {0}")]
    Read(String),
    #[error("failed to parse ERPS config: {0}")]
    Parse(String),
    #[error("{0} must be greater than zero")]
    Zero(&'static str),
    #[error("maximum_elo_delta must be >= initial_elo_delta")]
    EloRange,
    #[error("credit values must be in 0..=100")]
    CreditRange,
    #[error("server class {class} max_instances must be in 1..=100")]
    InstanceLimit { class: String },
    #[error("server class {class} must have positive capacity and mode costs")]
    ServerCapacity { class: String },
    #[error("TLS certificate and private key must be configured together")]
    TlsPair,
}

impl ErpsConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|e| ConfigError::Read(e.to_string()))?;
        let config: Self = toml::from_str(&text).map_err(|e| ConfigError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        for (name, value) in [
            ("command_queue_capacity", self.command_queue_capacity),
            ("event_queue_capacity", self.event_queue_capacity),
            ("control_queue_capacity", self.control_queue_capacity),
        ] {
            if value == 0 {
                return Err(ConfigError::Zero(name));
            }
        }
        for (name, value) in [
            ("batch_window_ms", self.batch_window_ms),
            ("elo_step_seconds", self.elo_step_seconds),
            ("ready_timeout_seconds", self.ready_timeout_seconds),
            ("placement_timeout_seconds", self.placement_timeout_seconds),
            ("disconnect_grace_seconds", self.disconnect_grace_seconds),
            (
                "credit_suspension_base_seconds",
                self.credit_suspension_base_seconds,
            ),
            (
                "heartbeat_interval_seconds",
                self.heartbeat_interval_seconds,
            ),
            ("lost_after_seconds", self.lost_after_seconds),
            ("graceful_shutdown_seconds", self.graceful_shutdown_seconds),
        ] {
            if value == 0 {
                return Err(ConfigError::Zero(name));
            }
        }
        if self.initial_elo_delta < 0
            || self.elo_step < 0
            || self.maximum_elo_delta < self.initial_elo_delta
            || self.max_party_rating_spread < 0
        {
            return Err(ConfigError::EloRange);
        }
        if !self.elo_established_k.is_finite()
            || !self.elo_provisional_k.is_finite()
            || self.elo_established_k <= 0.0
            || self.elo_provisional_k <= 0.0
            || self.elo_maximum_match_delta <= 0
        {
            return Err(ConfigError::EloRange);
        }
        if self.minimum_credit > 100
            || self.reject_credit_penalty > 100
            || self.timeout_credit_penalty > 100
        {
            return Err(ConfigError::CreditRange);
        }
        if self.tls_certificate_path.is_some() != self.tls_private_key_path.is_some() {
            return Err(ConfigError::TlsPair);
        }
        for (class, policy) in &self.server_classes {
            if !(1..=100).contains(&policy.max_instances) {
                return Err(ConfigError::InstanceLimit {
                    class: class.clone(),
                });
            }
            if policy.capacity_limit == 0 || policy.mode_costs.values().any(|cost| *cost == 0) {
                return Err(ConfigError::ServerCapacity {
                    class: class.clone(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    #[test]
    fn defaults_are_valid() {
        ErpsConfig::default().validate().unwrap();
    }
    #[test]
    fn zero_queue_is_rejected() {
        let mut c = ErpsConfig::default();
        c.command_queue_capacity = 0;
        assert_eq!(
            c.validate(),
            Err(ConfigError::Zero("command_queue_capacity"))
        );
    }
    #[test]
    fn invalid_instance_limit_is_rejected() {
        let mut c = ErpsConfig::default();
        c.server_classes.insert(
            "x".into(),
            ServerClassPolicy {
                capacity_limit: 10,
                max_instances: 101,
                mode_costs: BTreeMap::from([("1v1".into(), 1)]),
            },
        );
        assert!(matches!(
            c.validate(),
            Err(ConfigError::InstanceLimit { .. })
        ));
    }
    #[test]
    fn zero_mode_cost_is_rejected() {
        let mut c = ErpsConfig::default();
        c.server_classes.insert(
            "x".into(),
            ServerClassPolicy {
                capacity_limit: 10,
                max_instances: 1,
                mode_costs: BTreeMap::from([("1v1".into(), 0)]),
            },
        );
        assert!(matches!(
            c.validate(),
            Err(ConfigError::ServerCapacity { .. })
        ));
    }
    #[test]
    fn tls_pair_is_atomic() {
        let mut c = ErpsConfig::default();
        c.tls_certificate_path = Some("server.pem".into());
        assert_eq!(c.validate(), Err(ConfigError::TlsPair));
    }
    #[test]
    fn negative_matching_ranges_are_rejected() {
        for mutate in [
            |c: &mut ErpsConfig| c.initial_elo_delta = -1,
            |c: &mut ErpsConfig| c.elo_step = -1,
            |c: &mut ErpsConfig| c.max_party_rating_spread = -1,
        ] {
            let mut config = ErpsConfig::default();
            mutate(&mut config);
            assert_eq!(config.validate(), Err(ConfigError::EloRange));
        }
    }
}
