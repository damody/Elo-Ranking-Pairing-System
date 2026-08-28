//! Capacity-safe game server placement.
use crate::{
    components::QueueMode,
    id::{MatchId, ServerId},
    server::{Health, Instance, InstanceState, Registry},
};
use thiserror::Error;
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("no capacity")]
    NoCapacity,
    #[error("unknown server")]
    UnknownServer,
}
pub fn feasible(registry: &Registry, mode: QueueMode, region: &str) -> bool {
    registry.servers.values().any(|s| {
        s.health == Health::Healthy
            && s.region == region
            && s.modes.contains(&mode)
            && s.mode_costs
                .get(&mode)
                .is_some_and(|cost| s.capacity_used.saturating_add(*cost) <= s.capacity_total)
            && s.instances
                .values()
                .filter(|i| i.state != InstanceState::Finished)
                .count()
                < s.max_instances as usize
    })
}
pub fn reserve(
    registry: &mut Registry,
    match_id: MatchId,
    mode: QueueMode,
    region: &str,
) -> Result<ServerId, PlacementError> {
    let choice = registry
        .servers
        .values()
        .filter(|s| s.health == Health::Healthy && s.region == region && s.modes.contains(&mode))
        .filter_map(|s| {
            let cost = *s.mode_costs.get(&mode)?;
            let count = s
                .instances
                .values()
                .filter(|i| i.state != InstanceState::Finished)
                .count();
            if s.capacity_used.saturating_add(cost) > s.capacity_total
                || count >= s.max_instances as usize
            {
                return None;
            }
            Some((
                s.capacity_total - s.capacity_used - cost,
                s.failures,
                s.id,
                cost,
            ))
        })
        .min()
        .ok_or(PlacementError::NoCapacity)?;
    let s = registry.servers.get_mut(&choice.2).unwrap();
    s.capacity_used += choice.3;
    s.instances.insert(
        match_id,
        Instance {
            match_id,
            cost: choice.3,
            state: InstanceState::Reserved,
            endpoint: None,
            connection_token: None,
        },
    );
    Ok(s.id)
}
pub fn transition(
    registry: &mut Registry,
    server: ServerId,
    match_id: MatchId,
    state: InstanceState,
    endpoint: Option<String>,
    token: Option<String>,
) -> Result<(), PlacementError> {
    let s = registry
        .servers
        .get_mut(&server)
        .ok_or(PlacementError::UnknownServer)?;
    let i = s
        .instances
        .get_mut(&match_id)
        .ok_or(PlacementError::UnknownServer)?;
    i.state = state;
    if state == InstanceState::Ready {
        i.endpoint = endpoint;
        i.connection_token = token;
    }
    Ok(())
}
pub fn release(
    registry: &mut Registry,
    server: ServerId,
    match_id: MatchId,
) -> Result<(), PlacementError> {
    let s = registry
        .servers
        .get_mut(&server)
        .ok_or(PlacementError::UnknownServer)?;
    if let Some(i) = s.instances.remove(&match_id) {
        if !matches!(i.state, InstanceState::Finished | InstanceState::ServerLost) {
            s.capacity_used = s.capacity_used.saturating_sub(i.cost);
        }
    }
    Ok(())
}
pub fn retry_after_failure(
    registry: &mut Registry,
    failed_server: ServerId,
    match_id: MatchId,
    mode: QueueMode,
    region: &str,
) -> Result<ServerId, PlacementError> {
    release(registry, failed_server, match_id)?;
    if let Some(server) = registry.servers.get_mut(&failed_server) {
        server.failures = server.failures.saturating_add(1);
    }
    reserve(registry, match_id, mode, region)
}
#[derive(Clone, Copy, Debug)]
pub struct PlacementWait {
    pub started_at: u64,
    pub deadline: u64,
}
impl PlacementWait {
    pub fn new(now: u64, timeout: u64) -> Self {
        Self {
            started_at: now,
            deadline: now.saturating_add(timeout),
        }
    }
    pub fn expired(self, now: u64) -> bool {
        now > self.deadline
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        id::ServerGeneration,
        server::{GameServer, ServerLimits},
    };
    use std::collections::{BTreeMap, BTreeSet};
    fn server() -> GameServer {
        GameServer {
            id: ServerId::new(),
            generation: ServerGeneration(1),
            endpoint: "x".into(),
            region: "tw".into(),
            modes: BTreeSet::from([QueueMode::OneVsOne]),
            capacity_total: 10,
            capacity_used: 0,
            max_instances: 1,
            mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 2)]),
            last_heartbeat: 0,
            health: Health::Healthy,
            failures: 0,
            instances: BTreeMap::new(),
        }
    }
    #[test]
    fn instance_limit_and_release() {
        let mut r = Registry::default();
        r.register(
            server(),
            ServerLimits {
                max_capacity: 10,
                max_instances: 100,
            },
        )
        .unwrap();
        let id = *r.servers.keys().next().unwrap();
        let m = MatchId::new();
        assert_eq!(reserve(&mut r, m, QueueMode::OneVsOne, "tw"), Ok(id));
        assert_eq!(
            reserve(&mut r, MatchId::new(), QueueMode::OneVsOne, "tw"),
            Err(PlacementError::NoCapacity)
        );
        release(&mut r, id, m).unwrap();
        assert_eq!(r.servers[&id].capacity_used, 0);
    }
    #[test]
    fn reject_releases_full_reservation_and_retries_another_server() {
        let mut registry = Registry::default();
        let first = server();
        let first_id = first.id;
        let mut second = server();
        while second.id == first_id {
            second.id = ServerId::new();
        }
        registry
            .register(
                first,
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 100,
                },
            )
            .unwrap();
        registry
            .register(
                second,
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 100,
                },
            )
            .unwrap();
        let match_id = MatchId::new();
        let selected = reserve(&mut registry, match_id, QueueMode::OneVsOne, "tw").unwrap();
        let retried =
            retry_after_failure(&mut registry, selected, match_id, QueueMode::OneVsOne, "tw")
                .unwrap();
        assert_ne!(selected, retried);
        assert_eq!(registry.servers[&selected].capacity_used, 0);
        assert!(!registry.servers[&selected]
            .instances
            .contains_key(&match_id));
        assert_eq!(registry.servers[&retried].capacity_used, 2);
    }
    #[test]
    fn releasing_finished_instance_does_not_charge_another_match() {
        let mut registry = Registry::default();
        let mut value = server();
        value.max_instances = 2;
        let server_id = value.id;
        registry
            .register(
                value,
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 100,
                },
            )
            .unwrap();
        let finished = MatchId::new();
        let running = MatchId::new();
        reserve(&mut registry, finished, QueueMode::OneVsOne, "tw").unwrap();
        reserve(&mut registry, running, QueueMode::OneVsOne, "tw").unwrap();
        registry
            .servers
            .get_mut(&server_id)
            .unwrap()
            .instances
            .get_mut(&finished)
            .unwrap()
            .state = InstanceState::Finished;
        registry.servers.get_mut(&server_id).unwrap().capacity_used = 2;

        release(&mut registry, server_id, finished).unwrap();

        assert_eq!(registry.servers[&server_id].capacity_used, 2);
        assert!(registry.servers[&server_id]
            .instances
            .contains_key(&running));
    }
}
