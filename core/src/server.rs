//! Game server registry and launch lifecycle.
use crate::{
    components::QueueMode,
    id::{MatchId, ServerGeneration, ServerId},
};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Health {
    Healthy,
    Unhealthy,
    Lost,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstanceState {
    Reserved,
    Accepted,
    Ready,
    Running,
    Finished,
    ServerLost,
}
#[derive(Clone, Debug)]
pub struct Instance {
    pub match_id: MatchId,
    pub cost: u32,
    pub state: InstanceState,
    pub endpoint: Option<String>,
    pub connection_token: Option<String>,
}
#[derive(Clone, Debug)]
pub struct GameServer {
    pub id: ServerId,
    pub generation: ServerGeneration,
    pub endpoint: String,
    pub region: String,
    pub modes: BTreeSet<QueueMode>,
    pub capacity_total: u32,
    pub capacity_used: u32,
    pub max_instances: u16,
    pub mode_costs: BTreeMap<QueueMode, u32>,
    pub last_heartbeat: u64,
    pub health: Health,
    pub failures: u32,
    pub instances: BTreeMap<MatchId, Instance>,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ServerError {
    #[error("stale generation")]
    StaleGeneration,
    #[error("invalid capacity")]
    InvalidCapacity,
    #[error("unknown instance")]
    UnknownInstance,
}
#[derive(Clone, Copy)]
pub struct ServerLimits {
    pub max_capacity: u32,
    pub max_instances: u16,
}
#[derive(Default)]
pub struct Registry {
    pub servers: BTreeMap<ServerId, GameServer>,
}
impl Registry {
    pub fn is_current_generation(&self, id: ServerId, generation: ServerGeneration) -> bool {
        self.servers
            .get(&id)
            .is_some_and(|server| server.generation == generation)
    }

    pub fn register(
        &mut self,
        server: GameServer,
        limits: ServerLimits,
    ) -> Result<(), ServerError> {
        if server.capacity_total == 0
            || server.capacity_total > limits.max_capacity
            || server.max_instances == 0
            || server.max_instances > limits.max_instances.min(100)
            || server.mode_costs.values().any(|v| *v == 0)
            || server.capacity_used > server.capacity_total
            || server.instances.len() > server.max_instances as usize
        {
            return Err(ServerError::InvalidCapacity);
        }
        if let Some(old) = self.servers.get_mut(&server.id) {
            if old.generation > server.generation {
                return Err(ServerError::StaleGeneration);
            }
            if old.generation == server.generation {
                if old.region != server.region
                    || old.modes != server.modes
                    || old.capacity_total != server.capacity_total
                    || old.max_instances != server.max_instances
                    || old.mode_costs != server.mode_costs
                {
                    return Err(ServerError::InvalidCapacity);
                }
                old.endpoint = server.endpoint;
                old.last_heartbeat = server.last_heartbeat;
                old.health = Health::Healthy;
                return Ok(());
            }
        }
        self.servers.insert(server.id, server);
        Ok(())
    }
    pub fn heartbeat(
        &mut self,
        id: ServerId,
        generation: ServerGeneration,
        now: u64,
        used: u32,
    ) -> Result<(), ServerError> {
        let s = self
            .servers
            .get_mut(&id)
            .ok_or(ServerError::StaleGeneration)?;
        if s.generation != generation {
            return Err(ServerError::StaleGeneration);
        }
        s.last_heartbeat = now;
        let authoritative_used = s
            .instances
            .values()
            .filter(|instance| {
                !matches!(
                    instance.state,
                    InstanceState::Finished | InstanceState::ServerLost
                )
            })
            .map(|instance| instance.cost)
            .sum::<u32>();
        if used != authoritative_used {
            s.failures = s.failures.saturating_add(1);
        }
        s.capacity_used = authoritative_used.min(s.capacity_total);
        s.health = Health::Healthy;
        Ok(())
    }
    pub fn update_health(
        &mut self,
        now: u64,
        unhealthy_after: u64,
        lost_after: u64,
    ) -> Vec<(MatchId, bool)> {
        let mut lost = Vec::new();
        for s in self.servers.values_mut() {
            let age = now.saturating_sub(s.last_heartbeat);
            s.health = if age > lost_after {
                Health::Lost
            } else if age > unhealthy_after {
                Health::Unhealthy
            } else {
                Health::Healthy
            };
            if s.health == Health::Lost {
                for i in s.instances.values_mut() {
                    if i.state != InstanceState::Finished && i.state != InstanceState::ServerLost {
                        let was_running = i.state == InstanceState::Running;
                        i.state = InstanceState::ServerLost;
                        lost.push((i.match_id, was_running))
                    }
                }
                s.capacity_used = 0;
            }
        }
        lost
    }
    pub fn reconcile(
        &mut self,
        id: ServerId,
        generation: ServerGeneration,
        instances: BTreeMap<MatchId, Instance>,
    ) -> Result<(), ServerError> {
        let server = self
            .servers
            .get_mut(&id)
            .ok_or(ServerError::StaleGeneration)?;
        if server.generation != generation {
            return Err(ServerError::StaleGeneration);
        }
        if instances
            .keys()
            .any(|match_id| !server.instances.contains_key(match_id))
        {
            return Err(ServerError::UnknownInstance);
        }
        for (match_id, reported) in instances {
            let authoritative = server
                .instances
                .get_mut(&match_id)
                .ok_or(ServerError::UnknownInstance)?;
            if authoritative.cost != reported.cost {
                return Err(ServerError::InvalidCapacity);
            }
            authoritative.state = reported.state;
            authoritative.endpoint = reported.endpoint;
            authoritative.connection_token = reported.connection_token;
        }
        server.capacity_used = server
            .instances
            .values()
            .filter(|instance| {
                !matches!(
                    instance.state,
                    InstanceState::Finished | InstanceState::ServerLost
                )
            })
            .map(|instance| instance.cost)
            .sum::<u32>()
            .min(server.capacity_total);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(id: ServerId, generation: u64) -> GameServer {
        GameServer {
            id,
            generation: ServerGeneration(generation),
            endpoint: "game".into(),
            region: "tw".into(),
            modes: BTreeSet::from([QueueMode::OneVsOne]),
            capacity_total: 10,
            capacity_used: 0,
            max_instances: 2,
            mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
            last_heartbeat: 0,
            health: Health::Healthy,
            failures: 0,
            instances: BTreeMap::new(),
        }
    }

    #[test]
    fn stale_generation_cannot_overwrite_or_heartbeat_replacement() {
        let id = ServerId::new();
        let mut registry = Registry::default();
        let limits = ServerLimits {
            max_capacity: 10,
            max_instances: 2,
        };
        registry.register(server(id, 1), limits).unwrap();
        registry.register(server(id, 2), limits).unwrap();
        assert_eq!(
            registry.heartbeat(id, ServerGeneration(1), 100, 0),
            Err(ServerError::StaleGeneration)
        );
        assert_eq!(registry.servers[&id].generation, ServerGeneration(2));
        assert_eq!(registry.servers[&id].last_heartbeat, 0);
        assert!(!registry.is_current_generation(id, ServerGeneration(1)));
        assert!(registry.is_current_generation(id, ServerGeneration(2)));
    }

    #[test]
    fn lost_running_instance_is_marked_and_never_presented_as_retryable() {
        let id = ServerId::new();
        let match_id = MatchId::new();
        let mut value = server(id, 1);
        value.capacity_used = 1;
        value.instances.insert(
            match_id,
            Instance {
                match_id,
                cost: 1,
                state: InstanceState::Running,
                endpoint: Some("game".into()),
                connection_token: Some("secret".into()),
            },
        );
        let mut registry = Registry::default();
        registry
            .register(
                value,
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 2,
                },
            )
            .unwrap();
        assert_eq!(registry.update_health(11, 5, 10), vec![(match_id, true)]);
        assert_eq!(
            registry.servers[&id].instances[&match_id].state,
            InstanceState::ServerLost
        );
        assert_eq!(registry.servers[&id].capacity_used, 0);
    }
    #[test]
    fn reconcile_preserves_authoritative_cost_and_rejects_unknown_instance() {
        let id = ServerId::new();
        let match_id = MatchId::new();
        let mut value = server(id, 1);
        value.capacity_used = 1;
        value.instances.insert(
            match_id,
            Instance {
                match_id,
                cost: 1,
                state: InstanceState::Accepted,
                endpoint: None,
                connection_token: None,
            },
        );
        let mut registry = Registry::default();
        registry
            .register(
                value,
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 2,
                },
            )
            .unwrap();
        let mut report = registry.servers[&id].instances.clone();
        report.get_mut(&match_id).unwrap().state = InstanceState::Running;
        registry.reconcile(id, ServerGeneration(1), report).unwrap();
        assert_eq!(
            registry.servers[&id].instances[&match_id].state,
            InstanceState::Running
        );
        let unknown = MatchId::new();
        assert_eq!(
            registry.reconcile(
                id,
                ServerGeneration(1),
                BTreeMap::from([(
                    unknown,
                    Instance {
                        match_id: unknown,
                        cost: 1,
                        state: InstanceState::Running,
                        endpoint: None,
                        connection_token: None,
                    },
                )]),
            ),
            Err(ServerError::UnknownInstance)
        );
    }

    #[test]
    fn same_generation_registration_is_idempotent_and_preserves_ledger() {
        let id = ServerId::new();
        let match_id = MatchId::new();
        let mut initial = server(id, 7);
        initial.capacity_used = 1;
        initial.instances.insert(
            match_id,
            Instance {
                match_id,
                cost: 1,
                state: InstanceState::Running,
                endpoint: Some("old-game".into()),
                connection_token: Some("secret".into()),
            },
        );
        let limits = ServerLimits {
            max_capacity: 10,
            max_instances: 2,
        };
        let mut registry = Registry::default();
        registry.register(initial, limits).unwrap();
        let mut reconnect = server(id, 7);
        reconnect.endpoint = "new-control-endpoint".into();
        reconnect.last_heartbeat = 99;

        registry.register(reconnect, limits).unwrap();

        let current = &registry.servers[&id];
        assert_eq!(current.endpoint, "new-control-endpoint");
        assert_eq!(current.last_heartbeat, 99);
        assert_eq!(current.capacity_used, 1);
        assert_eq!(current.instances[&match_id].state, InstanceState::Running);
    }
}
