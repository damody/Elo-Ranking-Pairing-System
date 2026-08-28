use erps::{
    components::QueueMode,
    id::*,
    placement::{release, reserve},
    server::{GameServer, Health, Registry, ServerLimits},
};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
proptest! {#[test]fn reservations_never_exceed_limits(total in 1u32..1000,cost in 1u32..100,max_instances in 1u16..=100,attempts in 1usize..200){let cost=cost.min(total);let server=GameServer{id:ServerId::new(),generation:ServerGeneration(1),endpoint:"x".into(),region:"tw".into(),modes:BTreeSet::from([QueueMode::OneVsOne]),capacity_total:total,capacity_used:0,max_instances,mode_costs:BTreeMap::from([(QueueMode::OneVsOne,cost)]),last_heartbeat:0,health:Health::Healthy,failures:0,instances:BTreeMap::new()};let id=server.id;let mut r=Registry::default();r.register(server,ServerLimits{max_capacity:total,max_instances:100}).unwrap();let mut matches=Vec::new();for _ in 0..attempts{let m=MatchId::new();if reserve(&mut r,m,QueueMode::OneVsOne,"tw").is_ok(){matches.push(m);}}prop_assert!(r.servers[&id].capacity_used<=total);prop_assert!(r.servers[&id].instances.len()<=max_instances as usize);for m in matches{release(&mut r,id,m).unwrap();}prop_assert_eq!(r.servers[&id].capacity_used,0);}}
