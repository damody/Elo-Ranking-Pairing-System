//! Deterministic correctness-first load scenario shared by CLI and smoke tests.
use crate::{
    components::QueueMode,
    id::{MatchId, PartyId, PlayerId, ServerGeneration, ServerId, TicketId},
    matching::{
        claim::Claims, five_v_five, free_for_all, one_v_one, snapshot::CandidateSnapshot,
        PartyTicket,
    },
    placement,
    proposal::{Proposal, ProposalState},
    server::{GameServer, Health, Registry, ServerLimits},
};
use erps_client::{Client, ConnectOptions, Event, QueueMode as ClientMode};
use erps_proto::v1::{self as pb, game_server_service_client::GameServerServiceClient};
use serde::Serialize;
use specs::{Builder, Join, WorldExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

struct ProcessMemorySampler {
    system: sysinfo::System,
    pid: sysinfo::Pid,
    peak_bytes: u64,
}

impl ProcessMemorySampler {
    fn new() -> Result<Self, String> {
        let pid = sysinfo::get_current_pid().map_err(|e| e.to_string())?;
        let mut sampler = Self {
            system: sysinfo::System::new(),
            pid,
            peak_bytes: 0,
        };
        sampler.sample()?;
        Ok(sampler)
    }

    fn sample(&mut self) -> Result<(), String> {
        self.system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[self.pid]),
            true,
            sysinfo::ProcessRefreshKind::nothing().with_memory(),
        );
        let bytes = self
            .system
            .process(self.pid)
            .ok_or("current process missing from OS process table")?
            .memory();
        self.peak_bytes = self.peak_bytes.max(bytes);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ScenarioConfig {
    pub players: usize,
    pub seed: u64,
    pub workers: usize,
}
impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            players: 100_000,
            seed: 0x4552_5053,
            workers: std::thread::available_parallelism().map_or(1, usize::from),
        }
    }
}
#[derive(Clone, Debug)]
struct Party {
    id: PartyId,
    mode: QueueMode,
    players: Vec<PlayerId>,
    rating: i32,
    region: String,
}
#[derive(Clone, Debug)]
struct Server {
    id: ServerId,
    capacity: u32,
    used: u32,
    max_instances: u16,
    costs: [u32; 3],
}
#[derive(Serialize, Debug)]
pub struct LoadReport {
    pub status: &'static str,
    pub players: usize,
    pub parties: usize,
    pub matches: usize,
    pub unmatched: usize,
    pub invariant_failures: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
    pub elapsed_ms: u128,
    pub throughput_players_per_second: f64,
    pub seed: u64,
    pub workers: usize,
    pub logical_digest: u64,
    pub capacity_utilization: f64,
    pub ready_rate: f64,
    pub average_elo_quality: f64,
    pub retries: u64,
    pub cancellations: u64,
    pub rejections: u64,
    pub ready_timeouts: u64,
    pub server_cycles: u64,
    pub completed_matches: u64,
    pub memory_high_watermark_bytes: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub rust_version: String,
    pub operating_system: String,
    pub cpu_arch: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub settings: ScenarioSettings,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineComparison>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ScenarioSettings {
    pub mode_mix_percent: [u8; 3],
    pub party_limits: [u8; 3],
    pub candidate_budget_per_shard: usize,
    pub regions: [&'static str; 3],
    pub fleet_servers: usize,
    pub search_delta: i32,
    pub ready_deadline_ticks: u64,
    pub rejection_every_matches: u64,
    pub timeout_every_matches: u64,
    pub cancellation_every_matches: u64,
    pub server_cycle_every_matches: u64,
    pub server_capacity_range: [u32; 2],
    pub server_instance_range: [u16; 2],
    pub rating_range: [i32; 2],
}

#[derive(Clone, Debug, Serialize)]
pub struct BaselineComparison {
    pub comparable: bool,
    pub environment_differences: Vec<String>,
    pub setting_differences: Vec<String>,
    pub throughput_change_percent: Option<f64>,
    pub p99_change_percent: Option<f64>,
}

pub fn compare_baseline(current: &LoadReport, baseline: &serde_json::Value) -> BaselineComparison {
    let mut environment_differences = Vec::new();
    let mut setting_differences = Vec::new();
    for (key, current_value) in [
        (
            "operating_system",
            serde_json::json!(current.operating_system),
        ),
        ("cpu_arch", serde_json::json!(current.cpu_arch)),
        ("cpu_model", serde_json::json!(current.cpu_model)),
        ("logical_cpus", serde_json::json!(current.logical_cpus)),
        ("rust_version", serde_json::json!(current.rust_version)),
    ] {
        if baseline.get(key) != Some(&current_value) {
            environment_differences.push(key.into());
        }
    }
    for (key, current_value) in [
        ("players", serde_json::json!(current.players)),
        ("seed", serde_json::json!(current.seed)),
        ("workers", serde_json::json!(current.workers)),
        ("settings", serde_json::json!(current.settings)),
    ] {
        if baseline.get(key) != Some(&current_value) {
            setting_differences.push(key.into());
        }
    }
    let percent = |key: &str, value: f64| {
        baseline
            .get(key)
            .and_then(serde_json::Value::as_f64)
            .filter(|baseline| *baseline != 0.0)
            .map(|baseline| (value - baseline) * 100.0 / baseline)
    };
    BaselineComparison {
        comparable: environment_differences.is_empty() && setting_differences.is_empty(),
        environment_differences,
        setting_differences,
        throughput_change_percent: percent(
            "throughput_players_per_second",
            current.throughput_players_per_second,
        ),
        p99_change_percent: percent("p99_us", current.p99_us as f64),
    }
}

#[derive(Serialize, Debug)]
pub struct TransportReport {
    pub path: &'static str,
    pub players: usize,
    pub parties: usize,
    pub completed_matches: usize,
    pub completed_match_results: usize,
    pub mode_matches: [usize; 3],
    pub elapsed_ms: u128,
    pub rust_sdk: bool,
    pub game_server_stream: bool,
    pub fleet_servers: usize,
    pub servers_used: usize,
    pub regions_used: usize,
}

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn range(&mut self, n: usize) -> usize {
        (self.next() % (n as u64)) as usize
    }
}
fn push_mode(
    parties: &mut Vec<Party>,
    rng: &mut Rng,
    mode: QueueMode,
    mut players: usize,
    group: usize,
    max_party: usize,
) {
    while players > 0 {
        let match_size = players.min(group);
        let mut left = match_size;
        let base_rating = 900 + rng.range(1001) as i32;
        let region = ["tw", "us", "eu"][rng.range(3)].to_owned();
        while left > 0 {
            let consumed = match_size - left;
            let structural_limit = if group == 10 {
                5 - (consumed % 5)
            } else {
                left
            };
            let size = (1 + rng.range(max_party)).min(left).min(structural_limit);
            let members = (0..size)
                .map(|_| PlayerId::from_uuid(uuid::Uuid::from_u128(rng.next() as u128)))
                .collect();
            parties.push(Party {
                id: PartyId::from_uuid(uuid::Uuid::from_u128(rng.next() as u128)),
                mode,
                players: members,
                rating: base_rating + rng.range(201) as i32 - 100,
                region: region.clone(),
            });
            left -= size;
        }
        players -= match_size;
    }
}
fn generate(config: &ScenarioConfig) -> Vec<Party> {
    let mut rng = Rng(config.seed);
    let one = config.players * 30 / 100;
    let five = config.players * 50 / 100;
    let ffa = config.players - one - five;
    let mut parties = Vec::new();
    push_mode(&mut parties, &mut rng, QueueMode::OneVsOne, one, 2, 1);
    push_mode(&mut parties, &mut rng, QueueMode::FiveVsFive, five, 10, 5);
    push_mode(&mut parties, &mut rng, QueueMode::FreeForAll, ffa, 8, 4);
    parties
}
fn materialize_through_ecs(parties: &[Party]) -> Vec<Party> {
    let mut world = crate::world::build_world(&crate::ErpsConfig::default());
    for party in parties {
        for player in &party.players {
            world
                .create_entity()
                .with(crate::components::PlayerIdentity(*player))
                .with(crate::components::EloRating(BTreeMap::from([(
                    party.mode,
                    party.rating,
                )])))
                .with(crate::components::CreditScore(100))
                .with(crate::components::PlayerState::Queued)
                .build();
        }
        world
            .create_entity()
            .with(crate::components::PartyIdentity(party.id))
            .with(crate::components::PartyMembers(party.players.clone()))
            .with(crate::components::PartyRevision(1))
            .with(crate::components::PartyState::Queued)
            .build();
        world
            .create_entity()
            .with(crate::components::TicketIdentity(TicketId::from_uuid(
                uuid::Uuid::from_u128(party.id.as_uuid().as_u128() ^ 0x4552_5053),
            )))
            .with(crate::components::PartyIdentity(party.id))
            .with(crate::components::TicketMode(party.mode))
            .with(crate::components::TicketRegions(vec![party.region.clone()]))
            .with(crate::components::EnqueuedAt(0))
            .with(crate::components::TicketState::Queued)
            .build();
    }
    world.maintain();
    let party_ids = world.read_storage::<crate::components::PartyIdentity>();
    let members = world.read_storage::<crate::components::PartyMembers>();
    let ticket_modes = world.read_storage::<crate::components::TicketMode>();
    let ticket_regions = world.read_storage::<crate::components::TicketRegions>();
    let ticket_ids = world.read_storage::<crate::components::TicketIdentity>();
    let player_ids = world.read_storage::<crate::components::PlayerIdentity>();
    let ratings = world.read_storage::<crate::components::EloRating>();
    let player_ratings: BTreeMap<_, _> = (&player_ids, &ratings)
        .join()
        .map(|(player, ratings)| (player.0, ratings.0.clone()))
        .collect();
    let parties_by_id: BTreeMap<_, _> = (&party_ids, &members)
        .join()
        .map(|(party, members)| (party.0, members.0.clone()))
        .collect();
    (&party_ids, &ticket_ids, &ticket_modes, &ticket_regions)
        .join()
        .filter_map(|(party, _, mode, regions)| {
            let players = parties_by_id.get(&party.0)?.clone();
            let rating = players
                .first()
                .and_then(|player| player_ratings.get(player))
                .and_then(|ratings| ratings.get(&mode.0))
                .copied()
                .unwrap_or(1000);
            Some(Party {
                id: party.0,
                mode: mode.0,
                players,
                rating,
                region: regions.0.first().cloned().unwrap_or_default(),
            })
        })
        .collect()
}
fn fleet(seed: u64) -> Vec<Server> {
    let mut rng = Rng(seed ^ 0xfeed_beef);
    (0..128)
        .map(|i| Server {
            id: ServerId::from_uuid(uuid::Uuid::from_u128(rng.next() as u128)),
            capacity: 100 + rng.range(900) as u32,
            used: 0,
            max_instances: (1 + rng.range(100)) as u16,
            costs: [1 + (i % 3) as u32, 4 + (i % 7) as u32, 3 + (i % 5) as u32],
        })
        .collect()
}
fn runtime_registry(generated: &[Server]) -> Registry {
    let mut registry = Registry::default();
    for server in generated {
        registry
            .register(
                GameServer {
                    id: server.id,
                    generation: ServerGeneration(1),
                    endpoint: format!("load-{}", server.id),
                    region: ["tw", "us", "eu"][server.id.as_uuid().as_u128() as usize % 3].into(),
                    modes: BTreeSet::from([
                        QueueMode::OneVsOne,
                        QueueMode::FiveVsFive,
                        QueueMode::FreeForAll,
                    ]),
                    capacity_total: server.capacity,
                    capacity_used: server.used,
                    max_instances: server.max_instances,
                    mode_costs: BTreeMap::from([
                        (QueueMode::OneVsOne, server.costs[0]),
                        (QueueMode::FiveVsFive, server.costs[1]),
                        (QueueMode::FreeForAll, server.costs[2]),
                    ]),
                    last_heartbeat: 0,
                    health: Health::Healthy,
                    failures: 0,
                    instances: BTreeMap::new(),
                },
                ServerLimits {
                    max_capacity: u32::MAX,
                    max_instances: 100,
                },
            )
            .expect("generated fleet obeys server policy");
    }
    registry
}
fn candidate_for(
    group: &[&Party],
    mode: QueueMode,
    workers: usize,
) -> Option<crate::matching::Candidate> {
    let tickets = group
        .iter()
        .enumerate()
        .map(|(index, party)| PartyTicket {
            id: TicketId::from_uuid(uuid::Uuid::from_u128(
                party.id.as_uuid().as_u128() ^ index as u128,
            )),
            party: party.id,
            members: party.players.clone(),
            ratings: vec![party.rating; party.players.len()],
            effective_rating: party.rating,
            enqueued_at: index as u64,
            revision: 1,
            region: party.region.clone(),
            mode,
            search_delta: 600,
        })
        .collect();
    let snapshot = CandidateSnapshot::new(tickets);
    let candidates =
        crate::matching::dispatcher::generate(&snapshot, workers, 1024, |items| match mode {
            QueueMode::OneVsOne => one_v_one::build(items),
            QueueMode::FiveVsFive => five_v_five::build(items, 1024),
            QueueMode::FreeForAll => free_for_all::build(items, 1024),
        });
    candidates.into_iter().next()
}
fn registry_diagnostics(registry: &Registry) -> Vec<String> {
    registry
        .servers
        .values()
        .filter(|server| {
            server.capacity_used > server.capacity_total
                || server.instances.len() > server.max_instances as usize
        })
        .map(|server| format!("server {} exceeded capacity or instance limit", server.id))
        .collect()
}

pub fn run(config: ScenarioConfig) -> Result<LoadReport, String> {
    if config.players < 20 {
        return Err("players must be at least 20".into());
    }
    if config.workers == 0 {
        return Err("workers must be greater than zero".into());
    }
    let started = Instant::now();
    let mut memory_sampler = ProcessMemorySampler::new()?;
    let parties = materialize_through_ecs(&generate(&config));
    memory_sampler.sample()?;
    let party_count = parties.len();
    let servers = fleet(config.seed);
    let mut registry = runtime_registry(&servers);
    let mut seen = BTreeSet::new();
    let mut digest = 0u64;
    let mut matches = 0usize;
    let mut failures = 0usize;
    let mut diagnostics = Vec::new();
    let mut rejection_count = 0u64;
    let mut timeout_count = 0u64;
    let mut cancellation_count = 0u64;
    let mut retries = 0u64;
    let mut latencies = Vec::new();
    let mut elo_quality_sum = 0u64;
    let mut peak_capacity_used = 0u64;
    let mut server_cycle_count = 0u64;
    for mode in [
        QueueMode::OneVsOne,
        QueueMode::FiveVsFive,
        QueueMode::FreeForAll,
    ] {
        let target = match mode {
            QueueMode::OneVsOne => 2,
            QueueMode::FiveVsFive => 10,
            QueueMode::FreeForAll => 8,
        };
        let mode_parties: Vec<_> = parties.iter().filter(|p| p.mode == mode).collect();
        let mut group = Vec::new();
        let mut count = 0;
        for p in mode_parties {
            if count + p.players.len() > target {
                failures += 1;
                diagnostics.push(format!(
                    "{mode:?}: generated party group exceeds roster size"
                ));
                group.clear();
                count = 0;
            }
            count += p.players.len();
            group.push(p);
            if count == target {
                let operation_started = Instant::now();
                let Some(candidate) = candidate_for(&group, mode, config.workers) else {
                    failures += 1;
                    diagnostics.push(format!("{mode:?}: no candidate for complete roster"));
                    group.clear();
                    count = 0;
                    continue;
                };
                let ids: Vec<_> = candidate.teams.iter().flatten().copied().collect();
                if ids.len() != target || ids.iter().any(|id| !seen.insert(*id)) {
                    failures += 1;
                    diagnostics.push(format!("{mode:?}: duplicate player or wrong roster size"));
                }
                if mode == QueueMode::FreeForAll
                    && (candidate.teams.len() != 8
                        || candidate.teams.iter().any(|team| team.len() != 1))
                {
                    failures += 1;
                    diagnostics.push("FreeForAll: roster is not eight singleton teams".into());
                }
                let mut claims = Claims::default();
                if !claims.commit(&candidate, |_| true) {
                    failures += 1;
                    diagnostics.push(format!("{mode:?}: candidate claim failed"));
                }
                elo_quality_sum = elo_quality_sum.saturating_add(
                    candidate.quality_key.0.unsigned_abs() as u64
                        + candidate.quality_key.1.unsigned_abs() as u64
                        + candidate.quality_key.2.unsigned_abs() as u64,
                );
                let owners = group
                    .iter()
                    .flat_map(|party| party.players.iter().map(move |player| (*player, party.id)))
                    .collect();
                let mut proposal = Proposal::from_candidate(&candidate, owners, 0, 15);
                if matches % 200 == 199 {
                    let rejector = ids[0];
                    let _ = proposal.respond(proposal.id, rejector, false);
                    rejection_count += 1;
                    retries += 1;
                    proposal = Proposal::from_candidate(&candidate, BTreeMap::new(), 1, 15);
                } else if matches % 500 == 499 {
                    let _ = proposal.expire(16);
                    timeout_count += 1;
                    retries += 1;
                    proposal = Proposal::from_candidate(&candidate, BTreeMap::new(), 17, 15);
                } else if matches % 100 == 99 {
                    cancellation_count += 1;
                    claims.release(&candidate.tickets);
                    if !claims.commit(&candidate, |_| true) {
                        failures += 1;
                        diagnostics.push(format!("{mode:?}: cancel/recommit was not atomic"));
                    }
                }
                for player in &ids {
                    if proposal.respond(proposal.id, *player, true).is_err() {
                        failures += 1;
                        diagnostics.push(format!("{mode:?}: ready response rejected"));
                    }
                }
                if proposal.state != ProposalState::AwaitingPlacement {
                    failures += 1;
                    diagnostics.push(format!("{mode:?}: all accepts did not reach placement"));
                }
                let match_id = MatchId::from_uuid(uuid::Uuid::from_u128(matches as u128 + 1));
                if let Ok(server_id) =
                    placement::reserve(&mut registry, match_id, mode, &group[0].region)
                {
                    peak_capacity_used = peak_capacity_used.max(
                        registry
                            .servers
                            .values()
                            .map(|server| server.capacity_used as u64)
                            .sum(),
                    );
                    digest = digest
                        .wrapping_mul(1099511628211)
                        .wrapping_add(ids.len() as u64)
                        .wrapping_add(p.rating as u64)
                        .wrapping_add(p.id.as_uuid().as_u128() as u64);
                    matches += 1;
                    if matches.is_multiple_of(256) {
                        memory_sampler.sample()?;
                    }
                    placement::transition(
                        &mut registry,
                        server_id,
                        match_id,
                        crate::server::InstanceState::Ready,
                        Some("load-endpoint".into()),
                        Some("redacted".into()),
                    )
                    .map_err(|error| error.to_string())?;
                    placement::release(&mut registry, server_id, match_id)
                        .map_err(|error| error.to_string())?;
                    if matches.is_multiple_of(1000) {
                        if let Some(server) = registry.servers.values_mut().next() {
                            server.health = Health::Lost;
                            server.health = Health::Healthy;
                            server_cycle_count += 1;
                        }
                    }
                } else {
                    failures += 1;
                    diagnostics.push(format!(
                        "{mode:?}: no capacity for generated complete match"
                    ));
                }
                latencies.push(operation_started.elapsed().as_micros() as u64);
                group.clear();
                count = 0;
            }
        }
        if count != 0 {
            failures += 1;
            diagnostics.push(format!("{mode:?}: incomplete trailing roster"));
        }
    }
    let elapsed = started.elapsed();
    let unmatched = config.players.saturating_sub(seen.len());
    if unmatched != 0 {
        failures += 1;
        diagnostics.push(format!(
            "{unmatched} generated players were never committed"
        ));
    }
    let server_diagnostics = registry_diagnostics(&registry);
    failures += server_diagnostics.len();
    diagnostics.extend(server_diagnostics);
    let total_capacity: u64 = registry
        .servers
        .values()
        .map(|s| s.capacity_total as u64)
        .sum();
    let total_used: u64 = registry
        .servers
        .values()
        .map(|s| s.capacity_used as u64)
        .sum();
    latencies.sort_unstable();
    let percentile = |numerator: usize| -> u64 {
        latencies
            .get(latencies.len().saturating_sub(1) * numerator / 100)
            .copied()
            .unwrap_or_default()
    };
    memory_sampler.sample()?;
    let report = LoadReport {
        status: if failures == 0 { "PASS" } else { "FAIL" },
        players: config.players,
        parties: party_count,
        matches,
        unmatched,
        invariant_failures: failures,
        diagnostics,
        elapsed_ms: elapsed.as_millis(),
        throughput_players_per_second: config.players as f64 / elapsed.as_secs_f64().max(0.000_001),
        seed: config.seed,
        workers: config.workers,
        logical_digest: digest,
        capacity_utilization: peak_capacity_used.max(total_used) as f64
            / total_capacity.max(1) as f64,
        ready_rate: matches as f64
            / (matches as u64 + rejection_count + timeout_count).max(1) as f64,
        average_elo_quality: elo_quality_sum as f64 / matches.max(1) as f64,
        retries,
        cancellations: cancellation_count,
        rejections: rejection_count,
        ready_timeouts: timeout_count,
        server_cycles: server_cycle_count,
        completed_matches: matches as u64,
        memory_high_watermark_bytes: memory_sampler.peak_bytes,
        p50_us: percentile(50),
        p95_us: percentile(95),
        p99_us: percentile(99),
        rust_version: "rust-1.95-toolchain".into(),
        operating_system: std::env::consts::OS.into(),
        cpu_arch: std::env::consts::ARCH.into(),
        cpu_model: std::env::var("PROCESSOR_IDENTIFIER")
            .or_else(|_| std::env::var("HOSTTYPE"))
            .unwrap_or_else(|_| "unknown".into()),
        logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        settings: ScenarioSettings {
            mode_mix_percent: [30, 50, 20],
            party_limits: [1, 5, 4],
            candidate_budget_per_shard: 1024,
            regions: ["tw", "us", "eu"],
            fleet_servers: 128,
            search_delta: 600,
            ready_deadline_ticks: 15,
            rejection_every_matches: 200,
            timeout_every_matches: 500,
            cancellation_every_matches: 100,
            server_cycle_every_matches: 1000,
            server_capacity_range: [100, 999],
            server_instance_range: [1, 100],
            rating_range: [800, 2000],
        },
        baseline: None,
        transport: None,
    };
    if failures == 0 {
        Ok(report)
    } else {
        Err(serde_json::to_string_pretty(&report).unwrap())
    }
}

/// Executes the same ready/placement invariants through real loopback gRPC.
fn validate_grpc_launch(
    mode: QueueMode,
    launch: &pb::LaunchMatch,
    parties: &[Vec<String>],
) -> Result<(), String> {
    let expected_mode = match mode {
        QueueMode::OneVsOne => pb::QueueMode::OneVOne,
        QueueMode::FiveVsFive => pb::QueueMode::FiveVFive,
        QueueMode::FreeForAll => pb::QueueMode::FreeForAll,
    } as i32;
    if launch.mode != expected_mode {
        return Err("gRPC launch mode mismatch".into());
    }
    let expected_shape: &[usize] = match mode {
        QueueMode::OneVsOne => &[1, 1],
        QueueMode::FiveVsFive => &[5, 5],
        QueueMode::FreeForAll => &[1, 1, 1, 1, 1, 1, 1, 1],
    };
    let actual_shape: Vec<_> = launch
        .teams
        .iter()
        .map(|team| team.player_ids.len())
        .collect();
    if actual_shape != expected_shape {
        return Err(format!(
            "gRPC launch team shape mismatch: expected {expected_shape:?}, got {actual_shape:?}"
        ));
    }
    let mut expected_players: Vec<_> = parties.iter().flatten().cloned().collect();
    let mut actual_players: Vec<_> = launch
        .teams
        .iter()
        .flat_map(|team| team.player_ids.iter().cloned())
        .collect();
    expected_players.sort();
    actual_players.sort();
    if actual_players != expected_players {
        return Err("gRPC launch roster contains missing, duplicate, or foreign players".into());
    }
    if mode == QueueMode::FiveVsFive {
        for party in parties {
            if !launch
                .teams
                .iter()
                .any(|team| party.iter().all(|player| team.player_ids.contains(player)))
            {
                return Err("gRPC launch split a 5v5 party across teams".into());
            }
        }
    }
    Ok(())
}

pub async fn run_grpc(config: &ScenarioConfig) -> Result<TransportReport, String> {
    let started = Instant::now();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    drop(listener);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server_config = crate::ErpsConfig {
        allow_development_plaintext: true,
        ..Default::default()
    };
    tokio::spawn(async move {
        let _ = crate::grpc::serve(addr, server_config, async {
            let _ = stop_rx.await;
        })
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;
    let endpoint = format!("http://{addr}");
    let mut game = GameServerServiceClient::connect(endpoint.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mode_servers = [
        (pb::QueueMode::OneVOne, 1u32, 8u32, 2u32),
        (pb::QueueMode::FiveVFive, 5, 20, 4),
        (pb::QueueMode::FreeForAll, 4, 12, 3),
    ];
    let fleet: Vec<_> = ["tw", "us", "eu"]
        .into_iter()
        .flat_map(|region| mode_servers.into_iter().map(move |server| (region, server)))
        .collect();
    let mut game_servers = Vec::new();
    for (index, (region, (supported_mode, cost, capacity, max_instances))) in
        fleet.into_iter().enumerate()
    {
        let server_id = ServerId::from_uuid(uuid::Uuid::from_u128(
            config.seed as u128 + index as u128 + 1,
        ))
        .to_string();
        game.register(pb::RegisterServerRequest {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            auth_token: "load-server".into(),
            server_id: server_id.clone(),
            generation: 1,
            endpoint: format!("127.0.0.1:{}", 7200 + index),
            region: region.into(),
            capacity_total: capacity,
            max_instances,
            mode_costs: vec![pb::ModeCost {
                mode: supported_mode as i32,
                cost,
            }],
            instances: vec![],
            server_class: String::new(),
        })
        .await
        .map_err(|e| e.to_string())?;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let controls = game
            .control_stream(ReceiverStream::new(rx))
            .await
            .map_err(|e| e.to_string())?
            .into_inner();
        let heartbeat_tx = tx.clone();
        let heartbeat_server = server_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                if heartbeat_tx
                    .send(pb::ServerControl {
                        api: Some(pb::ApiVersion {
                            major: 1,
                            minor: 0,
                            capabilities: vec![],
                        }),
                        server_id: heartbeat_server.clone(),
                        generation: 1,
                        auth_token: "load-server".into(),
                        message: Some(pb::server_control::Message::Heartbeat(pb::Heartbeat {
                            capacity_used: 0,
                            running_instances: 0,
                        })),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        game_servers.push((server_id, tx, controls));
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    async fn proposal(s: &mut erps_client::EventStream) -> Result<String, String> {
        loop {
            match s.next().await {
                Some(Ok(Event::Proposal { proposal_id, .. })) => return Ok(proposal_id),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.to_string()),
                None => return Err("event stream ended".into()),
            }
        }
    }
    async fn matched(s: &mut erps_client::EventStream) -> Result<(), String> {
        loop {
            match s.next().await {
                Some(Ok(Event::Matched { .. })) => return Ok(()),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.to_string()),
                None => return Err("event stream ended".into()),
            }
        }
    }
    async fn next_launch(
        controls: &mut tonic::Streaming<pb::ErpsControl>,
    ) -> Result<pb::LaunchMatch, String> {
        loop {
            let control = controls
                .message()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("control ended")?;
            if let Some(pb::erps_control::Message::Launch(launch)) = control.message {
                return Ok(launch);
            }
        }
    }
    async fn wait_match_result_ack(
        controls: &mut tonic::Streaming<pb::ErpsControl>,
        expected_match_id: &str,
    ) -> Result<(), String> {
        loop {
            let control = controls
                .message()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("control ended before match result acknowledgement")?;
            if let Some(pb::erps_control::Message::MatchResultAck(match_id)) = control.message {
                if match_id == expected_match_id {
                    return Ok(());
                }
            }
        }
    }
    let generated = generate(config);
    let generated_party_count = generated.len();
    let mut groups: Vec<Vec<&Party>> = Vec::new();
    for mode in [
        QueueMode::OneVsOne,
        QueueMode::FiveVsFive,
        QueueMode::FreeForAll,
    ] {
        let target = match mode {
            QueueMode::OneVsOne => 2,
            QueueMode::FiveVsFive => 10,
            QueueMode::FreeForAll => 8,
        };
        let mut group = Vec::new();
        let mut size = 0;
        for party in generated.iter().filter(|party| party.mode == mode) {
            size += party.players.len();
            group.push(party);
            if size == target {
                groups.push(std::mem::take(&mut group));
                size = 0;
            }
        }
        if size != 0 {
            return Err("gRPC scenario generated an incomplete roster".into());
        }
    }
    let mut completed_matches = 0usize;
    let mut completed_players = 0usize;
    let mut mode_matches = [0usize; 3];
    let mut used_servers = BTreeSet::new();
    let mut used_regions = BTreeSet::new();
    for (group_index, group) in groups.into_iter().enumerate() {
        let mut clients = Vec::new();
        let mut streams = Vec::new();
        let mut leaders = Vec::new();
        let mut expected_parties = Vec::new();
        let mode = group[0].mode;
        let region = group[0].region.clone();
        let mode_index = match mode {
            QueueMode::OneVsOne => 0,
            QueueMode::FiveVsFive => 1,
            QueueMode::FreeForAll => 2,
        };
        let region_index = match region.as_str() {
            "tw" => 0,
            "us" => 1,
            "eu" => 2,
            _ => return Err(format!("unsupported generated region {region}")),
        };
        let server_index = region_index * 3 + mode_index;
        let (server_id, tx, controls) = &mut game_servers[server_index];
        used_servers.insert(server_id.clone());
        used_regions.insert(region.clone());
        for (party_index, party) in group.into_iter().enumerate() {
            let first_token = format!("grpc-{group_index}-{party_index}-0");
            let mut leader =
                Client::connect(ConnectOptions::plaintext_loopback(&endpoint, first_token))
                    .await
                    .map_err(|e| e.to_string())?;
            let created = leader
                .create_party(format!("Load{group_index}Party{party_index}"))
                .await
                .map_err(|e| e.to_string())?;
            let mut revision = created.revision;
            let party_id = created.entity_id;
            let mut party_clients = vec![leader];
            if party.players.len() > 1 {
                let invite = party_clients[0]
                    .create_invite(
                        party_id.clone(),
                        revision,
                        60,
                        (party.players.len() - 1) as u32,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                for member_index in 1..party.players.len() {
                    let mut member = Client::connect(ConnectOptions::plaintext_loopback(
                        &endpoint,
                        format!("grpc-{group_index}-{party_index}-{member_index}"),
                    ))
                    .await
                    .map_err(|e| e.to_string())?;
                    revision = member
                        .join_party(invite.clone())
                        .await
                        .map_err(|e| e.to_string())?
                        .revision;
                    party_clients.push(member);
                }
            }
            let leader_slot = clients.len();
            leaders.push((leader_slot, party_id, revision));
            expected_parties.push(
                party_clients
                    .iter()
                    .map(|client| client.player_id().unwrap_or_default().to_owned())
                    .collect(),
            );
            clients.extend(party_clients);
        }
        for client in &mut clients {
            streams.push(client.events().await.map_err(|e| e.to_string())?);
        }
        for (leader_slot, party_id, revision) in leaders {
            clients[leader_slot]
                .enqueue(
                    party_id,
                    revision,
                    match mode {
                        QueueMode::OneVsOne => ClientMode::OneVsOne,
                        QueueMode::FiveVsFive => ClientMode::FiveVsFive,
                        QueueMode::FreeForAll => ClientMode::FreeForAll,
                    },
                    [region.clone()],
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut proposal_id = None;
        for stream in &mut streams {
            let id = tokio::time::timeout(std::time::Duration::from_secs(3), proposal(stream))
                .await
                .map_err(|_| "proposal timeout".to_owned())??;
            if proposal_id.as_ref().is_some_and(|expected| expected != &id) {
                return Err("proposal mismatch".into());
            }
            proposal_id = Some(id);
        }
        let proposal_id = proposal_id.ok_or("empty gRPC roster")?;
        for client in &mut clients {
            client
                .accept_match(&proposal_id)
                .await
                .map_err(|e| e.to_string())?;
        }
        let launch = tokio::time::timeout(std::time::Duration::from_secs(3), next_launch(controls))
            .await
            .map_err(|_| "launch timeout".to_owned())??;
        validate_grpc_launch(mode, &launch, &expected_parties)?;
        let match_id = launch.match_id.clone();
        tx.send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id: server_id.clone(),
            generation: 1,
            auth_token: "load-server".into(),
            message: Some(pb::server_control::Message::LaunchResult(
                pb::LaunchResult {
                    match_id: match_id.clone(),
                    state: "accepted".into(),
                    endpoint: String::new(),
                    connection_token: String::new(),
                    reason: String::new(),
                },
            )),
        })
        .await
        .map_err(|e| e.to_string())?;
        tx.send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id: server_id.clone(),
            generation: 1,
            auth_token: "load-server".into(),
            message: Some(pb::server_control::Message::LaunchResult(
                pb::LaunchResult {
                    match_id: match_id.clone(),
                    state: "ready".into(),
                    endpoint: "127.0.0.1:7201".into(),
                    connection_token: format!("load-token-{group_index}"),
                    reason: String::new(),
                },
            )),
        })
        .await
        .map_err(|e| e.to_string())?;
        for stream in &mut streams {
            tokio::time::timeout(std::time::Duration::from_secs(3), matched(stream))
                .await
                .map_err(|_| "match event timeout".to_owned())??;
        }
        let placements = launch
            .teams
            .iter()
            .enumerate()
            .flat_map(|(team_index, team)| {
                team.player_ids
                    .iter()
                    .cloned()
                    .map(move |player_id| pb::PlayerPlacement {
                        player_id,
                        rank: (team_index + 1) as u32,
                    })
            })
            .collect();
        let completed_match_id = match_id.clone();
        tx.send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id: server_id.clone(),
            generation: 1,
            auth_token: "load-server".into(),
            message: Some(pb::server_control::Message::MatchResult(pb::MatchResult {
                match_id,
                placements,
            })),
        })
        .await
        .map_err(|e| e.to_string())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            wait_match_result_ack(controls, &completed_match_id),
        )
        .await
        .map_err(|_| "match result acknowledgement timeout".to_owned())??;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let state = clients[0].get_state().await.map_err(|e| e.to_string())?;
                if state.match_id.is_none() {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| "match completion reconciliation timeout".to_owned())??;
        completed_players += clients.len();
        completed_matches += 1;
        mode_matches[match mode {
            QueueMode::OneVsOne => 0,
            QueueMode::FiveVsFive => 1,
            QueueMode::FreeForAll => 2,
        }] += 1;
    }
    let _ = stop_tx.send(());
    Ok(TransportReport {
        path: "grpc-loopback",
        players: completed_players,
        parties: generated_party_count,
        completed_matches,
        completed_match_results: completed_matches,
        mode_matches,
        elapsed_ms: started.elapsed().as_millis(),
        rust_sdk: true,
        game_server_stream: true,
        fleet_servers: game_servers.len(),
        servers_used: used_servers.len(),
        regions_used: used_regions.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn memory_high_watermark_comes_from_the_operating_system() {
        let mut sampler = ProcessMemorySampler::new().unwrap();
        sampler.sample().unwrap();
        assert!(sampler.peak_bytes > 0);
    }

    #[test]
    fn deterministic_1000_player_smoke() {
        let a = run(ScenarioConfig {
            players: 1000,
            seed: 7,
            workers: 1,
        })
        .unwrap();
        let b = run(ScenarioConfig {
            players: 1000,
            seed: 7,
            workers: 8,
        })
        .unwrap();
        assert_eq!(a.players, 1000);
        assert_eq!(a.matches, b.matches);
        assert_eq!(a.logical_digest, b.logical_digest);
        assert_eq!(a.invariant_failures, 0)
    }
    #[test]
    fn zero_workers_is_rejected_instead_of_misreported() {
        assert_eq!(
            run(ScenarioConfig {
                players: 20,
                seed: 1,
                workers: 0,
            })
            .unwrap_err(),
            "workers must be greater than zero"
        );
    }
    #[test]
    fn generated_party_limits_hold() {
        let p = generate(&ScenarioConfig {
            players: 1000,
            seed: 9,
            workers: 1,
        });
        assert!(p.iter().all(|v| v.players.len()
            <= match v.mode {
                QueueMode::OneVsOne => 1,
                QueueMode::FiveVsFive => 5,
                QueueMode::FreeForAll => 4,
            }));
    }
    #[test]
    fn baseline_comparison_rejects_different_environment_or_settings() {
        let report = run(ScenarioConfig {
            players: 1000,
            seed: 9,
            workers: 1,
        })
        .unwrap();
        let identical = serde_json::to_value(&report).unwrap();
        assert!(compare_baseline(&report, &identical).comparable);
        let mut changed = identical;
        changed["cpu_arch"] = serde_json::json!("different-arch");
        changed["seed"] = serde_json::json!(10);
        let comparison = compare_baseline(&report, &changed);
        assert!(!comparison.comparable);
        assert_eq!(comparison.environment_differences, ["cpu_arch"]);
        assert_eq!(comparison.setting_differences, ["seed"]);
    }
    #[test]
    fn over_capacity_checker_returns_minimal_server_diagnostic() {
        let mut registry = runtime_registry(&fleet(1));
        let server = registry.servers.values_mut().next().unwrap();
        server.capacity_used = server.capacity_total + 1;
        let diagnostics = registry_diagnostics(&registry);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].contains("exceeded capacity"));
    }

    #[test]
    fn grpc_launch_checker_rejects_split_party_and_foreign_roster() {
        let parties = vec![
            vec!["a".into(), "b".into()],
            vec!["c".into(), "d".into(), "e".into()],
            vec!["f".into(), "g".into(), "h".into(), "i".into(), "j".into()],
        ];
        let split = pb::LaunchMatch {
            mode: pb::QueueMode::FiveVFive as i32,
            teams: vec![
                pb::Team {
                    team_index: 0,
                    player_ids: ["a", "c", "d", "e", "f"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
                pb::Team {
                    team_index: 1,
                    player_ids: ["b", "g", "h", "i", "j"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            validate_grpc_launch(QueueMode::FiveVsFive, &split, &parties).unwrap_err(),
            "gRPC launch split a 5v5 party across teams"
        );
        let mut foreign = split;
        foreign.teams[1].player_ids[4] = "outsider".into();
        assert_eq!(
            validate_grpc_launch(QueueMode::FiveVsFive, &foreign, &parties).unwrap_err(),
            "gRPC launch roster contains missing, duplicate, or foreign players"
        );
    }

    #[tokio::test]
    async fn grpc_transport_uses_heterogeneous_servers_in_every_region() {
        let report = run_grpc(&ScenarioConfig {
            players: 120,
            seed: 1_163_022_419,
            workers: 8,
        })
        .await
        .unwrap();
        assert_eq!(report.fleet_servers, 9);
        assert_eq!(report.regions_used, 3);
        assert!(report.servers_used >= 6);
        assert!(report.mode_matches.into_iter().all(|matches| matches > 0));
        assert_eq!(report.completed_match_results, report.completed_matches);
    }
}
