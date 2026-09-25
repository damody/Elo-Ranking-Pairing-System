use super::*;
use crate::components::{BucketOwner, SearchRange};
use std::collections::BTreeMap;

const PLAYERS: usize = 1_000;
const ARRIVALS_PER_SECOND: usize = 17;
const MATCH_DURATION_SECONDS: u64 = 10;
const MODE: QueueMode = QueueMode::OneVsOne;

struct ScheduledResult {
    due_second: u64,
    match_id: MatchId,
    teams: [PlayerId; 2],
    before: [i32; 2],
    winner: usize,
}

fn next_random(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    *seed >> 32
}

fn expected_after(before: [i32; 2], winner: usize) -> [i32; 2] {
    let probability = 1.0 / (1.0 + 10f64.powf(f64::from(before[1] - before[0]) / 400.0));
    let first_score = if winner == 0 { 1.0 } else { 0.0 };
    let first_delta = (40.0 * (first_score - probability)).round() as i32;
    let second_delta = (40.0 * (probability - first_score)).round() as i32;
    [before[0] + first_delta, before[1] + second_delta]
}

#[test]
fn paced_1000_players_match_nearby_elo_and_settle_after_ten_seconds() {
    let mut config = ErpsConfig {
        initial_elo_delta: 20,
        maximum_elo_delta: 20,
        ..ErpsConfig::default()
    };
    config.deterministic_seed = 0x17_1000;
    let mut state = AuthorityState::new(config, Arc::new(crate::metrics::Metrics::default()));
    let server_id = ServerId::from_uuid(uuid::Uuid::from_u128(20_001));
    state
        .registry
        .register(
            GameServer {
                id: server_id,
                generation: ServerGeneration(1),
                endpoint: "game".into(),
                region: "tw".into(),
                modes: BTreeSet::from([MODE]),
                capacity_total: 100,
                capacity_used: 0,
                max_instances: 100,
                mode_costs: BTreeMap::from([(MODE, 1)]),
                last_heartbeat: now_ms(),
                health: Health::Healthy,
                failures: 0,
                instances: BTreeMap::new(),
            },
            ServerLimits {
                max_capacity: 100,
                max_instances: 100,
            },
        )
        .unwrap();
    let (control_tx, _control_rx) = mpsc::channel(1_024);
    state.controls.insert(server_id, control_tx);

    let mut seed = 0x005e_ed17_u64;
    let mut arrivals = Vec::with_capacity(PLAYERS);
    let mut original_ratings = BTreeMap::new();
    for index in 0..PLAYERS {
        let player = PlayerId::from_uuid(uuid::Uuid::from_u128(index as u128 + 1));
        let rating = 800 + (index / 100) as i32 * 100 + (next_random(&mut seed) % 11) as i32;
        original_ratings.insert(player, rating);
        arrivals.push(player);
    }
    for index in (1..arrivals.len()).rev() {
        let other = (next_random(&mut seed) as usize) % (index + 1);
        arrivals.swap(index, other);
    }

    let mut released = 0;
    let mut settled_players = BTreeSet::new();
    let mut pending = Vec::<ScheduledResult>::new();
    let mut matched = 0;
    let mut settled = 0;
    let mut total_rating_gap = 0u64;
    let mut maximum_rating_gap = 0i32;
    let mut maximum_concurrent = 0usize;
    let last_arrival_second = (PLAYERS - 1) / ARRIVALS_PER_SECOND;
    for second in 0..=(last_arrival_second as u64 + MATCH_DURATION_SECONDS) {
        let mut future = Vec::new();
        for result in pending.drain(..) {
            if result.due_second > second {
                future.push(result);
                continue;
            }
            for (slot, player) in result.teams.iter().enumerate() {
                assert_eq!(state.ratings[&(*player, MODE)], result.before[slot]);
            }
            state
                .finish_match(
                    server_id,
                    pb::MatchResult {
                        match_id: result.match_id.to_string(),
                        placements: result
                            .teams
                            .iter()
                            .enumerate()
                            .map(|(slot, player)| pb::PlayerPlacement {
                                player_id: player.to_string(),
                                rank: if slot == result.winner { 1 } else { 2 },
                            })
                            .collect(),
                    },
                )
                .unwrap();
            let expected = expected_after(result.before, result.winner);
            for (slot, player) in result.teams.iter().enumerate() {
                assert_eq!(state.ratings[&(*player, MODE)], expected[slot]);
                assert_eq!(state.completed_games[&(*player, MODE)], 1);
                assert_eq!(
                    state.client_state(*player).profile.unwrap().rating,
                    expected[slot]
                );
                assert_eq!(state.credit[player], 100);
                assert_eq!(
                    state.parties[&state.player_party[player]].state,
                    PartyState::Idle
                );
                assert!(settled_players.insert(*player), "player settled twice");
            }
            settled += 1;
        }
        pending = future;

        for _ in 0..ARRIVALS_PER_SECOND.min(PLAYERS - released) {
            let player = arrivals[released];
            let mut party = Party::new(player, &format!("Player{}", released + 1)).unwrap();
            party.id = PartyId::from_uuid(uuid::Uuid::from_u128(10_000 + released as u128));
            party.state = PartyState::Queued;
            let party_id = party.id;
            state.sessions.insert(format!("session-{released}"), player);
            state.player_party.insert(player, party_id);
            state
                .ratings
                .insert((player, MODE), original_ratings[&player]);
            state.credit.insert(player, 100);
            state.tickets.insert(
                party_id,
                TicketRecord {
                    id: TicketId::from_uuid(uuid::Uuid::from_u128(30_000 + released as u128)),
                    mode: MODE,
                    regions: vec!["tw".into()],
                    enqueued_at: second,
                    queued_since_ms: now_ms(),
                },
            );
            state.parties.insert(party_id, party);
            released += 1;
        }
        state.attempt_match_batch(MODE, "tw");

        let proposals: Vec<_> = state
            .proposals
            .iter()
            .filter_map(|(id, proposal)| {
                (proposal.state == ProposalState::AwaitingAccept).then_some(*id)
            })
            .collect();
        for proposal_id in proposals {
            let teams = state.proposals[&proposal_id].teams.clone();
            assert_eq!(teams.len(), 2);
            assert!(teams.iter().all(|team| team.len() == 1));
            let players = [teams[0][0], teams[1][0]];
            let before = [
                state.ratings[&(players[0], MODE)],
                state.ratings[&(players[1], MODE)],
            ];
            let gap = (before[0] - before[1]).abs();
            assert!(gap <= 20, "match paired Elo {before:?}, gap={gap}");
            maximum_rating_gap = maximum_rating_gap.max(gap);
            total_rating_gap += gap as u64;
            for player in players {
                state
                    .proposals
                    .get_mut(&proposal_id)
                    .unwrap()
                    .respond(proposal_id, player, true)
                    .unwrap();
            }
            let match_id = state.launch(proposal_id).unwrap();
            state
                .launch_result(
                    server_id,
                    pb::LaunchResult {
                        match_id: match_id.to_string(),
                        state: "accepted".into(),
                        endpoint: String::new(),
                        connection_token: String::new(),
                        reason: String::new(),
                    },
                )
                .unwrap();
            state
                .launch_result(
                    server_id,
                    pb::LaunchResult {
                        match_id: match_id.to_string(),
                        state: "ready".into(),
                        endpoint: "game".into(),
                        connection_token: format!("match-{matched}"),
                        reason: String::new(),
                    },
                )
                .unwrap();
            pending.push(ScheduledResult {
                due_second: second + MATCH_DURATION_SECONDS,
                match_id,
                teams: players,
                before,
                winner: (next_random(&mut seed) % 2) as usize,
            });
            matched += 1;
        }
        let server = &state.registry.servers[&server_id];
        assert_eq!(server.capacity_used as usize, pending.len());
        assert_eq!(server.instances.len(), pending.len());
        assert!(server.instances.len() <= server.max_instances as usize);
        maximum_concurrent = maximum_concurrent.max(pending.len());
    }
    assert_eq!(released, PLAYERS);
    assert_eq!(matched, PLAYERS / 2);
    assert_eq!(settled, PLAYERS / 2);
    assert_eq!(settled_players.len(), PLAYERS);
    assert!(pending.is_empty());
    assert!(state.tickets.is_empty());
    assert!(state.proposals.is_empty());
    assert!(state.launches.is_empty());
    assert_eq!(state.registry.servers[&server_id].capacity_used, 0);
    assert!(state.registry.servers[&server_id].instances.is_empty());
    assert_eq!(state.pending_profile_saves.len(), PLAYERS);

    // A later queue snapshot must use the updated rating, including ECS bucket metadata.
    let replay_players = [arrivals[0], arrivals[PLAYERS - 1]];
    for player in replay_players {
        let party_id = state.player_party[&player];
        state.parties.get_mut(&party_id).unwrap().state = PartyState::Queued;
        state.tickets.insert(
            party_id,
            TicketRecord {
                id: TicketId::new(),
                mode: MODE,
                regions: vec!["tw".into()],
                enqueued_at: 70,
                queued_since_ms: now_ms(),
            },
        );
    }
    state.rebuild_ecs();
    let snapshot = state.ecs_tickets(MODE, "tw");
    assert_eq!(snapshot.len(), 2);
    for ticket in &snapshot {
        let rating = state.ratings[&(ticket.members[0], MODE)];
        assert_eq!(ticket.effective_rating, rating);
        let entities = state.world.entities();
        let ticket_ids = state
            .world
            .read_storage::<crate::components::TicketIdentity>();
        let search_ranges = state.world.read_storage::<SearchRange>();
        let buckets = state.world.read_storage::<BucketOwner>();
        let (_, range, bucket) = (&entities, &ticket_ids, &search_ranges, &buckets)
            .join()
            .find_map(|(_, id, range, bucket)| (id.0 == ticket.id).then_some((id, range, bucket)))
            .unwrap();
        assert_eq!(range.minimum, rating - 20);
        assert_eq!(range.maximum, rating + 20);
        assert_eq!(bucket.bucket, rating.div_euclid(100));
    }
    println!(
        "ERPS_1000_PASS players={PLAYERS} arrivals_per_second={ARRIVALS_PER_SECOND} matches={matched} settled_after_seconds={MATCH_DURATION_SECONDS} max_elo_gap={maximum_rating_gap} mean_elo_gap={:.2} peak_instances={maximum_concurrent}",
        total_rating_gap as f64 / matched as f64
    );
}

#[test]
fn five_v_five_live_queue_releases_structure_after_sixty_seconds() {
    let mut state = AuthorityState::new(
        ErpsConfig::default(),
        Arc::new(crate::metrics::Metrics::default()),
    );
    let server_id = ServerId::from_uuid(uuid::Uuid::from_u128(40_001));
    state
        .registry
        .register(
            GameServer {
                id: server_id,
                generation: ServerGeneration(1),
                endpoint: "game".into(),
                region: "tw".into(),
                modes: BTreeSet::from([QueueMode::FiveVsFive]),
                capacity_total: 10,
                capacity_used: 0,
                max_instances: 10,
                mode_costs: BTreeMap::from([(QueueMode::FiveVsFive, 1)]),
                last_heartbeat: now_ms(),
                health: Health::Healthy,
                failures: 0,
                instances: BTreeMap::new(),
            },
            ServerLimits {
                max_capacity: 10,
                max_instances: 10,
            },
        )
        .unwrap();
    let (control_tx, _control_rx) = mpsc::channel(16);
    state.controls.insert(server_id, control_tx);

    let mut oldest_party = None;
    let mut next_player_id = 50_000u128;
    for (index, size) in [4, 1, 2, 2, 1].into_iter().enumerate() {
        let mut members = Vec::new();
        for _ in 0..size {
            let player = PlayerId::from_uuid(uuid::Uuid::from_u128(next_player_id));
            next_player_id += 1;
            state.sessions.insert(player.to_string(), player);
            state.ratings.insert(
                (player, QueueMode::FiveVsFive),
                if index < 2 { 1000 } else { 1012 },
            );
            members.push(player);
        }
        let mut party = Party::new(members[0], &format!("Group{index}")).unwrap();
        party.id = PartyId::from_uuid(uuid::Uuid::from_u128(60_000 + index as u128));
        party.members = members.clone();
        party.state = PartyState::Queued;
        for player in members {
            state.player_party.insert(player, party.id);
        }
        if index == 0 {
            oldest_party = Some(party.id);
        }
        state.tickets.insert(
            party.id,
            TicketRecord {
                id: TicketId::from_uuid(uuid::Uuid::from_u128(70_000 + index as u128)),
                mode: QueueMode::FiveVsFive,
                regions: vec!["tw".into()],
                enqueued_at: index as u64,
                queued_since_ms: now_ms(),
            },
        );
        state.parties.insert(party.id, party);
    }
    let oldest_party = oldest_party.unwrap();
    assert_eq!(state.attempt_match_batch(QueueMode::FiveVsFive, "tw"), 0);
    state
        .tickets
        .get_mut(&oldest_party)
        .unwrap()
        .queued_since_ms = now_ms() - 59_000;
    assert_eq!(state.attempt_match_batch(QueueMode::FiveVsFive, "tw"), 0);
    state
        .tickets
        .get_mut(&oldest_party)
        .unwrap()
        .queued_since_ms = now_ms() - 60_000;
    assert_eq!(state.attempt_match_batch(QueueMode::FiveVsFive, "tw"), 1);
    let proposal = state.proposals.values().next().unwrap();
    assert_eq!(proposal.teams.len(), 2);
    let structures = proposal
        .teams
        .iter()
        .map(|team| {
            let mut sizes = state
                .parties
                .values()
                .filter(|party| party.members.iter().all(|player| team.contains(player)))
                .map(|party| party.members.len())
                .collect::<Vec<_>>();
            sizes.sort_unstable_by(|a, b| b.cmp(a));
            sizes
        })
        .collect::<Vec<_>>();
    assert!(structures.contains(&vec![4, 1]));
    assert!(structures.contains(&vec![2, 2, 1]));
    assert_eq!(state.tickets.len(), 0);

    let proposal_id = *state.proposals.keys().next().unwrap();
    let teams = state.proposals[&proposal_id].teams.clone();
    let seasoned = [teams[0][0], teams[1][0]];
    let before = teams
        .iter()
        .flatten()
        .map(|player| (*player, state.ratings[&(*player, QueueMode::FiveVsFive)]))
        .collect::<BTreeMap<_, _>>();
    for player in seasoned {
        state
            .completed_games
            .insert((player, QueueMode::FiveVsFive), 10);
    }
    for player in teams.iter().flatten() {
        state
            .proposals
            .get_mut(&proposal_id)
            .unwrap()
            .respond(proposal_id, *player, true)
            .unwrap();
    }
    let match_id = state.launch(proposal_id).unwrap();
    for lifecycle in ["accepted", "ready"] {
        state
            .launch_result(
                server_id,
                pb::LaunchResult {
                    match_id: match_id.to_string(),
                    state: lifecycle.into(),
                    endpoint: "game".into(),
                    connection_token: "token".into(),
                    reason: String::new(),
                },
            )
            .unwrap();
    }
    state
        .finish_match(
            server_id,
            pb::MatchResult {
                match_id: match_id.to_string(),
                placements: teams
                    .iter()
                    .enumerate()
                    .flat_map(|(index, team)| {
                        team.iter().map(move |player| pb::PlayerPlacement {
                            player_id: player.to_string(),
                            rank: index as u32 + 1,
                        })
                    })
                    .collect(),
            },
        )
        .unwrap();
    for (team_index, team) in teams.iter().enumerate() {
        let other_mean = teams[1 - team_index]
            .iter()
            .map(|player| before[player])
            .sum::<i32>()
            / 5;
        for player in team {
            let current = before[player];
            let old_games = if seasoned.contains(player) { 10 } else { 0 };
            let probability = 1.0 / (1.0 + 10f64.powf(f64::from(other_mean - current) / 400.0));
            let actual = if team_index == 0 { 1.0 } else { 0.0 };
            let k = if old_games < 10 { 40.0 } else { 20.0 };
            let expected = current + (k * (actual - probability)).round() as i32;
            assert_eq!(state.ratings[&(*player, QueueMode::FiveVsFive)], expected);
            assert_eq!(
                state.completed_games[&(*player, QueueMode::FiveVsFive)],
                old_games + 1
            );
        }
    }
    assert_eq!(state.registry.servers[&server_id].capacity_used, 0);
    assert!(state.registry.servers[&server_id].instances.is_empty());
}
