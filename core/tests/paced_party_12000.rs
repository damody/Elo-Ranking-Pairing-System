use erps::{
    components::QueueMode,
    id::{PartyId, PlayerId, TicketId},
    matching::{self, dispatcher, five_v_five, PartyTicket},
    rating::{self, RatingPolicy},
};
use std::{
    collections::{BTreeSet, VecDeque},
    time::Instant,
};

const PLAYERS: usize = 10_000;
const SIMULATED_SECONDS: u64 = 12_000;
const ARRIVALS_PER_SECOND: usize = 17;
const MATCH_SECONDS: u64 = 10;
const SEARCH_DELTA: i32 = 200;
const PARTITIONS: [&[usize]; 4] = [&[5], &[4, 1], &[3, 2], &[2, 2, 1]];

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0 >> 32
    }

    fn index(&mut self, count: usize) -> usize {
        (self.next() as usize) % count
    }
}

struct RunningMatch {
    due_second: u64,
    teams: [Vec<usize>; 2],
    before: [Vec<i32>; 2],
}

fn player_id(index: usize) -> PlayerId {
    PlayerId::from_uuid(uuid::Uuid::from_u128(index as u128 + 1))
}

fn player_index(id: PlayerId) -> usize {
    id.as_uuid().as_u128() as usize - 1
}

fn nearest_idle_player(idle: &BTreeSet<(i32, usize)>, rating: i32) -> usize {
    let lower = idle.range(..=(rating, usize::MAX)).next_back().copied();
    let upper = idle.range((rating, 0)..).next().copied();
    [lower, upper]
        .into_iter()
        .flatten()
        .min_by_key(|(candidate_rating, player)| {
            (
                (i64::from(*candidate_rating) - i64::from(rating)).abs(),
                *player,
            )
        })
        .expect("available rating neighbor")
        .1
}

fn team_mean(ratings: &[i32]) -> i32 {
    ratings.iter().sum::<i32>() / ratings.len() as i32
}

fn independently_expected_rating(
    current: i32,
    opponent_mean: i32,
    won: bool,
    completed_games: u32,
) -> i32 {
    let probability = 1.0 / (1.0 + 10f64.powf(f64::from(opponent_mean - current) / 400.0));
    let actual = if won { 1.0 } else { 0.0 };
    let k = if completed_games < 10 { 40.0 } else { 20.0 };
    current + (k * (actual - probability)).round().clamp(-40.0, 40.0) as i32
}

fn build_match(
    mut players: Vec<usize>,
    ratings: &[i32],
    second: u64,
    match_number: usize,
    rng: &mut Rng,
    partition_counts: &mut [usize; 4],
) -> RunningMatch {
    for index in (1..players.len()).rev() {
        let other = rng.index(index + 1);
        players.swap(index, other);
    }
    let partition_index = rng.index(PARTITIONS.len());
    let partition = PARTITIONS[partition_index];
    partition_counts[partition_index] += 1;
    let mut tickets = Vec::new();
    let mut offset = 0;
    for _ in 0..2 {
        for &size in partition {
            let members = players[offset..offset + size]
                .iter()
                .copied()
                .map(player_id)
                .collect::<Vec<_>>();
            let party_ratings = players[offset..offset + size]
                .iter()
                .map(|player| ratings[*player])
                .collect::<Vec<_>>();
            let serial = match_number as u128 * 10 + tickets.len() as u128 + 1;
            tickets.push(PartyTicket {
                id: TicketId::from_uuid(uuid::Uuid::from_u128(1_000_000 + serial)),
                party: PartyId::from_uuid(uuid::Uuid::from_u128(2_000_000 + serial)),
                members,
                effective_rating: matching::effective_rating_for_mode(
                    QueueMode::FiveVsFive,
                    &party_ratings,
                    0,
                    0,
                    SEARCH_DELTA,
                )
                .unwrap_or_else(|| {
                    panic!("random party rating spread is bounded: {party_ratings:?}")
                }),
                ratings: party_ratings,
                enqueued_at: second,
                revision: 1,
                region: "tw".into(),
                mode: QueueMode::FiveVsFive,
                search_delta: SEARCH_DELTA,
                wait_seconds: 0,
            });
            offset += size;
        }
    }
    assert_eq!(offset, 10);
    let mut candidates = five_v_five::build(&tickets, 128);
    dispatcher::sort_candidates(&mut candidates);
    let candidate = candidates
        .into_iter()
        .next()
        .expect("a mirrored party composition with nearby Elo must match");
    assert_eq!(candidate.teams.len(), 2);
    assert!(candidate.teams.iter().all(|team| team.len() == 5));
    let structures = candidate
        .teams
        .iter()
        .map(|team| {
            let mut sizes = tickets
                .iter()
                .filter(|ticket| ticket.members.iter().all(|player| team.contains(player)))
                .map(|ticket| ticket.members.len())
                .collect::<Vec<_>>();
            sizes.sort_unstable();
            sizes
        })
        .collect::<Vec<_>>();
    assert_eq!(structures[0], structures[1], "party shapes must mirror");
    let teams: [Vec<PlayerId>; 2] = candidate.teams.try_into().expect("two 5v5 teams");
    let teams = teams.map(|team| team.into_iter().map(player_index).collect());
    let before = teams
        .each_ref()
        .map(|team: &Vec<usize>| team.iter().map(|player| ratings[*player]).collect());
    RunningMatch {
        due_second: second + MATCH_SECONDS,
        teams,
        before,
    }
}

#[test]
fn ten_thousand_players_arrive_seventeen_per_second_for_twelve_thousand_seconds() {
    let started = Instant::now();
    let mut rng = Rng(0x1712_0001_0000);
    let mut ratings = (0..PLAYERS)
        .map(|_| 900 + rng.index(201) as i32)
        .collect::<Vec<_>>();
    let initial_ratings = ratings.clone();
    let mut order = (0..PLAYERS).collect::<Vec<_>>();
    for index in (1..order.len()).rev() {
        let other = rng.index(index + 1);
        order.swap(index, other);
    }
    let mut idle_order = VecDeque::from(
        order
            .into_iter()
            .map(|player| (player, 0u32))
            .collect::<Vec<_>>(),
    );
    let mut idle_by_rating = (0..PLAYERS)
        .map(|player| (ratings[player], player))
        .collect::<BTreeSet<_>>();
    let mut idle_generation = vec![0u32; PLAYERS];
    let mut waiting = VecDeque::new();
    let mut running = VecDeque::<RunningMatch>::new();
    let mut state = vec![0u8; PLAYERS]; // 0 idle, 1 waiting, 2 in a match.
    let mut completed = vec![0u32; PLAYERS];
    let mut partition_counts = [0usize; 4];
    let mut arrivals = 0usize;
    let mut matches = 0usize;
    let mut settled = 0usize;
    let mut elo_updates = 0usize;
    let mut rematches_using_updated_elo = 0usize;
    let mut maximum_team_mean_gap = 0i32;
    let mut total_team_mean_gap = 0u64;
    let mut peak_running = 0usize;
    let policy = RatingPolicy::default();

    for second in 0..SIMULATED_SECONDS + MATCH_SECONDS {
        while running
            .front()
            .is_some_and(|battle| battle.due_second == second)
        {
            let battle = running.pop_front().unwrap();
            let winner = rng.index(2);
            let winner_completed = battle.teams[winner]
                .iter()
                .map(|player| completed[*player])
                .collect::<Vec<_>>();
            let loser_completed = battle.teams[1 - winner]
                .iter()
                .map(|player| completed[*player])
                .collect::<Vec<_>>();
            let (next_winners, next_losers) = rating::team_update_with_completed(
                &battle.before[winner],
                &battle.before[1 - winner],
                &winner_completed,
                &loser_completed,
                policy,
            );
            for team in 0..2 {
                let next = if team == winner {
                    &next_winners
                } else {
                    &next_losers
                };
                let opponent_mean = team_mean(&battle.before[1 - team]);
                for (slot, &player) in battle.teams[team].iter().enumerate() {
                    assert_eq!(state[player], 2, "player is no longer in this match");
                    assert_eq!(ratings[player], battle.before[team][slot]);
                    assert_eq!(
                        next[slot],
                        independently_expected_rating(
                            ratings[player],
                            opponent_mean,
                            team == winner,
                            completed[player],
                        ),
                        "5v5 Elo differs from the independent formula"
                    );
                    assert!(if team == winner {
                        next[slot] > ratings[player]
                    } else {
                        next[slot] < ratings[player]
                    });
                    ratings[player] = next[slot];
                    completed[player] += 1;
                    elo_updates += 1;
                    state[player] = 0;
                    assert!(idle_by_rating.insert((ratings[player], player)));
                    idle_generation[player] += 1;
                    idle_order.push_back((player, idle_generation[player]));
                }
            }
            settled += 1;
        }

        if second < SIMULATED_SECONDS {
            for _ in 0..ARRIVALS_PER_SECOND {
                let player = if let Some(&anchor) = waiting.front() {
                    nearest_idle_player(&idle_by_rating, ratings[anchor])
                } else {
                    loop {
                        let (candidate, generation) = idle_order
                            .pop_front()
                            .expect("10,000-player pool has available players");
                        if generation == idle_generation[candidate]
                            && idle_by_rating.contains(&(ratings[candidate], candidate))
                        {
                            break candidate;
                        }
                    }
                };
                assert!(idle_by_rating.remove(&(ratings[player], player)));
                assert_eq!(state[player], 0, "player entered queue twice");
                state[player] = 1;
                waiting.push_back(player);
                arrivals += 1;
            }
            while waiting.len() >= 10 {
                let group = (0..10)
                    .map(|_| waiting.pop_front().unwrap())
                    .collect::<Vec<_>>();
                let battle = build_match(
                    group,
                    &ratings,
                    second,
                    matches,
                    &mut rng,
                    &mut partition_counts,
                );
                let gap = (team_mean(&battle.before[0]) - team_mean(&battle.before[1])).abs();
                assert!(gap <= 50, "opposing teams have excessive Elo gap: {gap}");
                maximum_team_mean_gap = maximum_team_mean_gap.max(gap);
                total_team_mean_gap += gap as u64;
                for team in &battle.teams {
                    for &player in team {
                        assert_eq!(state[player], 1, "party roster reused a player");
                        rematches_using_updated_elo += usize::from(
                            completed[player] > 0 && ratings[player] != initial_ratings[player],
                        );
                        state[player] = 2;
                    }
                }
                running.push_back(battle);
                matches += 1;
            }
            peak_running = peak_running.max(running.len());
        }
    }

    assert_eq!(arrivals, SIMULATED_SECONDS as usize * ARRIVALS_PER_SECOND);
    assert_eq!(matches, arrivals / 10);
    assert_eq!(settled, matches);
    assert_eq!(elo_updates, arrivals);
    assert!(waiting.is_empty());
    assert!(running.is_empty());
    assert_eq!(idle_by_rating.len(), PLAYERS);
    assert!(state.iter().all(|value| *value == 0));
    assert!(partition_counts.iter().all(|count| *count > 0));
    assert!(completed.iter().all(|count| *count >= 2));
    assert!(rematches_using_updated_elo > PLAYERS);
    assert!(
        peak_running <= 18,
        "10-second matches exceeded admission rate"
    );
    println!(
        "ERPS_PARTY_12000_PASS players={PLAYERS} simulated_seconds={SIMULATED_SECONDS} arrivals_per_second={ARRIVALS_PER_SECOND} admissions={arrivals} matches={matches} settled={settled} match_duration_seconds={MATCH_SECONDS} elo_updates={elo_updates} rematches_using_updated_elo={rematches_using_updated_elo} min_games={} max_games={} max_team_mean_elo_gap={maximum_team_mean_gap} mean_team_mean_elo_gap={:.2} peak_running={peak_running} partitions={partition_counts:?} elapsed_ms={}",
        completed.iter().min().unwrap(),
        completed.iter().max().unwrap(),
        total_team_mean_gap as f64 / matches as f64,
        started.elapsed().as_millis(),
    );
}
