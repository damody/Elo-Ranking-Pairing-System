use erps::{
    components::QueueMode,
    id::*,
    matching::{self, five_v_five, free_for_all, one_v_one, PartyTicket},
};
use proptest::prelude::*;
fn ticket(size: usize, rating: i32, at: u64, mode: QueueMode) -> PartyTicket {
    PartyTicket {
        id: TicketId::new(),
        party: PartyId::new(),
        members: (0..size).map(|_| PlayerId::new()).collect(),
        ratings: vec![rating; size],
        effective_rating: matching::effective_rating_for_mode(
            mode,
            &vec![rating; size],
            10,
            1,
            600,
        )
        .unwrap(),
        enqueued_at: at,
        revision: 0,
        region: "tw".into(),
        mode,
        search_delta: 600,
        wait_seconds: 0,
    }
}
#[test]
fn modes_build_exact_rosters() {
    let one = vec![
        ticket(1, 1000, 0, QueueMode::OneVsOne),
        ticket(1, 1010, 1, QueueMode::OneVsOne),
    ];
    assert_eq!(
        one_v_one::build(&one)[0]
            .teams
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    let five = vec![
        ticket(3, 1000, 0, QueueMode::FiveVsFive),
        ticket(2, 1000, 0, QueueMode::FiveVsFive),
        ticket(3, 1000, 0, QueueMode::FiveVsFive),
        ticket(2, 1000, 0, QueueMode::FiveVsFive),
    ];
    let c = five_v_five::build(&five, 100).remove(0);
    assert_eq!(c.teams.iter().map(Vec::len).collect::<Vec<_>>(), vec![5, 5]);
    let ffa = vec![
        ticket(4, 1000, 0, QueueMode::FreeForAll),
        ticket(2, 1000, 0, QueueMode::FreeForAll),
        ticket(2, 1000, 0, QueueMode::FreeForAll),
    ];
    assert_eq!(free_for_all::build(&ffa, 100)[0].teams.len(), 8);
}
#[test]
fn effective_rating_rejects_spread() {
    assert!(matching::effective_rating(&[500, 1500], 0, 0, 600).is_none());
    assert_eq!(
        matching::effective_rating(&[1000, 1100], 10, 1, 600),
        Some(1061)
    );
}

#[test]
fn effective_rating_clamps_extreme_adjustments_without_overflow() {
    assert_eq!(
        matching::effective_rating(&[i32::MAX, i32::MAX], i32::MAX, i32::MAX, 0),
        Some(i32::MAX)
    );
    assert_eq!(
        matching::effective_rating(&[i32::MIN, i32::MIN], i32::MIN, i32::MIN, 0),
        Some(i32::MIN)
    );
}

#[test]
fn five_v_five_party_advantage_uses_exact_size_schedule() {
    for (size, bonus) in [(1, 0), (2, 5), (3, 10), (4, 20), (5, 30)] {
        assert_eq!(matching::five_v_five_party_bonus(size), bonus);
        assert_eq!(
            matching::effective_rating_for_mode(
                QueueMode::FiveVsFive,
                &vec![1000; size],
                10,
                0,
                600,
            ),
            Some(1000 + bonus)
        );
    }
}

fn team_structure(team: &[PlayerId], tickets: &[PartyTicket]) -> Vec<usize> {
    let mut sizes = tickets
        .iter()
        .filter(|ticket| ticket.members.iter().all(|player| team.contains(player)))
        .map(|ticket| ticket.members.len())
        .collect::<Vec<_>>();
    sizes.sort_unstable_by(|a, b| b.cmp(a));
    sizes
}

#[test]
fn five_v_five_mirrors_four_plus_one_and_two_plus_two_plus_one_before_sixty_seconds() {
    let sizes = [4, 1, 4, 1, 2, 2, 1, 2, 2, 1];
    let tickets = sizes
        .into_iter()
        .map(|size| ticket(size, 1000, 0, QueueMode::FiveVsFive))
        .collect::<Vec<_>>();
    let candidates = five_v_five::build(&tickets, 10_000);
    assert!(!candidates.is_empty());
    let mut found_four_plus_one = false;
    let mut found_two_plus_two_plus_one = false;
    for candidate in candidates {
        let left = team_structure(&candidate.teams[0], &tickets);
        let right = team_structure(&candidate.teams[1], &tickets);
        assert_eq!(left, right, "fresh 5v5 teams must mirror party sizes");
        found_four_plus_one |= left == [4, 1];
        found_two_plus_two_plus_one |= left == [2, 2, 1];
    }
    assert!(found_four_plus_one);
    assert!(found_two_plus_two_plus_one);
}

#[test]
fn five_v_five_cross_structure_waits_sixty_seconds_and_requires_compensating_elo() {
    let mut tickets = [4, 1, 2, 2, 1]
        .into_iter()
        .enumerate()
        .map(|(index, size)| {
            ticket(
                size,
                if index < 2 { 1000 } else { 1011 },
                0,
                QueueMode::FiveVsFive,
            )
        })
        .collect::<Vec<_>>();
    assert!(five_v_five::build(&tickets, 100).is_empty());
    tickets[0].wait_seconds = 59;
    assert!(five_v_five::build(&tickets, 100).is_empty());
    tickets[0].wait_seconds = 60;
    assert!(
        five_v_five::build(&tickets, 100).is_empty(),
        "11 raw Elo points cannot offset the 12 point weighted party advantage"
    );
    for ticket in &mut tickets[2..] {
        ticket.ratings.fill(1012);
        ticket.effective_rating =
            matching::effective_rating_for_mode(QueueMode::FiveVsFive, &ticket.ratings, 10, 1, 600)
                .unwrap();
    }
    assert!(
        !five_v_five::build(&tickets, 100).is_empty(),
        "12 higher raw Elo points should permit the cross-structure match"
    );
}

proptest! {
    #[test]
    fn random_five_v_five_partitions_never_cross_structure_before_sixty_seconds(
        left in 0usize..7,
        right in 0usize..7,
    ) {
        let partitions: [&[usize]; 7] = [
            &[5], &[4, 1], &[3, 2], &[3, 1, 1],
            &[2, 2, 1], &[2, 1, 1, 1], &[1, 1, 1, 1, 1],
        ];
        let tickets = partitions[left]
            .iter()
            .chain(partitions[right])
            .map(|size| ticket(*size, 1000, 0, QueueMode::FiveVsFive))
            .collect::<Vec<_>>();
        let candidates = five_v_five::build(&tickets, 10_000);
        for candidate in &candidates {
            prop_assert_eq!(candidate.teams.len(), 2);
            prop_assert!(candidate.teams.iter().all(|team| team.len() == 5));
            prop_assert_eq!(
                team_structure(&candidate.teams[0], &tickets),
                team_structure(&candidate.teams[1], &tickets)
            );
            let players = candidate.teams.iter().flatten().copied().collect::<std::collections::BTreeSet<_>>();
            prop_assert_eq!(players.len(), 10);
        }
        if left == right {
            prop_assert!(!candidates.is_empty());
        }
    }
}
proptest! {#[test]fn ffa_never_emits_wrong_team_count(sizes in prop::collection::vec(1usize..=4,1..10)){let tickets:Vec<_>=sizes.into_iter().map(|s|ticket(s,1000,0,QueueMode::FreeForAll)).collect();for c in free_for_all::build(&tickets,100){prop_assert_eq!(c.teams.len(),8);prop_assert!(c.teams.iter().all(|t|t.len()==1));}}}
