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
        effective_rating: rating,
        enqueued_at: at,
        revision: 0,
        region: "tw".into(),
        mode,
        search_delta: 600,
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
        ticket(5, 1000, 0, QueueMode::FiveVsFive),
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
proptest! {#[test]fn ffa_never_emits_wrong_team_count(sizes in prop::collection::vec(1usize..=4,1..10)){let tickets:Vec<_>=sizes.into_iter().map(|s|ticket(s,1000,0,QueueMode::FreeForAll)).collect();for c in free_for_all::build(&tickets,100){prop_assert_eq!(c.teams.len(),8);prop_assert!(c.teams.iter().all(|t|t.len()==1));}}}
