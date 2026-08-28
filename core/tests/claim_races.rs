use erps::{
    id::{PlayerId, TicketId},
    matching::{claim::Claims, Candidate},
};
fn candidate(id: TicketId, shard: u64) -> Candidate {
    Candidate {
        tickets: vec![id],
        teams: vec![vec![PlayerId::new()]],
        oldest_enqueued_at: 1,
        quality_key: (0, 0, 0),
        owner_shard: shard,
    }
}
#[test]
fn halo_duplicate_can_only_commit_once() {
    let id = TicketId::new();
    let mut claims = Claims::default();
    assert!(claims.commit(&candidate(id, 1), |_| true));
    assert!(!claims.commit(&candidate(id, 2), |_| true));
}
#[test]
fn cancellation_wins_revalidation_race() {
    let id = TicketId::new();
    let mut claims = Claims::default();
    assert!(!claims.commit(&candidate(id, 1), |ticket| ticket != id));
}
