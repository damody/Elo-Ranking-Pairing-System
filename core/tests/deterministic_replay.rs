use erps::{
    components::QueueMode,
    id::*,
    matching::{dispatcher, one_v_one, snapshot::CandidateSnapshot, PartyTicket},
};
fn fixture() -> CandidateSnapshot {
    CandidateSnapshot::new(
        (0..20)
            .map(|i| PartyTicket {
                id: TicketId::from_uuid(uuid::Uuid::from_u128(i + 1)),
                party: PartyId::from_uuid(uuid::Uuid::from_u128(100 + i)),
                members: vec![PlayerId::from_uuid(uuid::Uuid::from_u128(1000 + i))],
                ratings: vec![1000 + i as i32],
                effective_rating: 1000 + i as i32,
                enqueued_at: i as u64,
                revision: 0,
                region: "tw".into(),
                mode: QueueMode::OneVsOne,
                search_delta: 100,
                wait_seconds: 0,
            })
            .collect(),
    )
}
#[test]
fn worker_counts_produce_same_candidates() {
    let s = fixture();
    assert_eq!(
        dispatcher::generate(&s, 1, 1000, one_v_one::build),
        dispatcher::generate(&s, 8, 1000, one_v_one::build)
    );
}
