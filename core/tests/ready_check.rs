use erps::{
    components::AcceptState,
    id::{PartyId, PlayerId, TicketId},
    matching::Candidate,
    proposal::{cancellation_decisions, Proposal, ProposalError, ProposalState},
};
use std::collections::BTreeMap;
fn fixture() -> (Proposal, [PlayerId; 3]) {
    let p = [PlayerId::new(), PlayerId::new(), PlayerId::new()];
    let party = PartyId::new();
    let candidate = Candidate {
        tickets: vec![TicketId::new()],
        teams: vec![vec![p[0]], vec![p[1]], vec![p[2]]],
        oldest_enqueued_at: 7,
        quality_key: (0, 0, 0),
        owner_shard: 0,
    };
    let owners = BTreeMap::from([(p[0], party), (p[1], party), (p[2], PartyId::new())]);
    (Proposal::from_candidate(&candidate, owners, 100, 15), p)
}
#[test]
fn mixed_party_failure_preserves_unaffected_solo() {
    let (mut proposal, p) = fixture();
    proposal.respond(proposal.id, p[0], false).unwrap();
    let decisions = cancellation_decisions(&proposal, false);
    assert!(decisions.iter().find(|v| v.player == p[2]).unwrap().requeue);
    assert!(decisions
        .iter()
        .filter(|v| v.player == p[0] || v.player == p[1])
        .all(|v| v.party_not_ready));
}
#[test]
fn deadline_is_exclusive_and_late_accept_is_stale() {
    let (mut proposal, p) = fixture();
    assert!(!proposal.expire(115));
    assert!(proposal.expire(116));
    assert_eq!(proposal.responses[&p[0]], AcceptState::TimedOut);
    assert_eq!(
        proposal.respond(proposal.id, p[0], true),
        Err(ProposalError::Stale)
    );
}
#[test]
fn duplicate_accept_is_idempotent() {
    let (mut proposal, p) = fixture();
    assert_eq!(
        proposal.respond(proposal.id, p[0], true).unwrap(),
        ProposalState::AwaitingAccept
    );
    assert_eq!(
        proposal.respond(proposal.id, p[0], true).unwrap(),
        ProposalState::AwaitingAccept
    );
}
