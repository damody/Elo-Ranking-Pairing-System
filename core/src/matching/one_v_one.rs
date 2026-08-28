use super::{Candidate, PartyTicket};
pub fn build(t: &[PartyTicket]) -> Vec<Candidate> {
    let mut ordered: Vec<_> = t
        .iter()
        .filter(|ticket| ticket.members.len() == 1)
        .collect();
    ordered.sort_by_key(|ticket| (ticket.effective_rating, ticket.enqueued_at, ticket.id));
    let mut out = Vec::new();
    for pair in ordered.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a.region != b.region {
            continue;
        }
        let diff = (a.effective_rating - b.effective_rating).abs();
        if diff > a.search_delta.min(b.search_delta) {
            continue;
        }
        out.push(Candidate {
            tickets: vec![a.id, b.id],
            teams: vec![a.members.clone(), b.members.clone()],
            oldest_enqueued_at: a.enqueued_at.min(b.enqueued_at),
            quality_key: (diff, 0, 0),
            owner_shard: 0,
        });
    }
    out
}
