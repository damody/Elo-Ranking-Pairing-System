use super::PartyTicket;
#[derive(Clone, Debug, Default)]
pub struct CandidateSnapshot {
    pub tickets: Vec<PartyTicket>,
}
impl CandidateSnapshot {
    pub fn new(mut tickets: Vec<PartyTicket>) -> Self {
        tickets.sort_by_key(|t| {
            (
                t.region.clone(),
                t.mode,
                t.effective_rating,
                t.enqueued_at,
                t.id,
            )
        });
        Self { tickets }
    }
}
