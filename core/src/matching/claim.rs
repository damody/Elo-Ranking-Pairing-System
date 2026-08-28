use super::Candidate;
use crate::id::TicketId;
use std::collections::BTreeSet;
#[derive(Default)]
pub struct Claims {
    active: BTreeSet<TicketId>,
}
impl Claims {
    pub fn commit(&mut self, c: &Candidate, valid: impl Fn(TicketId) -> bool) -> bool {
        let ids = c.stable_ticket_ids();
        if ids.iter().any(|id| self.active.contains(id) || !valid(*id)) {
            return false;
        }
        self.active.extend(ids);
        true
    }
    pub fn release(&mut self, ids: &[TicketId]) {
        for id in ids {
            self.active.remove(id);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_fails() {
        let id = TicketId::new();
        let c = Candidate {
            tickets: vec![id],
            teams: vec![],
            oldest_enqueued_at: 0,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut x = Claims::default();
        assert!(x.commit(&c, |_| true));
        assert!(!x.commit(&c, |_| true));
    }
}
