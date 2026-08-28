use crate::{
    components::AcceptState,
    credit::CreditCause,
    id::{PartyId, PlayerId, ProposalId, TicketId},
    matching::Candidate,
};
use std::collections::BTreeMap;
use thiserror::Error;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposalState {
    AwaitingAccept,
    AwaitingPlacement,
    Cancelled,
}
#[derive(Clone, Debug)]
pub struct Proposal {
    pub id: ProposalId,
    pub tickets: Vec<TicketId>,
    pub teams: Vec<Vec<PlayerId>>,
    pub parties: BTreeMap<PlayerId, PartyId>,
    pub responses: BTreeMap<PlayerId, AcceptState>,
    pub deadline: u64,
    pub state: ProposalState,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProposalError {
    #[error("stale proposal")]
    Stale,
    #[error("unknown player")]
    UnknownPlayer,
}
impl Proposal {
    pub fn from_candidate(
        c: &Candidate,
        parties: BTreeMap<PlayerId, PartyId>,
        now: u64,
        timeout: u64,
    ) -> Self {
        let responses = c
            .teams
            .iter()
            .flatten()
            .map(|p| (*p, AcceptState::Pending))
            .collect();
        Self {
            id: ProposalId::new(),
            tickets: c.tickets.clone(),
            teams: c.teams.clone(),
            parties,
            responses,
            deadline: now.saturating_add(timeout),
            state: ProposalState::AwaitingAccept,
        }
    }
    pub fn respond(
        &mut self,
        id: ProposalId,
        player: PlayerId,
        accept: bool,
    ) -> Result<ProposalState, ProposalError> {
        if id != self.id || self.state != ProposalState::AwaitingAccept {
            return Err(ProposalError::Stale);
        }
        let state = self
            .responses
            .get_mut(&player)
            .ok_or(ProposalError::UnknownPlayer)?;
        let value = if accept {
            AcceptState::Accepted
        } else {
            AcceptState::Rejected
        };
        if *state == AcceptState::Pending {
            *state = value;
        }
        if self.responses.values().any(|s| *s == AcceptState::Rejected) {
            self.state = ProposalState::Cancelled
        } else if self.responses.values().all(|s| *s == AcceptState::Accepted) {
            self.state = ProposalState::AwaitingPlacement
        }
        Ok(self.state)
    }
    pub fn expire(&mut self, now: u64) -> bool {
        if self.state != ProposalState::AwaitingAccept || now <= self.deadline {
            return false;
        }
        for s in self.responses.values_mut() {
            if *s == AcceptState::Pending {
                *s = AcceptState::TimedOut
            }
        }
        self.state = ProposalState::Cancelled;
        true
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequeueDecision {
    pub player: PlayerId,
    pub requeue: bool,
    pub party_not_ready: bool,
    pub credit_cause: Option<CreditCause>,
}
pub fn cancellation_decisions(p: &Proposal, infra: bool) -> Vec<RequeueDecision> {
    let failed_parties: Vec<_> = p
        .responses
        .iter()
        .filter(|(_, s)| matches!(s, AcceptState::Rejected | AcceptState::TimedOut))
        .filter_map(|(player, _)| p.parties.get(player))
        .collect();
    p.responses
        .iter()
        .map(|(player, state)| {
            let failed = matches!(state, AcceptState::Rejected | AcceptState::TimedOut);
            let affected = p
                .parties
                .get(player)
                .is_some_and(|party| failed_parties.contains(&party));
            RequeueDecision {
                player: *player,
                requeue: !affected && !failed,
                party_not_ready: affected,
                credit_cause: if infra {
                    Some(CreditCause::InfrastructureFailure)
                } else if *state == AcceptState::Rejected {
                    Some(CreditCause::Rejected)
                } else if *state == AcceptState::TimedOut {
                    Some(CreditCause::TimedOut)
                } else {
                    None
                },
            }
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::Candidate;
    #[test]
    fn all_accept_before_placement() {
        let players = [PlayerId::new(), PlayerId::new()];
        let c = Candidate {
            tickets: vec![TicketId::new()],
            teams: vec![vec![players[0]], vec![players[1]]],
            oldest_enqueued_at: 0,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut p = Proposal::from_candidate(&c, BTreeMap::new(), 10, 15);
        assert_eq!(
            p.respond(p.id, players[0], true).unwrap(),
            ProposalState::AwaitingAccept
        );
        assert_eq!(
            p.respond(p.id, players[1], true).unwrap(),
            ProposalState::AwaitingPlacement
        );
        assert_eq!(
            p.respond(ProposalId::new(), players[0], true),
            Err(ProposalError::Stale)
        );
    }
    #[test]
    fn timeout_marks_pending() {
        let player = PlayerId::new();
        let c = Candidate {
            tickets: vec![],
            teams: vec![vec![player]],
            oldest_enqueued_at: 0,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut p = Proposal::from_candidate(&c, BTreeMap::new(), 0, 15);
        assert!(p.expire(16));
        assert_eq!(p.responses[&player], AcceptState::TimedOut);
    }
}
