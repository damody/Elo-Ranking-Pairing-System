//! Party validation and lifecycle.
use crate::{
    components::{PartyState, QueueMode},
    id::{PartyId, PlayerId},
};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartyError {
    #[error("invalid party name")]
    InvalidName,
    #[error("leader required")]
    LeaderRequired,
    #[error("party revision conflict")]
    RevisionConflict,
    #[error("party is frozen")]
    Frozen,
    #[error("invalid party size")]
    InvalidSize,
    #[error("no common region")]
    NoCommonRegion,
}
pub fn normalize_name(input: &str) -> Result<String, PartyError> {
    let normalized: String = input.nfc().collect();
    let count = normalized.chars().count();
    if !(1..=24).contains(&count) || !normalized.chars().all(char::is_alphanumeric) {
        return Err(PartyError::InvalidName);
    }
    Ok(normalized)
}
#[derive(Clone, Debug)]
pub struct Party {
    pub id: PartyId,
    pub name: String,
    pub leader: PlayerId,
    pub members: Vec<PlayerId>,
    pub revision: u64,
    pub state: PartyState,
}
impl Party {
    pub fn new(leader: PlayerId, name: &str) -> Result<Self, PartyError> {
        Ok(Self {
            id: PartyId::new(),
            name: normalize_name(name)?,
            leader,
            members: vec![leader],
            revision: 0,
            state: PartyState::Idle,
        })
    }
    fn editable(&self, actor: PlayerId, revision: u64) -> Result<(), PartyError> {
        if actor != self.leader {
            return Err(PartyError::LeaderRequired);
        }
        if revision != self.revision {
            return Err(PartyError::RevisionConflict);
        }
        if self.state != PartyState::Idle {
            return Err(PartyError::Frozen);
        }
        Ok(())
    }
    pub fn rename(&mut self, actor: PlayerId, revision: u64, name: &str) -> Result<(), PartyError> {
        self.editable(actor, revision)?;
        self.name = normalize_name(name)?;
        self.revision += 1;
        Ok(())
    }
    pub fn join(&mut self, player: PlayerId) -> Result<(), PartyError> {
        if self.state != PartyState::Idle {
            return Err(PartyError::Frozen);
        }
        if !self.members.contains(&player) {
            self.members.push(player);
            self.members.sort();
            self.revision += 1;
        }
        Ok(())
    }
    pub fn kick(
        &mut self,
        actor: PlayerId,
        revision: u64,
        player: PlayerId,
    ) -> Result<(), PartyError> {
        self.editable(actor, revision)?;
        if player == self.leader {
            return Err(PartyError::LeaderRequired);
        }
        self.members.retain(|id| *id != player);
        self.revision += 1;
        Ok(())
    }
    pub fn leave(&mut self, player: PlayerId) -> Result<bool, PartyError> {
        if self.state != PartyState::Idle {
            return Err(PartyError::Frozen);
        }
        self.members.retain(|id| *id != player);
        self.revision += 1;
        if player == self.leader {
            if let Some(next) = self.members.first().copied() {
                self.leader = next;
            }
        }
        Ok(self.members.is_empty())
    }
    pub fn validate_enqueue(&self, mode: QueueMode) -> Result<(), PartyError> {
        if !matches!(self.state, PartyState::Idle | PartyState::NotReady) {
            return Err(PartyError::Frozen);
        }
        let n = self.members.len();
        let valid = match mode {
            QueueMode::OneVsOne => n == 1,
            QueueMode::FiveVsFive => (1..=5).contains(&n),
            QueueMode::FreeForAll => (1..=4).contains(&n),
        };
        if valid {
            Ok(())
        } else {
            Err(PartyError::InvalidSize)
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemberEligibility {
    pub online: bool,
    pub credit: u8,
    pub active: bool,
    pub regions: BTreeSet<String>,
}
pub fn validate_party_enqueue(
    party: &Party,
    mode: QueueMode,
    members: &BTreeMap<PlayerId, MemberEligibility>,
    minimum_credit: u8,
) -> Result<BTreeSet<String>, PartyError> {
    party.validate_enqueue(mode)?;
    let states: Vec<_> = party.members.iter().map(|id| members.get(id)).collect();
    if states
        .iter()
        .any(|state| state.is_none_or(|v| !v.online || v.credit < minimum_credit || v.active))
    {
        return Err(PartyError::Frozen);
    }
    region_intersection(
        &states
            .into_iter()
            .map(|state| state.unwrap().regions.clone())
            .collect::<Vec<_>>(),
    )
}
#[derive(Clone, Debug)]
pub struct Invite {
    pub party: PartyId,
    pub expires_at: u64,
    pub remaining_uses: u32,
}
#[derive(Default)]
pub struct InviteStore {
    tokens: BTreeMap<String, Invite>,
}
impl InviteStore {
    pub fn create(&mut self, party: PartyId, now: u64, ttl: u64, uses: u32) -> String {
        let token = Uuid::new_v4().simple().to_string();
        self.tokens.insert(
            token.clone(),
            Invite {
                party,
                expires_at: now.saturating_add(ttl),
                remaining_uses: uses.max(1),
            },
        );
        token
    }
    pub fn consume(&mut self, token: &str, now: u64) -> Option<PartyId> {
        let invite = self.tokens.get_mut(token)?;
        if now > invite.expires_at || invite.remaining_uses == 0 {
            return None;
        }
        invite.remaining_uses -= 1;
        Some(invite.party)
    }
}
pub fn region_intersection(regions: &[BTreeSet<String>]) -> Result<BTreeSet<String>, PartyError> {
    let Some(first) = regions.first() else {
        return Err(PartyError::NoCommonRegion);
    };
    let result = regions
        .iter()
        .skip(1)
        .fold(first.clone(), |a, b| a.intersection(b).cloned().collect());
    if result.is_empty() {
        Err(PartyError::NoCommonRegion)
    } else {
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cjk_valid_special_invalid() {
        assert_eq!(normalize_name("台灣第一隊5").unwrap(), "台灣第一隊5");
        for n in ["My Team", "隊伍#1", "🔥勝利隊🔥", "abc_def"] {
            assert_eq!(normalize_name(n), Err(PartyError::InvalidName));
        }
    }
    #[test]
    fn revision_and_freeze_are_enforced() {
        let p = PlayerId::new();
        let mut party = Party::new(p, "隊伍1").unwrap();
        assert_eq!(
            party.rename(p, 1, "隊伍2"),
            Err(PartyError::RevisionConflict)
        );
        party.state = PartyState::Queued;
        assert_eq!(party.rename(p, 0, "隊伍2"), Err(PartyError::Frozen));
    }
    #[test]
    fn invite_expires_and_counts_uses() {
        let mut s = InviteStore::default();
        let party = PartyId::new();
        let t = s.create(party, 10, 5, 1);
        assert_eq!(s.consume(&t, 15), Some(party));
        assert_eq!(s.consume(&t, 15), None);
    }
    #[test]
    fn mode_sizes_are_exact() {
        let p = PlayerId::new();
        let mut party = Party::new(p, "A1").unwrap();
        assert!(party.validate_enqueue(QueueMode::OneVsOne).is_ok());
        party.members.push(PlayerId::new());
        assert_eq!(
            party.validate_enqueue(QueueMode::OneVsOne),
            Err(PartyError::InvalidSize)
        );
        assert!(party.validate_enqueue(QueueMode::FreeForAll).is_ok());
    }
    #[test]
    fn queued_or_matched_party_cannot_enqueue_again() {
        let mut party = Party::new(PlayerId::new(), "State1").unwrap();
        party.state = PartyState::Queued;
        assert_eq!(
            party.validate_enqueue(QueueMode::OneVsOne),
            Err(PartyError::Frozen)
        );
        party.state = PartyState::Matched;
        assert_eq!(
            party.validate_enqueue(QueueMode::OneVsOne),
            Err(PartyError::Frozen)
        );
        party.state = PartyState::NotReady;
        assert!(party.validate_enqueue(QueueMode::OneVsOne).is_ok());
    }
}
