//! Rating and credit profile provider boundary.

use crate::{components::QueueMode, id::PlayerId};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlayerProfile {
    pub ratings: BTreeMap<QueueMode, i32>,
    pub completed_matches: BTreeMap<QueueMode, u32>,
    pub credit: u8,
    pub recent_credit_violations: u32,
}
impl Default for PlayerProfile {
    fn default() -> Self {
        Self {
            ratings: BTreeMap::from([
                (QueueMode::OneVsOne, 1000),
                (QueueMode::FiveVsFive, 1000),
                (QueueMode::FreeForAll, 1000),
            ]),
            completed_matches: BTreeMap::new(),
            credit: 100,
            recent_credit_violations: 0,
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProfileError {
    #[error("profile unavailable: {0}")]
    Unavailable(String),
    #[error("profile conflict")]
    Conflict,
}

#[async_trait]
pub trait PlayerProfileProvider: Send + Sync {
    async fn load(&self, player: PlayerId) -> Result<PlayerProfile, ProfileError>;
    async fn save(
        &self,
        player: PlayerId,
        expected: Option<PlayerProfile>,
        updated: PlayerProfile,
    ) -> Result<(), ProfileError>;
}

#[derive(Clone, Default)]
pub struct MemoryProfileProvider {
    inner: Arc<RwLock<BTreeMap<PlayerId, PlayerProfile>>>,
}
#[async_trait]
impl PlayerProfileProvider for MemoryProfileProvider {
    async fn load(&self, player: PlayerId) -> Result<PlayerProfile, ProfileError> {
        Ok(self.inner.read().get(&player).cloned().unwrap_or_default())
    }
    async fn save(
        &self,
        player: PlayerId,
        expected: Option<PlayerProfile>,
        updated: PlayerProfile,
    ) -> Result<(), ProfileError> {
        let mut profiles = self.inner.write();
        if let Some(expected) = expected {
            if profiles.get(&player).cloned().unwrap_or_default() != expected {
                return Err(ProfileError::Conflict);
            }
        }
        profiles.insert(player, updated);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn memory_provider_is_deterministic_and_conflict_safe() {
        let p = MemoryProfileProvider::default();
        let id = PlayerId::new();
        let initial = p.load(id).await.unwrap();
        assert_eq!(initial.credit, 100);
        let mut changed = initial.clone();
        changed.credit = 98;
        p.save(id, Some(initial.clone()), changed.clone())
            .await
            .unwrap();
        assert_eq!(p.load(id).await.unwrap(), changed);
        assert_eq!(
            p.save(id, Some(initial), PlayerProfile::default()).await,
            Err(ProfileError::Conflict)
        );
    }
}
