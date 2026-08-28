use crate::id::{PlayerId, SessionId};
use std::collections::BTreeMap;
#[derive(Clone, Debug)]
pub struct Session {
    pub id: SessionId,
    pub player: PlayerId,
    pub disconnected_at: Option<u64>,
}
#[derive(Default)]
pub struct Sessions {
    items: BTreeMap<SessionId, Session>,
}
impl Sessions {
    pub fn connect(&mut self, player: PlayerId) -> SessionId {
        let id = SessionId::new();
        self.items.insert(
            id,
            Session {
                id,
                player,
                disconnected_at: None,
            },
        );
        id
    }
    pub fn disconnect(&mut self, id: SessionId, now: u64) {
        if let Some(s) = self.items.get_mut(&id) {
            s.disconnected_at = Some(now)
        }
    }
    pub fn reconnect(&mut self, id: SessionId, now: u64, grace: u64) -> bool {
        let Some(s) = self.items.get_mut(&id) else {
            return false;
        };
        if s.disconnected_at
            .is_some_and(|at| now.saturating_sub(at) <= grace)
        {
            s.disconnected_at = None;
            true
        } else {
            false
        }
    }
    pub fn expired_players(&self, now: u64, grace: u64) -> Vec<PlayerId> {
        self.items
            .values()
            .filter_map(|session| {
                session
                    .disconnected_at
                    .filter(|at| now.saturating_sub(*at) > grace)
                    .map(|_| session.player)
            })
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reconnect_only_inside_grace() {
        let mut s = Sessions::default();
        let id = s.connect(PlayerId::new());
        s.disconnect(id, 10);
        assert!(s.reconnect(id, 40, 30));
        s.disconnect(id, 50);
        assert!(!s.reconnect(id, 81, 30));
    }
}
