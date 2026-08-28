//! Deterministic command envelopes and idempotent apply results.
use crate::id::{PlayerId, SessionId};
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope<T> {
    pub sequence: u64,
    pub request_id: String,
    pub session: SessionId,
    pub player: PlayerId,
    pub command: T,
}
#[derive(Default)]
pub struct RequestCache<R> {
    results: BTreeMap<(SessionId, String), R>,
}
impl<R: Clone> RequestCache<R> {
    pub fn get(&self, s: SessionId, r: &str) -> Option<R> {
        self.results.get(&(s, r.into())).cloned()
    }
    pub fn record(&mut self, s: SessionId, r: String, v: R) -> R {
        self.results.insert((s, r), v.clone());
        v
    }
}
pub fn sort_commands<T>(commands: &mut [CommandEnvelope<T>]) {
    commands.sort_by(|a, b| {
        (a.sequence, a.player, a.request_id.as_str()).cmp(&(
            b.sequence,
            b.player,
            b.request_id.as_str(),
        ))
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order_and_cache_are_stable() {
        let s = SessionId::new();
        let p = PlayerId::new();
        let mut q = vec![
            CommandEnvelope {
                sequence: 2,
                request_id: "b".into(),
                session: s,
                player: p,
                command: (),
            },
            CommandEnvelope {
                sequence: 1,
                request_id: "a".into(),
                session: s,
                player: p,
                command: (),
            },
        ];
        sort_commands(&mut q);
        assert_eq!(q[0].request_id, "a");
        let mut c = RequestCache::default();
        c.record(s, "x".into(), 7);
        assert_eq!(c.get(s, "x"), Some(7));
    }
}
