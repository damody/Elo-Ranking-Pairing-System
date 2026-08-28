//! Specs resources for queues, clocks and deterministic allocation.

use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityCommand {
    pub logical_sequence: u64,
    pub request_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientEvent {
    pub session: String,
    pub kind: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlMessage {
    pub server: String,
    pub kind: String,
}

pub struct BoundedQueue<T> {
    pub sender: Sender<T>,
    pub receiver: Receiver<T>,
    pub capacity: usize,
}
impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self {
            sender,
            receiver,
            capacity,
        }
    }
}
pub type CommandQueue = BoundedQueue<AuthorityCommand>;
pub type EventQueue = BoundedQueue<ClientEvent>;
pub type ControlQueue = BoundedQueue<ControlMessage>;

#[derive(Clone, Copy, Debug, Default)]
pub struct LogicalClock(pub u64);
#[derive(Clone, Copy, Debug)]
pub struct DeterministicSeed(pub u64);
impl Default for DeterministicSeed {
    fn default() -> Self {
        Self(0x4552_5053)
    }
}
#[derive(Clone, Debug, Default)]
pub struct IdAllocator {
    next_by_kind: BTreeMap<&'static str, u64>,
}
impl IdAllocator {
    pub fn next(&mut self, kind: &'static str) -> u64 {
        let next = self.next_by_kind.entry(kind).or_insert(0);
        *next += 1;
        *next
    }
}
