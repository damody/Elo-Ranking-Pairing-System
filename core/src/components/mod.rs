//! Specs components owned by the ERPS authority world.

use crate::id::*;
use serde::{Deserialize, Serialize};
use specs::{Component, DenseVecStorage};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueueMode {
    OneVsOne,
    FiveVsFive,
    FreeForAll,
}

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct PlayerIdentity(pub PlayerId);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct EloRating(pub BTreeMap<QueueMode, i32>);
#[derive(Clone, Copy, Debug, Component, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub struct CreditScore(pub u8);
impl Default for CreditScore {
    fn default() -> Self {
        Self(100)
    }
}
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ConnectionId(pub SessionId);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct AllowedRegions(pub BTreeSet<String>);
#[derive(Clone, Copy, Debug, Component, Default, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub enum PlayerState {
    #[default]
    Idle,
    Queued,
    Proposed,
    Matched,
    Suspended,
    Disconnected,
}

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct PartyIdentity(pub PartyId);
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct PartyName(pub String);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct PartyMembers(pub Vec<PlayerId>);
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct PartyLeader(pub PlayerId);
#[derive(Clone, Copy, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct PartyRevision(pub u64);
#[derive(Clone, Copy, Debug, Component, Default, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub enum PartyState {
    #[default]
    Idle,
    Queued,
    AwaitingAccept,
    NotReady,
    Matched,
}

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct TicketIdentity(pub TicketId);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct TicketMode(pub QueueMode);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct TicketRegions(pub Vec<String>);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct EnqueuedAt(pub u64);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct SearchRange {
    pub minimum: i32,
    pub maximum: i32,
}
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct BucketOwner {
    pub region: String,
    pub bucket: i32,
}
#[derive(Clone, Copy, Debug, Component, Default, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub enum TicketState {
    #[default]
    Queued,
    Claimed,
    Proposed,
    Cancelled,
}

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ProposalIdentity(pub ProposalId);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct ProposalRoster(pub Vec<Vec<PlayerId>>);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct AcceptDeadline(pub u64);
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum AcceptState {
    Pending,
    Accepted,
    Rejected,
    TimedOut,
}
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct PlayerAcceptStates(pub BTreeMap<PlayerId, AcceptState>);

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct MatchIdentity(pub MatchId);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct MatchRoster(pub Vec<Vec<PlayerId>>);
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct AssignedServer(pub ServerId);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ReservationCost(pub u32);
#[derive(Clone, Copy, Debug, Component, Default, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub enum MatchState {
    #[default]
    AwaitingAccept,
    AwaitingPlacement,
    Reserved,
    Accepted,
    Ready,
    Running,
    Finished,
    ServerLost,
    Cancelled,
}

#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ServerIdentity(pub ServerId);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct Generation(pub ServerGeneration);
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ServerEndpoint(pub String);
#[derive(Clone, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct ServerRegion(pub String);
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct SupportedModes(pub BTreeSet<QueueMode>);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct Capacity {
    pub total: u32,
    pub used: u32,
}
#[derive(Clone, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct ModeCosts(pub BTreeMap<QueueMode, u32>);
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct InstanceCapacity {
    pub maximum: u16,
    pub running: u16,
    pub reserved: u16,
}
#[derive(Clone, Copy, Debug, Component)]
#[storage(DenseVecStorage)]
pub struct LastHeartbeat(pub u64);
#[derive(Clone, Copy, Debug, Component, Default, PartialEq, Eq)]
#[storage(DenseVecStorage)]
pub enum ServerHealth {
    #[default]
    Healthy,
    Unhealthy,
    Lost,
}
#[derive(Clone, Copy, Debug, Component, Default)]
#[storage(DenseVecStorage)]
pub struct RecentLaunchFailures(pub u32);
