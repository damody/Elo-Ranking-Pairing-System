//! ERPS Specs world construction.

use crate::{components::*, config::ErpsConfig, resources::*};
use specs::{World, WorldExt};

pub fn build_world(config: &ErpsConfig) -> World {
    let mut world = World::new();
    world.register::<PlayerIdentity>();
    world.register::<EloRating>();
    world.register::<CreditScore>();
    world.register::<ConnectionId>();
    world.register::<AllowedRegions>();
    world.register::<PlayerState>();
    world.register::<PartyIdentity>();
    world.register::<PartyName>();
    world.register::<PartyMembers>();
    world.register::<PartyLeader>();
    world.register::<PartyRevision>();
    world.register::<PartyState>();
    world.register::<TicketIdentity>();
    world.register::<TicketMode>();
    world.register::<TicketRegions>();
    world.register::<EnqueuedAt>();
    world.register::<SearchRange>();
    world.register::<BucketOwner>();
    world.register::<TicketState>();
    world.register::<ProposalIdentity>();
    world.register::<ProposalRoster>();
    world.register::<AcceptDeadline>();
    world.register::<PlayerAcceptStates>();
    world.register::<MatchIdentity>();
    world.register::<MatchRoster>();
    world.register::<AssignedServer>();
    world.register::<ReservationCost>();
    world.register::<MatchState>();
    world.register::<ServerIdentity>();
    world.register::<Generation>();
    world.register::<ServerEndpoint>();
    world.register::<ServerRegion>();
    world.register::<SupportedModes>();
    world.register::<Capacity>();
    world.register::<ModeCosts>();
    world.register::<InstanceCapacity>();
    world.register::<LastHeartbeat>();
    world.register::<ServerHealth>();
    world.register::<RecentLaunchFailures>();
    world.insert(CommandQueue::new(config.command_queue_capacity));
    world.insert(EventQueue::new(config.event_queue_capacity));
    world.insert(ControlQueue::new(config.control_queue_capacity));
    world.insert(LogicalClock::default());
    world.insert(DeterministicSeed::default());
    world.insert(IdAllocator::default());
    world
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn world_has_bounded_resources() {
        let c = ErpsConfig::default();
        let w = build_world(&c);
        assert_eq!(
            w.read_resource::<CommandQueue>().capacity,
            c.command_queue_capacity
        );
        assert_eq!(w.read_resource::<LogicalClock>().0, 0);
    }
}
