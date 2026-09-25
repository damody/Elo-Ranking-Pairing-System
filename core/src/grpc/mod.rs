//! Tonic services backed by one bounded, authoritative command actor.

use crate::{
    components::{PartyState, QueueMode},
    config::ErpsConfig,
    id::{MatchId, PartyId, PlayerId, ProposalId, ServerGeneration, ServerId, TicketId},
    matching::{
        claim::Claims, dispatcher, five_v_five, free_for_all, one_v_one,
        snapshot::CandidateSnapshot, PartyTicket,
    },
    party::{InviteStore, Party},
    profile::{MemoryProfileProvider, PlayerProfile, PlayerProfileProvider},
    proposal::{cancellation_decisions, Proposal, ProposalState},
    server::{GameServer, Health, Instance, InstanceState, Registry, ServerLimits},
};
use erps_proto::v1::{
    self as pb, admin_service_server::AdminService, game_server_service_server::GameServerService,
    matchmaking_service_server::MatchmakingService,
};
use specs::{Builder, Join, World, WorldExt};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{
    wrappers::{BroadcastStream, ReceiverStream},
    Stream,
};
use tonic::{Request, Response, Status};

type Job = Box<dyn FnOnce(&mut AuthorityState) + Send>;
struct JobEnvelope {
    sequence: u64,
    apply: Job,
}
type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

struct DisconnectAwareEvents {
    inner: BroadcastStream<pb::ClientEvent>,
    core: CoreHandle,
    player: PlayerId,
    session_token: String,
}
impl Stream for DisconnectAwareEvents {
    type Item = Result<pb::ClientEvent, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(Status::resource_exhausted(
                "event consumer fell behind; call GetState before reconnecting",
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for DisconnectAwareEvents {
    fn drop(&mut self) {
        self.core
            .disconnected(self.player, self.session_token.clone());
    }
}

#[derive(Clone)]
pub struct CoreHandle {
    tx: mpsc::Sender<JobEnvelope>,
    metrics: Arc<crate::metrics::Metrics>,
    sequence: Arc<AtomicU64>,
    profiles: Arc<dyn PlayerProfileProvider>,
}
impl CoreHandle {
    pub fn spawn(config: ErpsConfig) -> Self {
        Self::spawn_with_profile_provider(config, Arc::new(MemoryProfileProvider::default()))
    }
    pub fn spawn_with_profile_provider(
        config: ErpsConfig,
        profiles: Arc<dyn PlayerProfileProvider>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<JobEnvelope>(config.command_queue_capacity);
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let actor_metrics = metrics.clone();
        let sequence = Arc::new(AtomicU64::new(0));
        let actor_profiles = profiles.clone();
        let batch_window = std::time::Duration::from_millis(config.batch_window_ms);
        tokio::spawn(async move {
            let mut state = AuthorityState::new(config, actor_metrics);
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    job = rx.recv() => match job {
                        Some(job) => {
                            tokio::time::sleep(batch_window).await;
                            let mut batch = vec![job];
                            while let Ok(job) = rx.try_recv() {
                                batch.push(job);
                            }
                            batch.sort_by_key(|job| job.sequence);
                            for job in batch {
                                state.logical_clock = state.logical_clock.saturating_add(1);
                                (job.apply)(&mut state);
                            }
                            state.rebuild_ecs();
                            state.attempt_all_matches();
                            state.rebuild_ecs();
                            for (player, profile) in std::mem::take(&mut state.pending_profile_saves) {
                                if actor_profiles.save(player, None, profile).await.is_err() {
                                    state.metrics.invariant_failures.add(1);
                                }
                            }
                        },
                        None => break,
                    },
                    _ = interval.tick() => {
                        state.tick(now_ms());
                        for (player, profile) in std::mem::take(&mut state.pending_profile_saves) {
                            if actor_profiles.save(player, None, profile).await.is_err() {
                                state.metrics.invariant_failures.add(1);
                            }
                        }
                    },
                }
            }
        });
        Self {
            tx,
            metrics,
            sequence,
            profiles,
        }
    }
    pub fn metrics(&self) -> Arc<crate::metrics::Metrics> {
        self.metrics.clone()
    }
    async fn begin_drain(&self) {
        let _ = self
            .call(|state| {
                state.config.drain_mode = true;
                Ok(())
            })
            .await;
    }
    fn disconnected(&self, player: PlayerId, session_token: String) {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.try_send(JobEnvelope {
            sequence,
            apply: Box::new(move |state| {
                if state.current_session.get(&player) == Some(&session_token) {
                    state.offline.insert(player);
                    state.disconnect_deadlines.insert(
                        player,
                        now_ms().saturating_add(
                            state.config.disconnect_grace_seconds.saturating_mul(1000),
                        ),
                    );
                }
            }),
        });
    }
    async fn call<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut AuthorityState) -> Result<R, Status> + Send + 'static,
    ) -> Result<R, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.metrics.commands.add(1);
        self.metrics
            .command_queue_high
            .observe(self.tx.max_capacity().saturating_sub(self.tx.capacity()) as u64);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.tx
            .try_send(JobEnvelope {
                sequence,
                apply: Box::new(move |state| {
                    let _ = reply_tx.send(f(state));
                }),
            })
            .map_err(|_| Status::resource_exhausted("ERPS command queue is full"))?;
        reply_rx
            .await
            .map_err(|_| Status::unavailable("ERPS authority stopped"))?
    }
}

#[derive(Clone)]
struct TicketRecord {
    id: TicketId,
    mode: QueueMode,
    regions: Vec<String>,
    enqueued_at: u64,
    queued_since_ms: u64,
}
struct AuthorityState {
    config: ErpsConfig,
    next_domain_id: u64,
    logical_clock: u64,
    world: World,
    metrics: Arc<crate::metrics::Metrics>,
    sessions: BTreeMap<String, PlayerId>,
    current_session: BTreeMap<PlayerId, String>,
    disconnect_deadlines: BTreeMap<PlayerId, u64>,
    offline: BTreeSet<PlayerId>,
    parties: BTreeMap<PartyId, Party>,
    player_party: BTreeMap<PlayerId, PartyId>,
    invites: InviteStore,
    tickets: BTreeMap<PartyId, TicketRecord>,
    request_cache: BTreeMap<(PlayerId, String), pb::OperationResult>,
    events: BTreeMap<PlayerId, broadcast::Sender<pb::ClientEvent>>,
    registry: Registry,
    proposals: BTreeMap<ProposalId, Proposal>,
    player_proposal: BTreeMap<PlayerId, ProposalId>,
    controls: BTreeMap<ServerId, mpsc::Sender<Result<pb::ErpsControl, Status>>>,
    launches: BTreeMap<MatchId, (ServerId, ProposalId)>,
    launch_deadlines: BTreeMap<MatchId, u64>,
    placement_waiting: BTreeMap<ProposalId, u64>,
    proposal_tickets: BTreeMap<ProposalId, Vec<(PartyId, TicketRecord)>>,
    credit: BTreeMap<PlayerId, u8>,
    recent_credit_violations: BTreeMap<PlayerId, u32>,
    credit_suspended_until: BTreeMap<PlayerId, u64>,
    completed_since_credit_recovery: BTreeMap<PlayerId, u32>,
    ratings: BTreeMap<(PlayerId, QueueMode), i32>,
    completed_games: BTreeMap<(PlayerId, QueueMode), u32>,
    pending_profile_saves: Vec<(PlayerId, PlayerProfile)>,
    player_match: BTreeMap<PlayerId, MatchId>,
    completed_match_results: BTreeSet<MatchId>,
    completed_match_result_order: VecDeque<MatchId>,
    claims: Claims,
}
impl AuthorityState {
    fn new(config: ErpsConfig, metrics: Arc<crate::metrics::Metrics>) -> Self {
        let world = crate::world::build_world(&config);
        Self {
            config,
            next_domain_id: 0,
            logical_clock: 0,
            world,
            metrics,
            sessions: BTreeMap::new(),
            current_session: BTreeMap::new(),
            disconnect_deadlines: BTreeMap::new(),
            offline: BTreeSet::new(),
            parties: BTreeMap::new(),
            player_party: BTreeMap::new(),
            invites: InviteStore::default(),
            tickets: BTreeMap::new(),
            request_cache: BTreeMap::new(),
            events: BTreeMap::new(),
            registry: Registry::default(),
            proposals: BTreeMap::new(),
            player_proposal: BTreeMap::new(),
            controls: BTreeMap::new(),
            launches: BTreeMap::new(),
            launch_deadlines: BTreeMap::new(),
            placement_waiting: BTreeMap::new(),
            proposal_tickets: BTreeMap::new(),
            credit: BTreeMap::new(),
            recent_credit_violations: BTreeMap::new(),
            credit_suspended_until: BTreeMap::new(),
            completed_since_credit_recovery: BTreeMap::new(),
            ratings: BTreeMap::new(),
            completed_games: BTreeMap::new(),
            pending_profile_saves: Vec::new(),
            player_match: BTreeMap::new(),
            completed_match_results: BTreeSet::new(),
            completed_match_result_order: VecDeque::new(),
            claims: Claims::default(),
        }
    }
    fn next_uuid(&mut self, kind: u8) -> uuid::Uuid {
        self.next_domain_id = self.next_domain_id.saturating_add(1);
        uuid::Uuid::from_u128(
            (u128::from(self.config.deterministic_seed) << 64)
                | (u128::from(kind) << 56)
                | u128::from(self.next_domain_id),
        )
    }

    fn remember_completed_match_result(&mut self, match_id: MatchId) {
        if self.completed_match_results.insert(match_id) {
            self.completed_match_result_order.push_back(match_id);
        }
        if self.completed_match_result_order.len() > 4096 {
            if let Some(expired) = self.completed_match_result_order.pop_front() {
                self.completed_match_results.remove(&expired);
            }
        }
    }
    fn rebuild_ecs(&mut self) {
        let entities: Vec<_> = self.world.entities().join().collect();
        for entity in entities {
            let _ = self.world.delete_entity(entity);
        }
        self.world.maintain();
        let mut players: Vec<_> = self.sessions.values().copied().collect();
        players.sort();
        players.dedup();
        for player in players {
            let state = if self.player_match.contains_key(&player) {
                crate::components::PlayerState::Matched
            } else if self.player_proposal.contains_key(&player) {
                crate::components::PlayerState::Proposed
            } else if self
                .player_party
                .get(&player)
                .is_some_and(|party| self.tickets.contains_key(party))
            {
                crate::components::PlayerState::Queued
            } else {
                crate::components::PlayerState::Idle
            };
            self.world
                .create_entity()
                .with(crate::components::PlayerIdentity(player))
                .with(crate::components::CreditScore(
                    self.credit.get(&player).copied().unwrap_or(100),
                ))
                .with(crate::components::EloRating(BTreeMap::from([
                    (
                        QueueMode::OneVsOne,
                        *self
                            .ratings
                            .get(&(player, QueueMode::OneVsOne))
                            .unwrap_or(&1000),
                    ),
                    (
                        QueueMode::FiveVsFive,
                        *self
                            .ratings
                            .get(&(player, QueueMode::FiveVsFive))
                            .unwrap_or(&1000),
                    ),
                    (
                        QueueMode::FreeForAll,
                        *self
                            .ratings
                            .get(&(player, QueueMode::FreeForAll))
                            .unwrap_or(&1000),
                    ),
                ])))
                .with(state)
                .build();
        }
        for party in self.parties.values() {
            self.world
                .create_entity()
                .with(crate::components::PartyIdentity(party.id))
                .with(crate::components::PartyName(party.name.clone()))
                .with(crate::components::PartyMembers(party.members.clone()))
                .with(crate::components::PartyLeader(party.leader))
                .with(crate::components::PartyRevision(party.revision))
                .with(party.state)
                .build();
        }
        for (party_id, ticket) in &self.tickets {
            let rating = self
                .parties
                .get(party_id)
                .and_then(|party| {
                    let ratings = party
                        .members
                        .iter()
                        .map(|player| *self.ratings.get(&(*player, ticket.mode)).unwrap_or(&1000))
                        .collect::<Vec<_>>();
                    crate::matching::effective_rating_for_mode(
                        ticket.mode,
                        &ratings,
                        self.config.party_size_rating_adjustment,
                        self.config.party_spread_rating_adjustment,
                        self.config.max_party_rating_spread,
                    )
                })
                .unwrap_or(1000);
            let search_delta = crate::matching::bucket::search_delta(
                self.config.initial_elo_delta,
                self.config.elo_step,
                self.config.elo_step_seconds,
                self.config.maximum_elo_delta,
                0,
                now_ms().saturating_sub(ticket.queued_since_ms) / 1000,
            );
            self.world
                .create_entity()
                .with(crate::components::TicketIdentity(ticket.id))
                .with(crate::components::TicketMode(ticket.mode))
                .with(crate::components::TicketRegions(ticket.regions.clone()))
                .with(crate::components::EnqueuedAt(ticket.enqueued_at))
                .with(crate::components::SearchRange {
                    minimum: rating.saturating_sub(search_delta),
                    maximum: rating.saturating_add(search_delta),
                })
                .with(crate::components::BucketOwner {
                    region: ticket.regions.first().cloned().unwrap_or_default(),
                    bucket: rating.div_euclid(100),
                })
                .with(crate::components::TicketState::Queued)
                .with(crate::components::PartyIdentity(*party_id))
                .build();
        }
        for proposal in self.proposals.values() {
            self.world
                .create_entity()
                .with(crate::components::ProposalIdentity(proposal.id))
                .with(crate::components::ProposalRoster(proposal.teams.clone()))
                .with(crate::components::AcceptDeadline(proposal.deadline))
                .with(crate::components::PlayerAcceptStates(
                    proposal.responses.clone(),
                ))
                .build();
        }
        for server in self.registry.servers.values() {
            self.world
                .create_entity()
                .with(crate::components::ServerIdentity(server.id))
                .with(crate::components::Generation(server.generation))
                .with(crate::components::ServerEndpoint(server.endpoint.clone()))
                .with(crate::components::ServerRegion(server.region.clone()))
                .with(crate::components::SupportedModes(server.modes.clone()))
                .with(crate::components::Capacity {
                    total: server.capacity_total,
                    used: server.capacity_used,
                })
                .with(crate::components::ModeCosts(server.mode_costs.clone()))
                .with(crate::components::InstanceCapacity {
                    maximum: server.max_instances,
                    running: server
                        .instances
                        .values()
                        .filter(|i| i.state == InstanceState::Running)
                        .count() as u16,
                    reserved: server
                        .instances
                        .values()
                        .filter(|i| i.state == InstanceState::Reserved)
                        .count() as u16,
                })
                .with(crate::components::LastHeartbeat(server.last_heartbeat))
                .with(match server.health {
                    Health::Healthy => crate::components::ServerHealth::Healthy,
                    Health::Unhealthy => crate::components::ServerHealth::Unhealthy,
                    Health::Lost => crate::components::ServerHealth::Lost,
                })
                .with(crate::components::RecentLaunchFailures(server.failures))
                .build();
        }
        self.world.maintain();
        let mut snapshot = crate::metrics::MetricSnapshot {
            party_structure_difference_sum: self
                .metrics
                .lifecycle
                .read()
                .party_structure_difference_sum,
            elo_quality_sum: self.metrics.lifecycle.read().elo_quality_sum,
            ..Default::default()
        };
        for (party_id, ticket) in &self.tickets {
            let players = self
                .parties
                .get(party_id)
                .map_or(0, |party| party.members.len() as u64);
            for region in &ticket.regions {
                *snapshot
                    .queue_players
                    .entry(crate::metrics::QueueLabel {
                        mode: ticket.mode,
                        region: region.clone(),
                    })
                    .or_default() += players;
            }
        }
        snapshot.capacity_total = self
            .registry
            .servers
            .values()
            .map(|server| server.capacity_total as u64)
            .sum();
        snapshot.capacity_used = self
            .registry
            .servers
            .values()
            .map(|server| server.capacity_used as u64)
            .sum();
        snapshot.reservations = self
            .registry
            .servers
            .values()
            .flat_map(|server| server.instances.values())
            .filter(|instance| instance.state == InstanceState::Reserved)
            .count() as u64;
        snapshot.running_instances = self
            .registry
            .servers
            .values()
            .flat_map(|server| server.instances.values())
            .filter(|instance| instance.state == InstanceState::Running)
            .count() as u64;
        *self.metrics.lifecycle.write() = snapshot;
    }
    fn tick(&mut self, now: u64) {
        let disconnected: Vec<_> = self
            .disconnect_deadlines
            .iter()
            .filter_map(|(player, deadline)| (now > *deadline).then_some(*player))
            .collect();
        for player in disconnected {
            self.disconnect_deadlines.remove(&player);
            if let Some(party_id) = self.player_party.get(&player).copied() {
                if let Some(proposal_id) = self.player_proposal.get(&player).copied() {
                    if let Some(proposal) = self.proposals.get_mut(&proposal_id) {
                        if let Some(response) = proposal.responses.get_mut(&player) {
                            *response = crate::components::AcceptState::TimedOut;
                            proposal.state = ProposalState::Cancelled;
                        }
                    }
                    self.cancel_proposal(proposal_id, false);
                } else if self.tickets.remove(&party_id).is_some() {
                    if let Some(party) = self.parties.get_mut(&party_id) {
                        party.state = PartyState::Idle;
                        party.revision = party.revision.saturating_add(1);
                    }
                }
            }
        }
        let expired: Vec<_> = self
            .proposals
            .iter_mut()
            .filter_map(|(id, proposal)| proposal.expire(now).then_some(*id))
            .collect();
        for id in expired {
            self.cancel_proposal(id, false);
        }
        let waiting: Vec<_> = self.placement_waiting.keys().copied().collect();
        for proposal_id in waiting {
            if self.launch(proposal_id).is_ok() {
                self.placement_waiting.remove(&proposal_id);
            } else if self
                .placement_waiting
                .get(&proposal_id)
                .is_some_and(|deadline| now > *deadline)
            {
                self.placement_waiting.remove(&proposal_id);
                self.cancel_proposal(proposal_id, true);
            }
        }
        let unhealthy_after = self
            .config
            .heartbeat_interval_seconds
            .saturating_mul(self.config.unhealthy_after_missed as u64)
            .saturating_mul(1000);
        let lost = self.registry.update_health(
            now,
            unhealthy_after,
            self.config.lost_after_seconds.saturating_mul(1000),
        );
        for (match_id, was_running) in lost {
            if let Some((_, proposal_id)) = self.launches.get(&match_id).copied() {
                if was_running {
                    if let Some(proposal) = self.proposals.get(&proposal_id) {
                        for player in proposal.responses.keys() {
                            if let Some(tx) = self.events.get(player) {
                                self.metrics.event_queue_high.observe(tx.len() as u64);
                                let _ = tx.send(pb::ClientEvent {
                                    event: Some(pb::client_event::Event::ServerLostMatchId(
                                        match_id.to_string(),
                                    )),
                                    deadline_ms: 0,
                                });
                            }
                        }
                    }
                } else if let Some((server, _)) = self.launches.get(&match_id).copied() {
                    if self.retry_launch(match_id, server, proposal_id).is_err() {
                        self.cancel_proposal(proposal_id, true);
                        self.launches.remove(&match_id);
                        self.launch_deadlines.remove(&match_id);
                    }
                }
            }
        }
        let timed_out: Vec<_> = self
            .launch_deadlines
            .iter()
            .filter_map(|(match_id, deadline)| (now > *deadline).then_some(*match_id))
            .collect();
        for match_id in timed_out {
            if let Some((server, proposal)) = self.launches.get(&match_id).copied() {
                if self.retry_launch(match_id, server, proposal).is_err() {
                    self.cancel_proposal(proposal, true);
                    self.launches.remove(&match_id);
                }
            }
        }
        self.rebuild_ecs();
        self.attempt_all_matches();
    }
    fn cancel_proposal(&mut self, proposal_id: ProposalId, infrastructure: bool) {
        let Some(proposal) = self.proposals.get(&proposal_id).cloned() else {
            return;
        };
        let decisions = cancellation_decisions(&proposal, infrastructure);
        let affected_players: Vec<_> = decisions.iter().map(|decision| decision.player).collect();
        let saved = self
            .proposal_tickets
            .remove(&proposal_id)
            .unwrap_or_default();
        self.claims.release(&proposal.tickets);
        self.placement_waiting.remove(&proposal_id);
        for decision in decisions {
            self.player_proposal.remove(&decision.player);
            if !infrastructure {
                if let Some(
                    cause @ (crate::credit::CreditCause::Rejected
                    | crate::credit::CreditCause::TimedOut),
                ) = decision.credit_cause
                {
                    let violations = self
                        .recent_credit_violations
                        .entry(decision.player)
                        .or_default();
                    let outcome = crate::credit::apply(
                        self.credit.get(&decision.player).copied().unwrap_or(100),
                        *violations,
                        0,
                        cause,
                        crate::credit::CreditPolicy {
                            reject_penalty: self.config.reject_credit_penalty,
                            timeout_penalty: self.config.timeout_credit_penalty,
                            minimum: self.config.minimum_credit,
                            ..Default::default()
                        },
                    );
                    *violations = outcome.suspension_steps;
                    self.credit.insert(decision.player, outcome.score);
                    self.completed_since_credit_recovery
                        .insert(decision.player, 0);
                    if !outcome.eligible {
                        self.credit_suspended_until.insert(
                            decision.player,
                            now_ms().saturating_add(
                                self.config
                                    .credit_suspension_base_seconds
                                    .saturating_mul(u64::from(outcome.suspension_steps))
                                    .saturating_mul(1000),
                            ),
                        );
                    }
                    match cause {
                        crate::credit::CreditCause::Rejected => {
                            self.metrics.ready_rejects.add(1);
                            self.metrics.credit_penalties.add(1);
                        }
                        crate::credit::CreditCause::TimedOut => {
                            self.metrics.ready_timeouts.add(1);
                            self.metrics.credit_penalties.add(1);
                        }
                        _ => {}
                    }
                }
            }
            let reason = match decision.credit_cause {
                Some(crate::credit::CreditCause::Rejected) => "rejected",
                Some(crate::credit::CreditCause::TimedOut) => "timed_out",
                Some(crate::credit::CreditCause::InfrastructureFailure) => "infrastructure_failure",
                Some(crate::credit::CreditCause::CompletedMatch) => "completed",
                None if decision.party_not_ready => "party_member_failed",
                None => "other_player_failed",
            };
            let suspended_until = self
                .credit_suspended_until
                .get(&decision.player)
                .copied()
                .unwrap_or(0);
            let credit = self.credit.get(&decision.player).copied().unwrap_or(100);
            if let Some(sender) = self.events.get(&decision.player) {
                let _ = sender.send(pb::ClientEvent {
                    event: Some(pb::client_event::Event::ProposalCancelled(
                        pb::ProposalCancelledEvent {
                            proposal_id: proposal_id.to_string(),
                            reason: reason.into(),
                            credit: u32::from(credit),
                            eligible: !credit_suspension_active(
                                credit,
                                self.config.minimum_credit,
                                Some(suspended_until),
                                now_ms(),
                            ),
                            credit_suspended_until_ms: suspended_until as i64,
                        },
                    )),
                    deadline_ms: 0,
                });
            }
        }
        for (party_id, ticket) in saved {
            let party_failed = self.parties.get(&party_id).is_some_and(|party| {
                party.members.iter().any(|player| {
                    proposal.responses.get(player).is_some_and(|state| {
                        matches!(
                            state,
                            crate::components::AcceptState::Rejected
                                | crate::components::AcceptState::TimedOut
                        )
                    })
                })
            });
            if let Some(party) = self.parties.get_mut(&party_id) {
                if party_failed && !infrastructure {
                    party.state = PartyState::NotReady;
                } else {
                    party.state = PartyState::Queued;
                    self.tickets.insert(party_id, ticket);
                }
                party.revision = party.revision.saturating_add(1);
            }
        }
        let changed_parties: BTreeSet<_> = affected_players
            .iter()
            .filter_map(|player| self.player_party.get(player).copied())
            .collect();
        for party_id in changed_parties {
            if let Some(party) = self.parties.get(&party_id) {
                self.emit_party(party);
            }
        }
        for player in affected_players {
            self.pending_profile_saves
                .push((player, self.current_profile(player)));
        }
    }
    fn player(&self, token: &str) -> Result<PlayerId, Status> {
        self.sessions
            .get(token)
            .copied()
            .ok_or_else(|| Status::unauthenticated("invalid session token"))
    }
    fn meta(&self, meta: Option<&pb::MutationMeta>) -> Result<(PlayerId, String), Status> {
        if self.config.drain_mode {
            return Err(Status::unavailable("ERPS is draining"));
        }
        let meta = meta.ok_or_else(|| Status::invalid_argument("mutation meta is required"))?;
        check_api(meta.api.as_ref())?;
        if meta.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        Ok((self.player(&meta.session_token)?, meta.request_id.clone()))
    }
    fn party_for(&self, id: &str) -> Result<PartyId, Status> {
        PartyId::from_str(id).map_err(|_| Status::invalid_argument("invalid party_id"))
    }
    fn emit_party(&self, party: &Party) {
        let event = pb::ClientEvent {
            event: Some(pb::client_event::Event::Party(self.party_view(party))),
            deadline_ms: 0,
        };
        for id in &party.members {
            if let Some(tx) = self.events.get(id) {
                let _ = tx.send(event.clone());
            }
        }
    }
    fn party_view(&self, party: &Party) -> pb::PartyView {
        pb::PartyView {
            party_id: party.id.to_string(),
            name: party.name.clone(),
            leader_id: party.leader.to_string(),
            members: party
                .members
                .iter()
                .map(|id| self.player_view(*id))
                .collect(),
            revision: party.revision,
            state: format!("{:?}", party.state),
        }
    }
    fn player_view(&self, player: PlayerId) -> pb::PlayerView {
        let rating = |mode| *self.ratings.get(&(player, mode)).unwrap_or(&1000);
        pb::PlayerView {
            player_id: player.to_string(),
            rating: rating(QueueMode::OneVsOne),
            credit: u32::from(self.credit.get(&player).copied().unwrap_or(100)),
            rating_one_v_one: rating(QueueMode::OneVsOne),
            rating_five_v_five: rating(QueueMode::FiveVsFive),
            rating_free_for_all: rating(QueueMode::FreeForAll),
        }
    }
    fn client_state(&self, player: PlayerId) -> pb::ClientState {
        let party = self
            .player_party
            .get(&player)
            .and_then(|id| self.parties.get(id));
        let ticket = party.and_then(|p| self.tickets.get(&p.id));
        let proposal_id = self.player_proposal.get(&player);
        let proposal = proposal_id.and_then(|proposal_id| self.proposals.get(proposal_id));
        let proposal_regions = proposal_id
            .and_then(|proposal_id| self.proposal_tickets.get(proposal_id))
            .and_then(|tickets| {
                tickets
                    .iter()
                    .map(|(_, ticket)| ticket.regions.iter().cloned().collect::<BTreeSet<_>>())
                    .reduce(|left, right| left.intersection(&right).cloned().collect())
            })
            .map(|regions| regions.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let credit = self.credit.get(&player).copied().unwrap_or(100);
        let suspended_until = self
            .credit_suspended_until
            .get(&player)
            .copied()
            .filter(|deadline| {
                credit_suspension_active(
                    credit,
                    self.config.minimum_credit,
                    Some(*deadline),
                    now_ms(),
                )
            })
            .unwrap_or(0);
        pb::ClientState {
            player_id: player.to_string(),
            party: party.map(|party| self.party_view(party)),
            ticket_id: ticket.map(|t| t.id.to_string()).unwrap_or_default(),
            proposal_id: self
                .player_proposal
                .get(&player)
                .map(ToString::to_string)
                .unwrap_or_default(),
            match_id: self
                .player_match
                .get(&player)
                .map(ToString::to_string)
                .unwrap_or_default(),
            profile: Some(self.player_view(player)),
            credit_suspended_until_ms: suspended_until as i64,
            queue_mode: ticket
                .map(|ticket| pb_mode(ticket.mode))
                .or_else(|| proposal.map(|proposal| pb_mode(proposal_mode(proposal))))
                .unwrap_or(pb::QueueMode::Unspecified as i32),
            allowed_regions: ticket
                .map(|ticket| ticket.regions.clone())
                .unwrap_or(proposal_regions),
            proposal_deadline_ms: proposal
                .map(|proposal| proposal.deadline as i64)
                .unwrap_or(0),
        }
    }
    fn cached(&self, p: PlayerId, r: &str) -> Option<pb::OperationResult> {
        self.request_cache.get(&(p, r.to_owned())).cloned()
    }
    fn remember(&mut self, p: PlayerId, r: String, v: pb::OperationResult) -> pb::OperationResult {
        self.request_cache.insert((p, r), v.clone());
        v
    }
    fn current_profile(&self, player: PlayerId) -> PlayerProfile {
        PlayerProfile {
            ratings: BTreeMap::from([
                (
                    QueueMode::OneVsOne,
                    *self
                        .ratings
                        .get(&(player, QueueMode::OneVsOne))
                        .unwrap_or(&1000),
                ),
                (
                    QueueMode::FiveVsFive,
                    *self
                        .ratings
                        .get(&(player, QueueMode::FiveVsFive))
                        .unwrap_or(&1000),
                ),
                (
                    QueueMode::FreeForAll,
                    *self
                        .ratings
                        .get(&(player, QueueMode::FreeForAll))
                        .unwrap_or(&1000),
                ),
            ]),
            completed_matches: BTreeMap::from([
                (
                    QueueMode::OneVsOne,
                    *self
                        .completed_games
                        .get(&(player, QueueMode::OneVsOne))
                        .unwrap_or(&0),
                ),
                (
                    QueueMode::FiveVsFive,
                    *self
                        .completed_games
                        .get(&(player, QueueMode::FiveVsFive))
                        .unwrap_or(&0),
                ),
                (
                    QueueMode::FreeForAll,
                    *self
                        .completed_games
                        .get(&(player, QueueMode::FreeForAll))
                        .unwrap_or(&0),
                ),
            ]),
            credit: self.credit.get(&player).copied().unwrap_or(100),
            recent_credit_violations: self
                .recent_credit_violations
                .get(&player)
                .copied()
                .unwrap_or(0),
        }
    }
    fn ecs_tickets(&self, mode: QueueMode, region: &str) -> Vec<PartyTicket> {
        let entities = self.world.entities();
        let party_ids = self
            .world
            .read_storage::<crate::components::PartyIdentity>();
        let party_members = self.world.read_storage::<crate::components::PartyMembers>();
        let party_revisions = self
            .world
            .read_storage::<crate::components::PartyRevision>();
        let ticket_ids = self
            .world
            .read_storage::<crate::components::TicketIdentity>();
        let ticket_modes = self.world.read_storage::<crate::components::TicketMode>();
        let ticket_regions = self
            .world
            .read_storage::<crate::components::TicketRegions>();
        let enqueued = self.world.read_storage::<crate::components::EnqueuedAt>();
        let player_ids = self
            .world
            .read_storage::<crate::components::PlayerIdentity>();
        let elo = self.world.read_storage::<crate::components::EloRating>();
        let player_ratings: BTreeMap<_, _> = (&player_ids, &elo)
            .join()
            .map(|(player, ratings)| (player.0, ratings.0.clone()))
            .collect();
        let parties: BTreeMap<_, _> = (&entities, &party_ids, &party_members, &party_revisions)
            .join()
            .filter(|(entity, _, _, _)| ticket_ids.get(*entity).is_none())
            .map(|(_, id, members, revision)| (id.0, (members.0.clone(), revision.0)))
            .collect();
        (
            &party_ids,
            &ticket_ids,
            &ticket_modes,
            &ticket_regions,
            &enqueued,
        )
            .join()
            .filter_map(|(party, ticket, ticket_mode, regions, enqueued)| {
                let (members, revision) = parties.get(&party.0)?;
                (ticket_mode.0 == mode && regions.0.iter().any(|candidate| candidate == region))
                    .then(|| {
                        let ratings: Vec<_> = members
                            .iter()
                            .map(|player| {
                                player_ratings
                                    .get(player)
                                    .and_then(|values| values.get(&mode))
                                    .copied()
                                    .unwrap_or(1000)
                            })
                            .collect();
                        let wait_seconds = now_ms().saturating_sub(
                            self.tickets
                                .get(&party.0)
                                .map_or(0, |ticket| ticket.queued_since_ms),
                        ) / 1000;
                        Some(PartyTicket {
                            id: ticket.0,
                            party: party.0,
                            members: members.clone(),
                            effective_rating: crate::matching::effective_rating_for_mode(
                                mode,
                                &ratings,
                                self.config.party_size_rating_adjustment,
                                self.config.party_spread_rating_adjustment,
                                self.config.max_party_rating_spread,
                            )?,
                            ratings,
                            enqueued_at: enqueued.0,
                            revision: *revision,
                            region: region.to_owned(),
                            mode,
                            search_delta: crate::matching::bucket::search_delta(
                                self.config.initial_elo_delta,
                                self.config.elo_step,
                                self.config.elo_step_seconds,
                                self.config.maximum_elo_delta,
                                0,
                                wait_seconds,
                            ),
                            wait_seconds,
                        })
                    })
                    .flatten()
            })
            .collect()
    }
    fn attempt_all_matches(&mut self) {
        let queues: BTreeSet<_> = self
            .tickets
            .values()
            .flat_map(|ticket| {
                ticket
                    .regions
                    .iter()
                    .cloned()
                    .map(move |region| (ticket.mode, region))
            })
            .collect();
        for (mode, region) in queues {
            self.attempt_match_batch(mode, &region);
        }
    }
    fn attempt_match_batch(&mut self, mode: QueueMode, region: &str) -> usize {
        if !crate::placement::feasible(&self.registry, mode, region) {
            return 0;
        }
        // Specs storages are the authoritative matching snapshot boundary. Domain maps are
        // deterministic lookup indexes used by command validation and atomic commit only.
        self.rebuild_ecs();
        let tickets = self.ecs_tickets(mode, region);
        let snapshot = CandidateSnapshot::new(tickets.clone());
        let workers = std::thread::available_parallelism().map_or(1, usize::from);
        let candidate_started = std::time::Instant::now();
        let candidates = dispatcher::generate(&snapshot, workers, 1024, |items| match mode {
            QueueMode::OneVsOne => one_v_one::build(items),
            QueueMode::FiveVsFive => five_v_five::build(items, 1024),
            QueueMode::FreeForAll => free_for_all::build(items, 1024),
        });
        self.metrics
            .candidate_compute_us
            .observe(candidate_started.elapsed().as_micros() as u64);
        let mut committed = 0;
        for candidate in candidates {
            let commit_started = std::time::Instant::now();
            if !self.claims.commit(&candidate, |ticket_id| {
                tickets.iter().any(|ticket| {
                    ticket.id == ticket_id
                        && self.parties.get(&ticket.party).is_some_and(|party| {
                            party.revision == ticket.revision
                                && party.state == PartyState::Queued
                                && party.members.iter().all(|player| {
                                    !self.offline.contains(player)
                                        && !self.player_proposal.contains_key(player)
                                        && !self.player_match.contains_key(player)
                                })
                        })
                        && self.tickets.contains_key(&ticket.party)
                })
            }) {
                continue;
            }
            self.commit_candidate(candidate, &tickets, mode, region);
            self.metrics
                .commit_us
                .observe(commit_started.elapsed().as_micros() as u64);
            committed += 1;
        }
        committed
    }
    fn commit_candidate(
        &mut self,
        candidate: crate::matching::Candidate,
        tickets: &[PartyTicket],
        mode: QueueMode,
        region: &str,
    ) {
        let selected: Vec<PartyId> = candidate
            .tickets
            .iter()
            .filter_map(|id| tickets.iter().find(|t| t.id == *id).map(|t| t.party))
            .collect();
        let owners: BTreeMap<PlayerId, PartyId> = selected
            .iter()
            .filter_map(|id| self.parties.get(id).map(|p| (id, p)))
            .flat_map(|(id, p)| p.members.iter().map(move |v| (*v, *id)))
            .collect();
        let mut proposal = Proposal::from_candidate(
            &candidate,
            owners,
            now_ms(),
            self.config.ready_timeout_seconds * 1000,
        );
        proposal.id = ProposalId::from_uuid(self.next_uuid(3));
        let proposal_id = proposal.id;
        let oldest_queued_since = selected
            .iter()
            .filter_map(|party| self.tickets.get(party).map(|ticket| ticket.queued_since_ms))
            .min()
            .unwrap_or_else(now_ms);
        self.metrics.queue_wait_us.observe(
            now_ms()
                .saturating_sub(oldest_queued_since)
                .saturating_mul(1000),
        );
        {
            let mut lifecycle = self.metrics.lifecycle.write();
            lifecycle.party_structure_difference_sum = lifecycle
                .party_structure_difference_sum
                .saturating_add(candidate.quality_key.2.unsigned_abs() as u64);
            lifecycle.elo_quality_sum = lifecycle
                .elo_quality_sum
                .saturating_add(candidate.quality_key.0.unsigned_abs() as u64)
                .saturating_add(candidate.quality_key.1.unsigned_abs() as u64);
        }
        let mut saved_tickets = Vec::new();
        for party_id in selected {
            if let Some(ticket) = self.tickets.remove(&party_id) {
                saved_tickets.push((party_id, ticket));
            }
            if let Some(p) = self.parties.get_mut(&party_id) {
                p.state = PartyState::AwaitingAccept;
                p.revision += 1;
            }
        }
        self.proposal_tickets.insert(proposal_id, saved_tickets);
        for player in proposal.responses.keys() {
            self.player_proposal.insert(*player, proposal_id);
            if let Some(tx) = self.events.get(player) {
                self.metrics.event_queue_high.observe(tx.len() as u64);
                let _ = tx.send(pb::ClientEvent {
                    event: Some(pb::client_event::Event::ProposalId(proposal_id.to_string())),
                    deadline_ms: proposal.deadline as i64,
                });
            }
        }
        self.proposals.insert(proposal_id, proposal);
        self.metrics.matches.add(1);
        tracing::info!(%proposal_id, ?mode, region, "match proposal created");
    }
    fn launch(&mut self, proposal_id: ProposalId) -> Result<MatchId, Status> {
        let proposal = self
            .proposals
            .get(&proposal_id)
            .cloned()
            .ok_or_else(|| Status::not_found("proposal"))?;
        let mode = if proposal.teams.len() == 8 {
            QueueMode::FreeForAll
        } else if proposal.teams.iter().map(Vec::len).sum::<usize>() == 10 {
            QueueMode::FiveVsFive
        } else {
            QueueMode::OneVsOne
        };
        let allowed_regions = self
            .proposal_tickets
            .get(&proposal_id)
            .map(|tickets| {
                tickets
                    .iter()
                    .map(|(_, ticket)| ticket.regions.iter().cloned().collect::<BTreeSet<_>>())
                    .reduce(|left, right| left.intersection(&right).cloned().collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let server_id = self
            .registry
            .servers
            .values()
            .filter(|s| {
                s.health == Health::Healthy
                    && allowed_regions.contains(&s.region)
                    && s.modes.contains(&mode)
                    && self.controls.contains_key(&s.id)
                    && s.instances.len() < s.max_instances as usize
            })
            .filter_map(|s| {
                let cost = s.mode_costs.get(&mode).copied()?;
                let required = s.capacity_used.checked_add(cost)?;
                let remaining = s.capacity_total.checked_sub(required)?;
                let load_ppm =
                    u64::from(s.capacity_used) * 1_000_000 / u64::from(s.capacity_total.max(1));
                Some((remaining, load_ppm, s.failures, s.instances.len(), s.id))
            })
            .min()
            .map(|choice| choice.4)
            .ok_or_else(|| Status::unavailable("no feasible game server"))?;
        let match_id = MatchId::from_uuid(self.next_uuid(4));
        let server = self.registry.servers.get_mut(&server_id).unwrap();
        let cost = server.mode_costs[&mode];
        server.capacity_used += cost;
        server.instances.insert(
            match_id,
            Instance {
                match_id,
                cost,
                state: InstanceState::Reserved,
                endpoint: None,
                connection_token: None,
            },
        );
        let teams = proposal
            .teams
            .iter()
            .enumerate()
            .map(|(i, p)| pb::Team {
                team_index: i as u32,
                player_ids: p.iter().map(ToString::to_string).collect(),
            })
            .collect();
        let command = pb::ErpsControl {
            message: Some(pb::erps_control::Message::Launch(pb::LaunchMatch {
                match_id: match_id.to_string(),
                mode: pb_mode(mode),
                teams,
                reserved_cost: cost,
            })),
        };
        let control = self
            .controls
            .get(&server_id)
            .ok_or_else(|| Status::unavailable("server control stream unavailable"))?;
        self.metrics
            .control_queue_high
            .observe(control.max_capacity().saturating_sub(control.capacity()) as u64);
        let sent = control.try_send(Ok(command));
        if sent.is_err() {
            let _ = crate::placement::release(&mut self.registry, server_id, match_id);
            return Err(Status::resource_exhausted("server control stream full"));
        }
        self.launches.insert(match_id, (server_id, proposal_id));
        self.launch_deadlines.insert(
            match_id,
            now_ms().saturating_add(self.config.placement_timeout_seconds * 1000),
        );
        Ok(match_id)
    }
    fn retry_launch(
        &mut self,
        match_id: MatchId,
        failed_server: ServerId,
        proposal_id: ProposalId,
    ) -> Result<(), Status> {
        let proposal = self
            .proposals
            .get(&proposal_id)
            .ok_or_else(|| Status::not_found("proposal"))?;
        let mode = proposal_mode(proposal);
        let region = self
            .proposal_tickets
            .get(&proposal_id)
            .and_then(|tickets| {
                tickets
                    .iter()
                    .map(|(_, ticket)| ticket.regions.iter().cloned().collect::<BTreeSet<_>>())
                    .reduce(|left, right| left.intersection(&right).cloned().collect())
            })
            .and_then(|regions| regions.into_iter().next())
            .ok_or_else(|| Status::failed_precondition("proposal has no common region"))?;
        let server_id = crate::placement::retry_after_failure(
            &mut self.registry,
            failed_server,
            match_id,
            mode,
            &region,
        )
        .map_err(|_| Status::unavailable("no alternate game server"))?;
        let cost = self.registry.servers[&server_id].instances[&match_id].cost;
        let teams = proposal
            .teams
            .iter()
            .enumerate()
            .map(|(i, players)| pb::Team {
                team_index: i as u32,
                player_ids: players.iter().map(ToString::to_string).collect(),
            })
            .collect();
        let sent = self.controls.get(&server_id).map(|control| {
            self.metrics
                .control_queue_high
                .observe(control.max_capacity().saturating_sub(control.capacity()) as u64);
            control.try_send(Ok(pb::ErpsControl {
                message: Some(pb::erps_control::Message::Launch(pb::LaunchMatch {
                    match_id: match_id.to_string(),
                    mode: pb_mode(mode),
                    teams,
                    reserved_cost: cost,
                })),
            }))
        });
        if !matches!(sent, Some(Ok(()))) {
            let _ = crate::placement::release(&mut self.registry, server_id, match_id);
            return Err(Status::unavailable(
                "alternate game server control stream unavailable",
            ));
        }
        self.launches.insert(match_id, (server_id, proposal_id));
        self.launch_deadlines.insert(
            match_id,
            now_ms().saturating_add(self.config.placement_timeout_seconds * 1000),
        );
        Ok(())
    }
    fn launch_result(
        &mut self,
        server_id: ServerId,
        result: pb::LaunchResult,
    ) -> Result<(), Status> {
        let match_id = MatchId::from_str(&result.match_id)
            .map_err(|_| Status::invalid_argument("match_id"))?;
        let (owner, proposal_id) = self
            .launches
            .get(&match_id)
            .copied()
            .ok_or_else(|| Status::not_found("launch"))?;
        if owner != server_id {
            return Err(Status::permission_denied(
                "launch belongs to another server",
            ));
        }
        let proposal = self
            .proposals
            .get(&proposal_id)
            .ok_or_else(|| Status::not_found("proposal"))?;
        let server = self
            .registry
            .servers
            .get_mut(&server_id)
            .ok_or_else(|| Status::not_found("server"))?;
        let instance = server
            .instances
            .get_mut(&match_id)
            .ok_or_else(|| Status::not_found("instance"))?;
        if result.state.eq_ignore_ascii_case("accepted") {
            match instance.state {
                InstanceState::Reserved => instance.state = InstanceState::Accepted,
                InstanceState::Accepted => return Ok(()),
                _ => {
                    return Err(Status::failed_precondition(
                        "launch acceptance is only valid for a reserved instance",
                    ));
                }
            }
            tracing::info!(%match_id, %server_id, "game instance accepted launch");
        } else if result.state.eq_ignore_ascii_case("ready") {
            if result.endpoint.trim().is_empty() || result.connection_token.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "ready launch requires endpoint and connection_token",
                ));
            }
            if !matches!(
                instance.state,
                InstanceState::Accepted | InstanceState::Ready | InstanceState::Running
            ) {
                return Err(Status::failed_precondition(
                    "launch readiness requires an accepted instance",
                ));
            }
            let already_published = proposal
                .responses
                .keys()
                .all(|player| self.player_match.get(player) == Some(&match_id));
            if already_published {
                instance.state = InstanceState::Running;
                instance.endpoint = Some(result.endpoint);
                instance.connection_token = Some(result.connection_token);
                self.launch_deadlines.remove(&match_id);
                return Ok(());
            }
            if let Some(deadline) = self.launch_deadlines.remove(&match_id) {
                let started_at =
                    deadline.saturating_sub(self.config.placement_timeout_seconds * 1000);
                self.metrics
                    .launch_us
                    .observe(now_ms().saturating_sub(started_at).saturating_mul(1000));
            }
            tracing::info!(%match_id, %server_id, "game instance ready");
            instance.state = InstanceState::Ready;
            instance.endpoint = Some(result.endpoint.clone());
            instance.connection_token = Some(result.connection_token.clone());
            let mode = if proposal.teams.len() == 8 {
                QueueMode::FreeForAll
            } else if proposal.teams.iter().map(Vec::len).sum::<usize>() == 10 {
                QueueMode::FiveVsFive
            } else {
                QueueMode::OneVsOne
            };
            let matched = pb::MatchEvent {
                match_id: match_id.to_string(),
                mode: pb_mode(mode),
                teams: proposal
                    .teams
                    .iter()
                    .enumerate()
                    .map(|(i, p)| pb::Team {
                        team_index: i as u32,
                        player_ids: p.iter().map(ToString::to_string).collect(),
                    })
                    .collect(),
                endpoint: result.endpoint,
                connection_token: result.connection_token,
            };
            for player in proposal.responses.keys() {
                self.player_match.insert(*player, match_id);
                self.player_proposal.remove(player);
                if let Some(tx) = self.events.get(player) {
                    self.metrics.event_queue_high.observe(tx.len() as u64);
                    let _ = tx.send(pb::ClientEvent {
                        event: Some(pb::client_event::Event::Matched(matched.clone())),
                        deadline_ms: 0,
                    });
                }
            }
            // Once the connectable match is published, the instance is authoritative Running.
            // Keeping it in Ready would incorrectly make heartbeat loss eligible for retry/migration.
            instance.state = InstanceState::Running;
            let matched_parties: Vec<_> = self
                .proposal_tickets
                .get(&proposal_id)
                .into_iter()
                .flatten()
                .filter_map(|(party_id, _)| {
                    let party = self.parties.get_mut(party_id)?;
                    party.state = PartyState::Matched;
                    Some(party.clone())
                })
                .collect();
            for party in &matched_parties {
                self.emit_party(party);
            }
        } else if result.state.eq_ignore_ascii_case("rejected")
            || result.state.eq_ignore_ascii_case("failed")
        {
            if !matches!(
                instance.state,
                InstanceState::Reserved | InstanceState::Accepted
            ) {
                return Err(Status::failed_precondition(
                    "launch rejection is only valid before an instance is ready",
                ));
            }
            self.metrics.launch_failures.add(1);
            let _ = server;
            self.retry_launch(match_id, server_id, proposal_id)?;
        } else {
            return Err(Status::invalid_argument("unknown launch result state"));
        }
        Ok(())
    }

    fn apply_server_control(
        &mut self,
        id: ServerId,
        generation: ServerGeneration,
        reply_stream: mpsc::Sender<Result<pb::ErpsControl, Status>>,
        message: Option<pb::server_control::Message>,
    ) -> Result<(), Status> {
        if !self.registry.is_current_generation(id, generation) {
            // A replaced process may keep its old stream alive briefly. Every control message
            // from it is ignored before it can replace the current sender or mutate the new
            // generation's ledger.
            return Ok(());
        }
        self.controls.insert(id, reply_stream.clone());
        match message {
            Some(pb::server_control::Message::Heartbeat(heartbeat)) => self
                .registry
                .heartbeat(id, generation, now_ms(), heartbeat.capacity_used)
                .map_err(|error| Status::failed_precondition(error.to_string()))?,
            Some(pb::server_control::Message::LaunchResult(result)) => {
                self.launch_result(id, result)?
            }
            Some(pb::server_control::Message::Instance(value)) => {
                let match_id = MatchId::from_str(&value.match_id)
                    .map_err(|_| Status::invalid_argument("match_id"))?;
                let server = self
                    .registry
                    .servers
                    .get_mut(&id)
                    .ok_or_else(|| Status::not_found("server"))?;
                let instance = server
                    .instances
                    .get_mut(&match_id)
                    .ok_or_else(|| Status::not_found("instance"))?;
                if instance.cost != value.reserved_cost {
                    return Err(Status::failed_precondition(
                        "reported instance cost differs from reservation",
                    ));
                }
                let previous = instance.state;
                let next = parse_instance(&value.state)?;
                if !valid_reported_instance_transition(previous, next) {
                    return Err(Status::failed_precondition(
                        "invalid game instance lifecycle transition",
                    ));
                }
                if matches!(next, InstanceState::Ready | InstanceState::Running)
                    && (value.endpoint.trim().is_empty()
                        || value.connection_token.trim().is_empty())
                {
                    return Err(Status::invalid_argument(
                        "ready or running instance requires endpoint and connection_token",
                    ));
                }
                instance.state = next;
                if matches!(next, InstanceState::Ready | InstanceState::Running) {
                    instance.endpoint = Some(value.endpoint);
                    instance.connection_token = Some(value.connection_token);
                }
            }
            Some(pb::server_control::Message::MatchResult(result)) => {
                let match_id = MatchId::from_str(&result.match_id)
                    .map_err(|_| Status::invalid_argument("match_id"))?;
                if !self.completed_match_results.contains(&match_id) {
                    self.finish_match(id, result)?;
                    self.remember_completed_match_result(match_id);
                }
                reply_stream
                    .try_send(Ok(pb::ErpsControl {
                        message: Some(pb::erps_control::Message::MatchResultAck(
                            match_id.to_string(),
                        )),
                    }))
                    .map_err(|_| Status::unavailable("match result acknowledgement queue full"))?;
            }
            None => {}
        }
        Ok(())
    }
    fn finish_match(&mut self, server_id: ServerId, result: pb::MatchResult) -> Result<(), Status> {
        let match_id = MatchId::from_str(&result.match_id)
            .map_err(|_| Status::invalid_argument("match_id"))?;
        let (owner, proposal_id) = self
            .launches
            .get(&match_id)
            .copied()
            .ok_or_else(|| Status::not_found("match"))?;
        if owner != server_id {
            return Err(Status::permission_denied("match belongs to another server"));
        }
        let instance = self
            .registry
            .servers
            .get(&server_id)
            .and_then(|server| server.instances.get(&match_id))
            .ok_or_else(|| Status::not_found("instance"))?;
        if instance.state != InstanceState::Running {
            return Err(Status::failed_precondition(
                "match result requires a running instance",
            ));
        }
        let proposal = self
            .proposals
            .get(&proposal_id)
            .cloned()
            .ok_or_else(|| Status::not_found("proposal"))?;
        let mode = proposal_mode(&proposal);
        let mut ranks = BTreeMap::new();
        for placement in result.placements {
            let player = PlayerId::from_str(&placement.player_id)
                .map_err(|_| Status::invalid_argument("placement player_id"))?;
            let rank = u8::try_from(placement.rank)
                .ok()
                .filter(|rank| *rank > 0)
                .ok_or_else(|| Status::invalid_argument("placement rank"))?;
            if ranks.insert(player, rank).is_some() {
                return Err(Status::invalid_argument("duplicate placement player"));
            }
        }
        let roster: BTreeSet<_> = proposal.teams.iter().flatten().copied().collect();
        if ranks.keys().copied().collect::<BTreeSet<_>>() != roster {
            return Err(Status::invalid_argument(
                "placements must cover the exact roster",
            ));
        }
        if ranks.values().any(|rank| usize::from(*rank) > roster.len()) {
            return Err(Status::invalid_argument("placement rank exceeds roster"));
        }
        if mode == QueueMode::FiveVsFive {
            let team_rank = |team: &[PlayerId]| {
                team.first()
                    .and_then(|first| ranks.get(first).copied())
                    .filter(|rank| team.iter().all(|player| ranks.get(player) == Some(rank)))
            };
            let first = team_rank(&proposal.teams[0]);
            let second = team_rank(&proposal.teams[1]);
            if first.is_none() || second.is_none() || first == second {
                return Err(Status::invalid_argument(
                    "5v5 placements require one consistent, distinct rank per team",
                ));
            }
        }
        let policy = crate::rating::RatingPolicy {
            established_k: self.config.elo_established_k,
            provisional_k: self.config.elo_provisional_k,
            provisional_matches: self.config.elo_provisional_matches,
            maximum_delta: self.config.elo_maximum_match_delta,
        };
        match mode {
            QueueMode::OneVsOne => {
                let a = proposal.teams[0][0];
                let b = proposal.teams[1][0];
                let ar = *self.ratings.get(&(a, mode)).unwrap_or(&1000);
                let br = *self.ratings.get(&(b, mode)).unwrap_or(&1000);
                let score = if ranks[&a] < ranks[&b] {
                    1.0
                } else if ranks[&a] == ranks[&b] {
                    0.5
                } else {
                    0.0
                };
                let (next_a, next_b) = crate::rating::one_vs_one(
                    ar,
                    br,
                    score,
                    *self.completed_games.get(&(a, mode)).unwrap_or(&0),
                    *self.completed_games.get(&(b, mode)).unwrap_or(&0),
                    policy,
                );
                self.ratings.insert((a, mode), next_a);
                self.ratings.insert((b, mode), next_b);
            }
            QueueMode::FiveVsFive => {
                let first_wins = ranks[&proposal.teams[0][0]] < ranks[&proposal.teams[1][0]];
                let (winners, losers) = if first_wins {
                    (&proposal.teams[0], &proposal.teams[1])
                } else {
                    (&proposal.teams[1], &proposal.teams[0])
                };
                let winner_ratings: Vec<_> = winners
                    .iter()
                    .map(|p| *self.ratings.get(&(*p, mode)).unwrap_or(&1000))
                    .collect();
                let loser_ratings: Vec<_> = losers
                    .iter()
                    .map(|p| *self.ratings.get(&(*p, mode)).unwrap_or(&1000))
                    .collect();
                let (next_winners, next_losers) =
                    crate::rating::team_update(&winner_ratings, &loser_ratings, policy);
                for (player, rating) in winners.iter().zip(next_winners) {
                    self.ratings.insert((*player, mode), rating);
                }
                for (player, rating) in losers.iter().zip(next_losers) {
                    self.ratings.insert((*player, mode), rating);
                }
            }
            QueueMode::FreeForAll => {
                let players: Vec<_> = proposal.teams.iter().flatten().copied().collect();
                let current: Vec<_> = players
                    .iter()
                    .map(|p| *self.ratings.get(&(*p, mode)).unwrap_or(&1000))
                    .collect();
                let ordered_ranks: Vec<_> = players.iter().map(|p| ranks[p]).collect();
                for (player, rating) in players.into_iter().zip(crate::rating::free_for_all(
                    &current,
                    &ordered_ranks,
                    policy,
                )) {
                    self.ratings.insert((player, mode), rating);
                }
            }
        }
        for player in roster {
            *self.completed_games.entry((player, mode)).or_default() += 1;
            let completed_since = self
                .completed_since_credit_recovery
                .entry(player)
                .or_default();
            let previous_score = self.credit.get(&player).copied().unwrap_or(100);
            let outcome = crate::credit::apply(
                previous_score,
                self.recent_credit_violations
                    .get(&player)
                    .copied()
                    .unwrap_or(0),
                *completed_since,
                crate::credit::CreditCause::CompletedMatch,
                crate::credit::CreditPolicy::default(),
            );
            self.credit.insert(player, outcome.score);
            if outcome.score > previous_score {
                *completed_since = 0;
            } else {
                *completed_since = completed_since.saturating_add(1);
            }
            self.player_match.remove(&player);
            self.pending_profile_saves
                .push((player, self.current_profile(player)));
        }
        let finished_parties = proposal.parties.values().copied().collect::<BTreeSet<_>>();
        for party_id in &finished_parties {
            if let Some(party) = self.parties.get_mut(party_id) {
                party.state = PartyState::Idle;
                party.revision = party.revision.saturating_add(1);
            }
        }
        for party_id in finished_parties {
            if let Some(party) = self.parties.get(&party_id) {
                self.emit_party(party);
            }
        }
        crate::placement::release(&mut self.registry, server_id, match_id)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        self.launches.remove(&match_id);
        self.launch_deadlines.remove(&match_id);
        self.proposal_tickets.remove(&proposal_id);
        self.proposals.remove(&proposal_id);
        tracing::info!(%match_id, %server_id, ?mode, "match result committed");
        Ok(())
    }
}
fn proposal_mode(proposal: &Proposal) -> QueueMode {
    if proposal.teams.len() == 8 {
        QueueMode::FreeForAll
    } else if proposal.teams.iter().map(Vec::len).sum::<usize>() == 10 {
        QueueMode::FiveVsFive
    } else {
        QueueMode::OneVsOne
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn check_api(api: Option<&pb::ApiVersion>) -> Result<(), Status> {
    match api {
        Some(v) if v.major == 1 => Ok(()),
        _ => Err(Status::failed_precondition("unsupported API major version")),
    }
}

fn credit_suspension_active(
    score: u8,
    minimum: u8,
    suspended_until_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    score < minimum && suspended_until_ms.is_some_and(|deadline| now_ms <= deadline)
}
fn ok(id: impl ToString, revision: u64) -> pb::OperationResult {
    pb::OperationResult {
        accepted: true,
        code: "OK".into(),
        message: String::new(),
        entity_id: id.to_string(),
        revision,
    }
}
fn mode(v: i32) -> Result<QueueMode, Status> {
    match pb::QueueMode::try_from(v).ok() {
        Some(pb::QueueMode::OneVOne) => Ok(QueueMode::OneVsOne),
        Some(pb::QueueMode::FiveVFive) => Ok(QueueMode::FiveVsFive),
        Some(pb::QueueMode::FreeForAll) => Ok(QueueMode::FreeForAll),
        _ => Err(Status::invalid_argument("queue mode is required")),
    }
}
fn pb_mode(v: QueueMode) -> i32 {
    match v {
        QueueMode::OneVsOne => pb::QueueMode::OneVOne as i32,
        QueueMode::FiveVsFive => pb::QueueMode::FiveVFive as i32,
        QueueMode::FreeForAll => pb::QueueMode::FreeForAll as i32,
    }
}
fn party_status(e: impl std::fmt::Display) -> Status {
    Status::failed_precondition(e.to_string())
}

pub trait TokenValidator: Send + Sync + 'static {
    fn validate(&self, token: &str) -> Result<PlayerId, Status>;
}
pub trait ServerTokenValidator: Send + Sync + 'static {
    fn validate(&self, token: &str, server: ServerId) -> Result<(), Status>;
}
#[derive(Default)]
pub struct DevelopmentTokenValidator;
impl TokenValidator for DevelopmentTokenValidator {
    fn validate(&self, token: &str) -> Result<PlayerId, Status> {
        if token.trim().is_empty() {
            return Err(Status::unauthenticated("auth token is required"));
        }
        let mut hash = 0xcbf29ce484222325u64;
        for byte in token.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        Ok(PlayerId::from_uuid(uuid::Uuid::from_u128(
            ((hash as u128) << 64) | (!hash as u128),
        )))
    }
}
impl ServerTokenValidator for DevelopmentTokenValidator {
    fn validate(&self, token: &str, _: ServerId) -> Result<(), Status> {
        if token.trim().is_empty() {
            Err(Status::unauthenticated("server auth token is required"))
        } else {
            Ok(())
        }
    }
}
pub struct StaticTokenValidator {
    identities: BTreeMap<String, PlayerId>,
}
pub struct StaticServerTokenValidator {
    identities: BTreeMap<String, ServerId>,
}
impl StaticServerTokenValidator {
    pub fn from_json_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let encoded: BTreeMap<String, String> = serde_json::from_slice(&std::fs::read(path)?)?;
        let identities = encoded
            .into_iter()
            .map(|(token, server)| Ok((token, ServerId::from_str(&server)?)))
            .collect::<Result<_, crate::id::ParseIdError>>()?;
        Ok(Self { identities })
    }
}
impl ServerTokenValidator for StaticServerTokenValidator {
    fn validate(&self, token: &str, server: ServerId) -> Result<(), Status> {
        match self.identities.get(token) {
            Some(expected) if *expected == server => Ok(()),
            _ => Err(Status::unauthenticated("invalid game server bearer token")),
        }
    }
}
impl StaticTokenValidator {
    pub fn from_json_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let encoded: BTreeMap<String, String> = serde_json::from_slice(&std::fs::read(path)?)?;
        let identities = encoded
            .into_iter()
            .map(|(token, player)| Ok((token, PlayerId::from_str(&player)?)))
            .collect::<Result<_, crate::id::ParseIdError>>()?;
        Ok(Self { identities })
    }
}
impl TokenValidator for StaticTokenValidator {
    fn validate(&self, token: &str) -> Result<PlayerId, Status> {
        self.identities
            .get(token)
            .copied()
            .ok_or_else(|| Status::unauthenticated("invalid bearer token"))
    }
}
#[derive(Clone)]
pub struct MatchmakingGrpc {
    core: CoreHandle,
    validator: Arc<dyn TokenValidator>,
}
#[derive(Clone)]
pub struct GameServerGrpc {
    core: CoreHandle,
    validator: Arc<dyn ServerTokenValidator>,
}
#[derive(Clone)]
pub struct AdminGrpc {
    core: CoreHandle,
}
impl MatchmakingGrpc {
    pub fn new(core: CoreHandle) -> Self {
        Self {
            core,
            validator: Arc::new(DevelopmentTokenValidator),
        }
    }
    pub fn with_validator(core: CoreHandle, validator: Arc<dyn TokenValidator>) -> Self {
        Self { core, validator }
    }
}
impl GameServerGrpc {
    pub fn new(core: CoreHandle) -> Self {
        Self {
            core,
            validator: Arc::new(DevelopmentTokenValidator),
        }
    }
    pub fn with_validator(core: CoreHandle, validator: Arc<dyn ServerTokenValidator>) -> Self {
        Self { core, validator }
    }
}
impl AdminGrpc {
    pub fn new(core: CoreHandle) -> Self {
        Self { core }
    }
}

#[tonic::async_trait]
impl MatchmakingService for MatchmakingGrpc {
    async fn open_session(
        &self,
        request: Request<pb::ConnectRequest>,
    ) -> Result<Response<pb::ConnectResponse>, Status> {
        let req = request.into_inner();
        check_api(req.api.as_ref())?;
        let player = self.validator.validate(&req.auth_token)?;
        let profile = self
            .core
            .profiles
            .load(player)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let value = self
            .core
            .call(move |s| {
                let token = uuid::Uuid::new_v4().simple().to_string();
                s.sessions.insert(token.clone(), player);
                if s.offline.remove(&player) || s.disconnect_deadlines.remove(&player).is_some() {
                    s.metrics.reconnects.add(1);
                }
                s.current_session.insert(player, token.clone());
                s.credit.insert(player, profile.credit);
                s.recent_credit_violations
                    .insert(player, profile.recent_credit_violations);
                if profile.credit < s.config.minimum_credit {
                    let suspension_deadline = now_ms().saturating_add(
                        s.config
                            .credit_suspension_base_seconds
                            .saturating_mul(u64::from(profile.recent_credit_violations.max(1)))
                            .saturating_mul(1000),
                    );
                    s.credit_suspended_until
                        .entry(player)
                        .or_insert(suspension_deadline);
                }
                for (mode, rating) in profile.ratings {
                    s.ratings.insert((player, mode), rating);
                }
                for (mode, completed) in profile.completed_matches {
                    s.completed_games.insert((player, mode), completed);
                }
                let (events, _) = broadcast::channel(s.config.event_queue_capacity);
                s.events.insert(player, events);
                Ok(pb::ConnectResponse {
                    session_token: token,
                    player_id: player.to_string(),
                    api: Some(pb::ApiVersion {
                        major: 1,
                        minor: 0,
                        capabilities: vec!["ready-check".into(), "server-placement".into()],
                    }),
                })
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn create_party(
        &self,
        request: Request<pb::CreatePartyRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                if s.player_party.contains_key(&player) {
                    return Err(Status::already_exists("player already belongs to a party"));
                }
                let mut party = Party::new(player, &req.name).map_err(party_status)?;
                party.id = PartyId::from_uuid(s.next_uuid(1));
                let id = party.id;
                let rev = party.revision;
                s.emit_party(&party);
                s.parties.insert(id, party);
                s.player_party.insert(player, id);
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn create_invite(
        &self,
        request: Request<pb::InviteRequest>,
    ) -> Result<Response<pb::InviteResponse>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, _) = s.meta(req.meta.as_ref())?;
                let id = s.party_for(&req.party_id)?;
                let p = s
                    .parties
                    .get(&id)
                    .ok_or_else(|| Status::not_found("party"))?;
                if p.leader != player {
                    return Err(Status::permission_denied("leader required"));
                }
                if p.revision != req.revision {
                    return Err(Status::aborted("party revision conflict"));
                }
                if p.state != PartyState::Idle {
                    return Err(Status::failed_precondition("party is frozen"));
                }
                let ttl = req.ttl_seconds.clamp(1, 3600) as u64 * 1000;
                let token = s.invites.create(id, now_ms(), ttl, req.uses.clamp(1, 100));
                Ok(pb::InviteResponse {
                    token,
                    expires_at_ms: (now_ms() + ttl) as i64,
                })
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn join_party(
        &self,
        request: Request<pb::JoinPartyRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                if s.player_party.contains_key(&player) {
                    return Err(Status::already_exists("player already belongs to a party"));
                }
                let id = s
                    .invites
                    .consume(&req.invite_token, now_ms())
                    .ok_or_else(|| Status::not_found("invite expired or exhausted"))?;
                let (rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    p.join(player).map_err(party_status)?;
                    (p.revision, p.clone())
                };
                s.player_party.insert(player, id);
                s.emit_party(&cloned);
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn leave_party(
        &self,
        request: Request<pb::PartyRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                let id = s.party_for(&req.party_id)?;
                let (empty, rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    if p.revision != req.revision {
                        return Err(Status::aborted("party revision conflict"));
                    }
                    let empty = p.leave(player).map_err(party_status)?;
                    (empty, p.revision, p.clone())
                };
                s.player_party.remove(&player);
                if empty {
                    s.parties.remove(&id);
                } else {
                    s.emit_party(&cloned);
                }
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn kick_member(
        &self,
        request: Request<pb::MemberRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                let id = s.party_for(&req.party_id)?;
                let target = PlayerId::from_str(&req.player_id)
                    .map_err(|_| Status::invalid_argument("invalid player_id"))?;
                let (rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    p.kick(player, req.revision, target).map_err(party_status)?;
                    (p.revision, p.clone())
                };
                s.player_party.remove(&target);
                s.emit_party(&cloned);
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn rename_party(
        &self,
        request: Request<pb::RenamePartyRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                let id = s.party_for(&req.party_id)?;
                let (rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    p.rename(player, req.revision, &req.name)
                        .map_err(party_status)?;
                    (p.revision, p.clone())
                };
                s.emit_party(&cloned);
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn enqueue(
        &self,
        request: Request<pb::EnqueueRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                let id = s.party_for(&req.party_id)?;
                let m = mode(req.mode)?;
                let eligibility = s
                    .parties
                    .get(&id)
                    .ok_or_else(|| Status::not_found("party"))?;
                if eligibility.members.iter().any(|member| {
                    s.offline.contains(member)
                        || !s.sessions.values().any(|online| online == member)
                }) {
                    return Err(Status::failed_precondition(
                        "all party members must be online",
                    ));
                }
                let eligibility_checked_at = now_ms();
                if eligibility.members.iter().any(|member| {
                    credit_suspension_active(
                        s.credit.get(member).copied().unwrap_or(100),
                        s.config.minimum_credit,
                        s.credit_suspended_until.get(member).copied(),
                        eligibility_checked_at,
                    )
                }) {
                    return Err(Status::failed_precondition(
                        "party contains a credit-suspended player",
                    ));
                }
                let member_ratings: Vec<_> = eligibility
                    .members
                    .iter()
                    .map(|member| *s.ratings.get(&(*member, m)).unwrap_or(&1000))
                    .collect();
                if i64::from(*member_ratings.iter().max().unwrap_or(&1000))
                    - i64::from(*member_ratings.iter().min().unwrap_or(&1000))
                    > i64::from(s.config.max_party_rating_spread)
                {
                    return Err(Status::failed_precondition(
                        "party rating spread exceeds configured maximum",
                    ));
                }
                if req.allowed_regions.is_empty()
                    || req
                        .allowed_regions
                        .iter()
                        .any(|region| region.trim().is_empty())
                {
                    return Err(Status::invalid_argument(
                        "at least one valid allowed region is required",
                    ));
                }
                let (rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    if p.leader != player {
                        return Err(Status::permission_denied("leader required"));
                    }
                    if p.revision != req.revision {
                        return Err(Status::aborted("party revision conflict"));
                    }
                    p.validate_enqueue(m).map_err(party_status)?;
                    p.state = PartyState::Queued;
                    p.revision += 1;
                    (p.revision, p.clone())
                };
                let ticket = TicketRecord {
                    id: TicketId::from_uuid(s.next_uuid(2)),
                    mode: m,
                    regions: req.allowed_regions,
                    enqueued_at: s.logical_clock,
                    queued_since_ms: now_ms(),
                };
                let tid = ticket.id;
                s.tickets.insert(id, ticket);
                s.emit_party(&cloned);
                Ok(s.remember(player, rid, ok(tid, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn cancel_queue(
        &self,
        request: Request<pb::PartyRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        let req = request.into_inner();
        let value = self
            .core
            .call(move |s| {
                let (player, rid) = s.meta(req.meta.as_ref())?;
                if let Some(v) = s.cached(player, &rid) {
                    return Ok(v);
                }
                let id = s.party_for(&req.party_id)?;
                let (rev, cloned) = {
                    let p = s
                        .parties
                        .get_mut(&id)
                        .ok_or_else(|| Status::not_found("party"))?;
                    if p.leader != player {
                        return Err(Status::permission_denied("leader required"));
                    }
                    if p.revision != req.revision {
                        return Err(Status::aborted("party revision conflict"));
                    }
                    p.state = PartyState::Idle;
                    p.revision += 1;
                    (p.revision, p.clone())
                };
                s.tickets.remove(&id);
                s.emit_party(&cloned);
                Ok(s.remember(player, rid, ok(id, rev)))
            })
            .await?;
        Ok(Response::new(value))
    }
    async fn accept_match(
        &self,
        request: Request<pb::ProposalResponseRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        proposal_response(&self.core, request.into_inner(), true)
            .await
            .map(Response::new)
    }
    async fn reject_match(
        &self,
        request: Request<pb::ProposalResponseRequest>,
    ) -> Result<Response<pb::OperationResult>, Status> {
        proposal_response(&self.core, request.into_inner(), false)
            .await
            .map(Response::new)
    }
    async fn get_state(
        &self,
        request: Request<pb::StateRequest>,
    ) -> Result<Response<pb::ClientState>, Status> {
        let req = request.into_inner();
        check_api(req.api.as_ref())?;
        let value = self
            .core
            .call(move |s| {
                let player = s.player(&req.session_token)?;
                Ok(s.client_state(player))
            })
            .await?;
        Ok(Response::new(value))
    }
    type WatchEventsStream = RpcStream<pb::ClientEvent>;
    async fn watch_events(
        &self,
        request: Request<pb::WatchEventsRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        let req = request.into_inner();
        check_api(req.api.as_ref())?;
        let token = req.session_token.clone();
        let (player, rx) = self
            .core
            .call(move |s| {
                let player = s.player(&req.session_token)?;
                let rx = s
                    .events
                    .get(&player)
                    .map(|v| v.subscribe())
                    .ok_or_else(|| Status::not_found("event channel"))?;
                Ok((player, rx))
            })
            .await?;
        let stream = DisconnectAwareEvents {
            inner: BroadcastStream::new(rx),
            core: self.core.clone(),
            player,
            session_token: token,
        };
        Ok(Response::new(Box::pin(stream)))
    }
}
async fn proposal_response(
    core: &CoreHandle,
    req: pb::ProposalResponseRequest,
    accepted: bool,
) -> Result<pb::OperationResult, Status> {
    core.call(move |s| {
        let (player, rid) = s.meta(req.meta.as_ref())?;
        if let Some(v) = s.cached(player, &rid) {
            return Ok(v);
        }
        let proposal_id = ProposalId::from_str(&req.proposal_id)
            .map_err(|_| Status::invalid_argument("proposal_id"))?;
        if s.player_proposal.get(&player) != Some(&proposal_id) {
            return Err(Status::failed_precondition("stale proposal"));
        }
        let state = s
            .proposals
            .get_mut(&proposal_id)
            .ok_or_else(|| Status::not_found("proposal"))?
            .respond(proposal_id, player, accepted)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        if accepted {
            s.metrics.ready_accepts.add(1);
        }
        if state == ProposalState::Cancelled {
            s.cancel_proposal(proposal_id, false);
        }
        if state == ProposalState::AwaitingPlacement {
            if let Some(proposal) = s.proposals.get(&proposal_id) {
                let created_at = proposal
                    .deadline
                    .saturating_sub(s.config.ready_timeout_seconds * 1000);
                s.metrics
                    .ready_check_us
                    .observe(now_ms().saturating_sub(created_at).saturating_mul(1000));
            }
            let placement_started = std::time::Instant::now();
            match s.launch(proposal_id) {
                Ok(_) => {}
                Err(status) if status.code() == tonic::Code::Unavailable => {
                    s.placement_waiting.insert(
                        proposal_id,
                        now_ms().saturating_add(s.config.placement_timeout_seconds * 1000),
                    );
                }
                Err(status) => return Err(status),
            }
            s.metrics
                .placement_us
                .observe(placement_started.elapsed().as_micros() as u64);
        }
        let result = pb::OperationResult {
            accepted: true,
            code: if accepted { "ACCEPTED" } else { "REJECTED" }.into(),
            message: String::new(),
            entity_id: proposal_id.to_string(),
            revision: 0,
        };
        Ok(s.remember(player, rid, result))
    })
    .await
}

#[tonic::async_trait]
impl GameServerService for GameServerGrpc {
    async fn register(
        &self,
        request: Request<pb::RegisterServerRequest>,
    ) -> Result<Response<pb::RegisterServerResponse>, Status> {
        let req = request.into_inner();
        check_api(req.api.as_ref())?;
        let server_id = ServerId::from_str(&req.server_id)
            .map_err(|_| Status::invalid_argument("invalid server_id"))?;
        self.validator.validate(&req.auth_token, server_id)?;
        let value = self
            .core
            .call(move |s| {
                let id = server_id;
                if s.config.tls_certificate_path.is_some() && req.server_class.is_empty() {
                    return Err(Status::permission_denied(
                        "production game servers require a trusted server class",
                    ));
                }
                let mut costs = BTreeMap::new();
                for c in req.mode_costs {
                    costs.insert(mode(c.mode)?, c.cost);
                }
                if let Some(policy) = s.config.server_classes.get(&req.server_class) {
                    let trusted_costs = policy
                        .mode_costs
                        .iter()
                        .map(|(name, cost)| {
                            let mode = match name.to_ascii_lowercase().as_str() {
                                "1v1" | "one_vs_one" => QueueMode::OneVsOne,
                                "5v5" | "five_vs_five" => QueueMode::FiveVsFive,
                                "ffa8" | "free_for_all" => QueueMode::FreeForAll,
                                _ => return Err(Status::invalid_argument("server class mode")),
                            };
                            Ok((mode, *cost))
                        })
                        .collect::<Result<BTreeMap<_, _>, Status>>()?;
                    if costs != trusted_costs {
                        return Err(Status::permission_denied(
                            "reported mode costs differ from trusted server class",
                        ));
                    }
                }
                let mut instances = BTreeMap::new();
                for reported in req.instances {
                    let match_id = MatchId::from_str(&reported.match_id)
                        .map_err(|_| Status::invalid_argument("instance match_id"))?;
                    if instances
                        .insert(
                            match_id,
                            Instance {
                                match_id,
                                cost: reported.reserved_cost,
                                state: parse_instance(&reported.state)?,
                                endpoint: (!reported.endpoint.is_empty())
                                    .then_some(reported.endpoint),
                                connection_token: (!reported.connection_token.is_empty())
                                    .then_some(reported.connection_token),
                            },
                        )
                        .is_some()
                    {
                        return Err(Status::invalid_argument("duplicate instance match_id"));
                    }
                }
                let capacity_used = instances
                    .values()
                    .filter(|instance| {
                        !matches!(
                            instance.state,
                            InstanceState::Finished | InstanceState::ServerLost
                        )
                    })
                    .try_fold(0_u32, |used, instance| used.checked_add(instance.cost))
                    .ok_or_else(|| Status::invalid_argument("instance capacity overflow"))?;
                let server = GameServer {
                    id,
                    generation: ServerGeneration(req.generation),
                    endpoint: req.endpoint,
                    region: req.region,
                    modes: costs.keys().copied().collect(),
                    capacity_total: req.capacity_total,
                    capacity_used,
                    max_instances: u16::try_from(req.max_instances)
                        .map_err(|_| Status::invalid_argument("max_instances"))?,
                    mode_costs: costs,
                    last_heartbeat: now_ms(),
                    health: Health::Healthy,
                    failures: 0,
                    instances,
                };
                s.registry
                    .register(
                        server,
                        req.server_class
                            .is_empty()
                            .then_some(ServerLimits {
                                max_capacity: u32::MAX,
                                max_instances: 100,
                            })
                            .or_else(|| {
                                s.config
                                    .server_classes
                                    .get(&req.server_class)
                                    .map(|policy| ServerLimits {
                                        max_capacity: policy.capacity_limit,
                                        max_instances: policy.max_instances,
                                    })
                            })
                            .ok_or_else(|| Status::permission_denied("unknown server class"))?,
                    )
                    .map_err(|e| Status::failed_precondition(e.to_string()))?;
                // Registration starts a new control-session handshake. Never leave the previous
                // generation's sender eligible to receive launches during this gap.
                s.controls.remove(&id);
                Ok(pb::RegisterServerResponse {
                    accepted: true,
                    code: "OK".into(),
                })
            })
            .await?;
        Ok(Response::new(value))
    }
    type ControlStreamStream = RpcStream<pb::ErpsControl>;
    async fn control_stream(
        &self,
        request: Request<tonic::Streaming<pb::ServerControl>>,
    ) -> Result<Response<Self::ControlStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let core = self.core.clone();
        let validator = self.validator.clone();
        let (tx, rx) = mpsc::channel(32);
        let control_tx = tx.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                let reply_stream = control_tx.clone();
                let message_validator = validator.clone();
                let result = core
                    .call(move |s| {
                        check_api(msg.api.as_ref())?;
                        let id = ServerId::from_str(&msg.server_id)
                            .map_err(|_| Status::invalid_argument("server_id"))?;
                        message_validator.validate(&msg.auth_token, id)?;
                        s.apply_server_control(
                            id,
                            ServerGeneration(msg.generation),
                            reply_stream,
                            msg.message,
                        )
                    })
                    .await;
                if let Err(e) = result {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn reconcile_instances(
        &self,
        request: Request<pb::ReconcileRequest>,
    ) -> Result<Response<pb::RegisterServerResponse>, Status> {
        let req = request.into_inner();
        check_api(req.api.as_ref())?;
        let id = ServerId::from_str(&req.server_id)
            .map_err(|_| Status::invalid_argument("server_id"))?;
        self.validator.validate(&req.auth_token, id)?;
        let value = self
            .core
            .call(move |s| {
                let id = id;
                let mut instances = BTreeMap::new();
                let mut recoverable = Vec::new();
                for i in req.instances {
                    let mid = crate::id::MatchId::from_str(&i.match_id)
                        .map_err(|_| Status::invalid_argument("match_id"))?;
                    let state = parse_instance(&i.state)?;
                    if matches!(state, InstanceState::Ready | InstanceState::Running)
                        && !i.endpoint.is_empty()
                        && !i.connection_token.is_empty()
                    {
                        recoverable.push(pb::LaunchResult {
                            match_id: i.match_id.clone(),
                            state: "ready".into(),
                            endpoint: i.endpoint.clone(),
                            connection_token: i.connection_token.clone(),
                            reason: String::new(),
                        });
                    }
                    if instances
                        .insert(
                            mid,
                            Instance {
                                match_id: mid,
                                cost: i.reserved_cost,
                                state,
                                endpoint: (!i.endpoint.is_empty()).then_some(i.endpoint),
                                connection_token: (!i.connection_token.is_empty())
                                    .then_some(i.connection_token),
                            },
                        )
                        .is_some()
                    {
                        return Err(Status::invalid_argument("duplicate instance match_id"));
                    }
                }
                s.registry
                    .reconcile(id, ServerGeneration(req.generation), instances)
                    .map_err(|e| Status::failed_precondition(e.to_string()))?;
                for ready in recoverable {
                    let match_id = MatchId::from_str(&ready.match_id)
                        .map_err(|_| Status::invalid_argument("match_id"))?;
                    let already_published = s
                        .player_match
                        .values()
                        .any(|published| *published == match_id);
                    if s.launches.contains_key(&match_id) && !already_published {
                        s.launch_result(id, ready)?;
                    }
                }
                Ok(pb::RegisterServerResponse {
                    accepted: true,
                    code: "OK".into(),
                })
            })
            .await?;
        Ok(Response::new(value))
    }
}
fn parse_instance(v: &str) -> Result<InstanceState, Status> {
    match v.to_ascii_lowercase().as_str() {
        "reserved" => Ok(InstanceState::Reserved),
        "accepted" => Ok(InstanceState::Accepted),
        "ready" => Ok(InstanceState::Ready),
        "running" => Ok(InstanceState::Running),
        "finished" => Ok(InstanceState::Finished),
        "serverlost" | "server_lost" => Ok(InstanceState::ServerLost),
        _ => Err(Status::invalid_argument("instance state")),
    }
}

fn valid_reported_instance_transition(previous: InstanceState, next: InstanceState) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (InstanceState::Reserved, InstanceState::Accepted)
                | (InstanceState::Accepted, InstanceState::Ready)
                | (InstanceState::Ready, InstanceState::Running)
        )
}

fn latency_summary(stage: &str, histogram: &crate::metrics::Histogram) -> pb::LatencySummary {
    let summary = histogram.summary();
    pb::LatencySummary {
        stage: stage.to_owned(),
        samples: summary.samples,
        p50_us: summary.p50,
        p95_us: summary.p95,
        p99_us: summary.p99,
    }
}

fn admin_metrics(metrics: &crate::metrics::Metrics) -> pb::MetricSummary {
    let lifecycle = metrics.lifecycle.read();
    pb::MetricSummary {
        commands: metrics.commands.get(),
        proposals: metrics.matches.get(),
        ready_accepts: metrics.ready_accepts.get(),
        ready_rejects: metrics.ready_rejects.get(),
        ready_timeouts: metrics.ready_timeouts.get(),
        credit_penalties: metrics.credit_penalties.get(),
        launch_failures: metrics.launch_failures.get(),
        reconnects: metrics.reconnects.get(),
        invariant_failures: metrics.invariant_failures.get(),
        command_queue_high: metrics.command_queue_high.get(),
        event_queue_high: metrics.event_queue_high.get(),
        control_queue_high: metrics.control_queue_high.get(),
        elo_quality_sum: lifecycle.elo_quality_sum,
        party_structure_difference_sum: lifecycle.party_structure_difference_sum,
        latencies: [
            ("queue_wait", &metrics.queue_wait_us),
            ("candidate_compute", &metrics.candidate_compute_us),
            ("commit", &metrics.commit_us),
            ("ready_check", &metrics.ready_check_us),
            ("placement", &metrics.placement_us),
            ("launch", &metrics.launch_us),
        ]
        .into_iter()
        .map(|(stage, histogram)| latency_summary(stage, histogram))
        .collect(),
    }
}

#[tonic::async_trait]
impl AdminService for AdminGrpc {
    async fn snapshot(
        &self,
        request: Request<pb::AdminSnapshotRequest>,
    ) -> Result<Response<pb::AdminSnapshot>, Status> {
        check_api(request.into_inner().api.as_ref())?;
        let value = self
            .core
            .call(|s| {
                let mut grouped: BTreeMap<(QueueMode, String), (u64, u64)> = BTreeMap::new();
                for (party_id, t) in &s.tickets {
                    let players = s
                        .parties
                        .get(party_id)
                        .map_or(0, |p| p.members.len() as u64);
                    for region in &t.regions {
                        let e = grouped.entry((t.mode, region.clone())).or_default();
                        e.0 += 1;
                        e.1 += players;
                    }
                }
                let queues = grouped
                    .into_iter()
                    .map(|((m, r), (tickets, players))| pb::QueueSummary {
                        mode: pb_mode(m),
                        region: r,
                        tickets,
                        players,
                    })
                    .collect();
                let servers = s
                    .registry
                    .servers
                    .values()
                    .map(|v| pb::ServerSummary {
                        server_id: v.id.to_string(),
                        health: format!("{:?}", v.health),
                        capacity_total: v.capacity_total,
                        capacity_used: v.capacity_used,
                        max_instances: v.max_instances as u32,
                        running_instances: v
                            .instances
                            .values()
                            .filter(|i| i.state == InstanceState::Running)
                            .count() as u32,
                    })
                    .collect();
                Ok(pb::AdminSnapshot {
                    queues,
                    servers: Some(pb::ServerList { servers }),
                    matches: s.launches.len() as u64,
                    reservations: s
                        .registry
                        .servers
                        .values()
                        .flat_map(|server| server.instances.values())
                        .filter(|instance| {
                            matches!(
                                instance.state,
                                InstanceState::Reserved | InstanceState::Accepted
                            )
                        })
                        .count() as u64,
                    metrics: Some(admin_metrics(&s.metrics)),
                })
            })
            .await?;
        Ok(Response::new(value))
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    config: ErpsConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    if config.tls_certificate_path.is_some() {
        anyhow::bail!(
            "production TLS requires serve_with_validator and a trusted identity validator"
        );
    }
    serve_with_validator(addr, config, Arc::new(DevelopmentTokenValidator), shutdown).await
}

pub async fn serve_with_validator(
    addr: std::net::SocketAddr,
    config: ErpsConfig,
    validator: Arc<dyn TokenValidator>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    if config.tls_certificate_path.is_some() {
        anyhow::bail!(
            "production TLS requires serve_with_validators and trusted client/server validators"
        );
    }
    serve_with_validators(
        addr,
        config,
        validator,
        Arc::new(DevelopmentTokenValidator),
        shutdown,
    )
    .await
}

pub async fn serve_with_validators(
    addr: std::net::SocketAddr,
    config: ErpsConfig,
    validator: Arc<dyn TokenValidator>,
    server_validator: Arc<dyn ServerTokenValidator>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    config.validate()?;
    let tls_pair = config
        .tls_certificate_path
        .clone()
        .zip(config.tls_private_key_path.clone());
    if tls_pair.is_none() && !(addr.ip().is_loopback() && config.allow_development_plaintext) {
        anyhow::bail!(
            "plaintext ERPS is only allowed on loopback with allow_development_plaintext=true"
        );
    }
    let drain_mode = config.drain_mode;
    let grace = std::time::Duration::from_secs(config.graceful_shutdown_seconds);
    let core = CoreHandle::spawn(config);
    let matchmaking = pb::matchmaking_service_server::MatchmakingServiceServer::new(
        MatchmakingGrpc::with_validator(core.clone(), validator),
    );
    let games = pb::game_server_service_server::GameServerServiceServer::new(
        GameServerGrpc::with_validator(core.clone(), server_validator),
    );
    let drain_core = core.clone();
    let admin = pb::admin_service_server::AdminServiceServer::new(AdminGrpc::new(core));
    let (mut reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_serving::<pb::matchmaking_service_server::MatchmakingServiceServer<MatchmakingGrpc>>()
        .await;
    reporter
        .set_serving::<pb::game_server_service_server::GameServerServiceServer<GameServerGrpc>>()
        .await;
    reporter
        .set_serving::<pb::admin_service_server::AdminServiceServer<AdminGrpc>>()
        .await;
    if drain_mode {
        reporter.set_not_serving::<pb::matchmaking_service_server::MatchmakingServiceServer<MatchmakingGrpc>>().await;
    }
    let mut builder = tonic::transport::Server::builder();
    if let Some((certificate, key)) = tls_pair {
        let identity = tonic::transport::Identity::from_pem(
            tokio::fs::read(certificate).await?,
            tokio::fs::read(key).await?,
        );
        builder =
            builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
    }
    let (drain_tx, drain_rx) = oneshot::channel();
    let wrapped_shutdown = async move {
        shutdown.await;
        drain_core.begin_drain().await;
        reporter.set_not_serving::<pb::matchmaking_service_server::MatchmakingServiceServer<MatchmakingGrpc>>().await;
        let _ = drain_tx.send(());
    };
    let server = builder
        .add_service(health)
        .add_service(matchmaking)
        .add_service(games)
        .add_service(admin)
        .serve_with_shutdown(addr, wrapped_shutdown);
    tokio::select! {result=server=>result?,_=async{let _=drain_rx.await;tokio::time::sleep(grace).await;}=>anyhow::bail!("bounded graceful shutdown deadline exceeded")}
    Ok(())
}

#[cfg(test)]
mod paced_rating_test;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn bounded_actor_runs_commands() {
        let core = CoreHandle::spawn(ErpsConfig::default());
        assert_eq!(core.call(|s| Ok(s.parties.len())).await.unwrap(), 0)
    }
    #[tokio::test]
    async fn admin_snapshot_rejects_incompatible_api_major() {
        let admin = AdminGrpc::new(CoreHandle::spawn(ErpsConfig::default()));
        let error = admin
            .snapshot(Request::new(pb::AdminSnapshotRequest {
                api: Some(pb::ApiVersion {
                    major: 2,
                    minor: 0,
                    capabilities: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
    #[tokio::test]
    async fn successful_server_reregistration_clears_previous_control_sender() {
        let core = CoreHandle::spawn(ErpsConfig::default());
        let server_id = ServerId::new();
        let (sender, _receiver) = mpsc::channel(1);
        core.call(move |state| {
            state
                .registry
                .register(
                    GameServer {
                        id: server_id,
                        generation: ServerGeneration(1),
                        endpoint: "game".into(),
                        region: "tw".into(),
                        modes: BTreeSet::from([QueueMode::OneVsOne]),
                        capacity_total: 10,
                        capacity_used: 0,
                        max_instances: 2,
                        mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                        last_heartbeat: 1,
                        health: Health::Healthy,
                        failures: 0,
                        instances: BTreeMap::new(),
                    },
                    ServerLimits {
                        max_capacity: u32::MAX,
                        max_instances: 100,
                    },
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            state.controls.insert(server_id, sender);
            Ok(())
        })
        .await
        .unwrap();
        let service = GameServerGrpc::new(core.clone());
        service
            .register(Request::new(pb::RegisterServerRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: server_id.to_string(),
                server_id: server_id.to_string(),
                generation: 1,
                endpoint: "game".into(),
                region: "tw".into(),
                capacity_total: 10,
                max_instances: 2,
                mode_costs: vec![pb::ModeCost {
                    mode: pb::QueueMode::OneVOne as i32,
                    cost: 1,
                }],
                instances: vec![],
                server_class: String::new(),
            }))
            .await
            .unwrap();
        assert!(!core
            .call(move |state| Ok(state.controls.contains_key(&server_id)))
            .await
            .unwrap());
    }
    #[test]
    fn admin_metrics_exposes_counters_high_watermarks_and_latency() {
        let metrics = crate::metrics::Metrics::default();
        metrics.commands.add(3);
        metrics.ready_accepts.add(2);
        metrics.command_queue_high.observe(7);
        metrics.queue_wait_us.observe(11);
        metrics.queue_wait_us.observe(29);
        metrics.lifecycle.write().elo_quality_sum = 41;
        let snapshot = admin_metrics(&metrics);
        assert_eq!(snapshot.commands, 3);
        assert_eq!(snapshot.ready_accepts, 2);
        assert_eq!(snapshot.command_queue_high, 7);
        assert_eq!(snapshot.elo_quality_sum, 41);
        let queue_wait = snapshot
            .latencies
            .iter()
            .find(|latency| latency.stage == "queue_wait")
            .unwrap();
        assert_eq!(queue_wait.samples, 2);
        assert_eq!(queue_wait.p99_us, 29);
    }
    #[test]
    fn mode_round_trip() {
        for m in [
            QueueMode::OneVsOne,
            QueueMode::FiveVsFive,
            QueueMode::FreeForAll,
        ] {
            assert_eq!(mode(pb_mode(m)).unwrap(), m)
        }
    }
    #[test]
    fn api_major_is_rejected_and_minor_is_forward_compatible() {
        assert!(check_api(Some(&pb::ApiVersion {
            major: 2,
            minor: 0,
            capabilities: vec![]
        }))
        .is_err());
        assert!(check_api(Some(&pb::ApiVersion {
            major: 1,
            minor: 999,
            capabilities: vec!["future".into()]
        }))
        .is_ok());
    }
    #[test]
    fn credit_suspension_expires_at_a_defined_deadline() {
        assert!(credit_suspension_active(59, 60, Some(1_000), 999));
        assert!(credit_suspension_active(59, 60, Some(1_000), 1_000));
        assert!(!credit_suspension_active(59, 60, Some(1_000), 1_001));
        assert!(!credit_suspension_active(60, 60, Some(1_000), 999));
        assert!(!credit_suspension_active(59, 60, None, 999));
    }
    struct FixedValidator(PlayerId);
    impl TokenValidator for FixedValidator {
        fn validate(&self, token: &str) -> Result<PlayerId, Status> {
            if token == "signed" {
                Ok(self.0)
            } else {
                Err(Status::unauthenticated("bad signature"))
            }
        }
    }
    #[test]
    fn injectable_validator_controls_identity() {
        let id = PlayerId::new();
        let v = FixedValidator(id);
        assert_eq!(v.validate("signed").unwrap(), id);
        assert!(v.validate(&id.to_string()).is_err());
    }
    #[test]
    fn game_server_validator_binds_token_to_server_identity() {
        let server = ServerId::new();
        let validator = StaticServerTokenValidator {
            identities: BTreeMap::from([("signed-server".into(), server)]),
        };
        assert!(validator.validate("signed-server", server).is_ok());
        assert!(validator
            .validate("signed-server", ServerId::new())
            .is_err());
        assert!(validator.validate("server-id-as-token", server).is_err());
    }
    #[test]
    fn stale_control_generation_cannot_replace_sender_or_mutate_instance() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let server_id = ServerId::new();
        let match_id = MatchId::new();
        state
            .registry
            .register(
                GameServer {
                    id: server_id,
                    generation: ServerGeneration(2),
                    endpoint: "game".into(),
                    region: "tw".into(),
                    modes: BTreeSet::from([QueueMode::OneVsOne]),
                    capacity_total: 10,
                    capacity_used: 1,
                    max_instances: 2,
                    mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                    last_heartbeat: 10,
                    health: Health::Healthy,
                    failures: 0,
                    instances: BTreeMap::from([(
                        match_id,
                        Instance {
                            match_id,
                            cost: 1,
                            state: InstanceState::Running,
                            endpoint: Some("game".into()),
                            connection_token: Some("secret".into()),
                        },
                    )]),
                },
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 2,
                },
            )
            .unwrap();
        let (sender, _receiver) = mpsc::channel(1);

        state
            .apply_server_control(
                server_id,
                ServerGeneration(1),
                sender,
                Some(pb::server_control::Message::Instance(pb::InstanceState {
                    match_id: match_id.to_string(),
                    state: "finished".into(),
                    reserved_cost: 1,
                    endpoint: String::new(),
                    connection_token: String::new(),
                })),
            )
            .unwrap();

        assert!(!state.controls.contains_key(&server_id));
        assert_eq!(state.registry.servers[&server_id].capacity_used, 1);
        assert_eq!(
            state.registry.servers[&server_id].instances[&match_id].state,
            InstanceState::Running
        );

        let (sender, _receiver) = mpsc::channel(1);
        let cost_error = state
            .apply_server_control(
                server_id,
                ServerGeneration(2),
                sender,
                Some(pb::server_control::Message::Instance(pb::InstanceState {
                    match_id: match_id.to_string(),
                    state: "running".into(),
                    reserved_cost: 2,
                    endpoint: "game".into(),
                    connection_token: "secret".into(),
                })),
            )
            .unwrap_err();
        assert_eq!(cost_error.code(), tonic::Code::FailedPrecondition);

        let (sender, _receiver) = mpsc::channel(1);
        let unknown_error = state
            .apply_server_control(
                server_id,
                ServerGeneration(2),
                sender,
                Some(pb::server_control::Message::Instance(pb::InstanceState {
                    match_id: MatchId::new().to_string(),
                    state: "running".into(),
                    reserved_cost: 1,
                    endpoint: "game".into(),
                    connection_token: "secret".into(),
                })),
            )
            .unwrap_err();
        assert_eq!(unknown_error.code(), tonic::Code::NotFound);
    }
    #[test]
    fn completed_match_result_idempotency_cache_is_bounded() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let first = MatchId::new();
        state.remember_completed_match_result(first);
        for _ in 0..4096 {
            state.remember_completed_match_result(MatchId::new());
        }
        assert_eq!(state.completed_match_results.len(), 4096);
        assert_eq!(state.completed_match_result_order.len(), 4096);
        assert!(!state.completed_match_results.contains(&first));
    }
    #[tokio::test]
    async fn production_plaintext_is_rejected_and_graceful_shutdown_completes() {
        let result = serve("0.0.0.0:0".parse().unwrap(), ErpsConfig::default(), async {
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("plaintext ERPS"));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let config = ErpsConfig {
            allow_development_plaintext: true,
            ..Default::default()
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            serve(addr, config, async {}),
        )
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn production_tls_handshake_and_token_identity_are_enforced() {
        use erps_proto::v1::game_server_service_client::GameServerServiceClient;
        use erps_proto::v1::matchmaking_service_client::MatchmakingServiceClient;
        use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate_pem = certified.cert.pem();
        let key_pem = certified.key_pair.serialize_pem();
        let directory = tempfile::tempdir().unwrap();
        let certificate_path = directory.path().join("server.pem");
        let key_path = directory.path().join("server-key.pem");
        std::fs::write(&certificate_path, &certificate_pem).unwrap();
        std::fs::write(&key_path, key_pem).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let player = PlayerId::new();
        let server = ServerId::new();
        let config = ErpsConfig {
            tls_certificate_path: Some(certificate_path.to_string_lossy().into_owned()),
            tls_private_key_path: Some(key_path.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let (stop_tx, stop_rx) = oneshot::channel();
        let running = tokio::spawn(serve_with_validators(
            addr,
            config,
            Arc::new(StaticTokenValidator {
                identities: BTreeMap::from([("signed-player".into(), player)]),
            }),
            Arc::new(StaticServerTokenValidator {
                identities: BTreeMap::from([("signed-server".into(), server)]),
            }),
            async move {
                let _ = stop_rx.await;
            },
        ));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let channel = Endpoint::from_shared(format!("https://localhost:{}", addr.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(&certificate_pem)),
            )
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut game = GameServerServiceClient::new(channel.clone());
        let untrusted = game
            .register(pb::RegisterServerRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: "signed-server".into(),
                server_id: server.to_string(),
                generation: 1,
                endpoint: "game".into(),
                region: "tw".into(),
                capacity_total: u32::MAX,
                max_instances: 100,
                mode_costs: vec![pb::ModeCost {
                    mode: pb::QueueMode::OneVOne as i32,
                    cost: 1,
                }],
                instances: vec![],
                server_class: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(untrusted.code(), tonic::Code::PermissionDenied);
        let mut client = MatchmakingServiceClient::new(channel);
        let bad = client
            .open_session(pb::ConnectRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: "unsigned".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(bad.code(), tonic::Code::Unauthenticated);
        let opened = client
            .open_session(pb::ConnectRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: "signed-player".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(opened.player_id, player.to_string());
        let sdk = erps_client::Client::connect(erps_client::ConnectOptions::tls_with_ca(
            format!("https://localhost:{}", addr.port()),
            "signed-player",
            "localhost",
            certificate_pem.as_bytes(),
        ))
        .await
        .unwrap();
        let expected_player = player.to_string();
        assert_eq!(sdk.player_id(), Some(expected_player.as_str()));
        let _ = stop_tx.send(());
        running.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_health_reports_every_public_service() {
        use tonic::server::NamedService;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (stop_tx, stop_rx) = oneshot::channel();
        let running = tokio::spawn(serve(
            addr,
            ErpsConfig {
                allow_development_plaintext: true,
                graceful_shutdown_seconds: 1,
                ..Default::default()
            },
            async move {
                let _ = stop_rx.await;
            },
        ));
        let endpoint = format!("http://{addr}");
        let mut health = loop {
            match tonic::transport::Endpoint::from_shared(endpoint.clone())
                .unwrap()
                .connect()
                .await
            {
                Ok(channel) => {
                    break tonic_health::pb::health_client::HealthClient::new(channel);
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        };
        for service in [
            <pb::matchmaking_service_server::MatchmakingServiceServer<MatchmakingGrpc> as NamedService>::NAME,
            <pb::game_server_service_server::GameServerServiceServer<GameServerGrpc> as NamedService>::NAME,
            <pb::admin_service_server::AdminServiceServer<AdminGrpc> as NamedService>::NAME,
        ] {
            let response = health
                .check(tonic_health::pb::HealthCheckRequest {
                    service: service.into(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.status,
                tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
                "{service} was not reported serving"
            );
        }
        let _ = stop_tx.send(());
        running.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn slow_consumer_is_terminated_and_get_state_recovers_latest_state() {
        use tokio_stream::StreamExt;

        let core = CoreHandle::spawn(ErpsConfig {
            event_queue_capacity: 2,
            ..Default::default()
        });
        let service = MatchmakingGrpc::new(core);
        let opened = service
            .open_session(Request::new(pb::ConnectRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: "slow-consumer".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let meta = |request_id: &str| pb::MutationMeta {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            request_id: request_id.into(),
            session_token: opened.session_token.clone(),
        };
        let created = service
            .create_party(Request::new(pb::CreatePartyRequest {
                meta: Some(meta("create")),
                name: "Slow1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let mut stream = service
            .watch_events(Request::new(pb::WatchEventsRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                session_token: opened.session_token.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        let mut revision = created.revision;
        for index in 0..6 {
            revision = service
                .rename_party(Request::new(pb::RenamePartyRequest {
                    meta: Some(meta(&format!("rename-{index}"))),
                    party_id: created.entity_id.clone(),
                    revision,
                    name: format!("Slow{index}"),
                }))
                .await
                .unwrap()
                .into_inner()
                .revision;
        }
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        let state = service
            .get_state(Request::new(pb::StateRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                session_token: opened.session_token,
            }))
            .await
            .unwrap()
            .into_inner();
        let party = state.party.unwrap();
        assert_eq!(party.name, "Slow5");
        assert_eq!(party.revision, revision);
    }
    #[tokio::test]
    async fn repeated_enqueue_request_id_returns_same_ticket_without_duplicate() {
        let core = CoreHandle::spawn(ErpsConfig::default());
        let service = MatchmakingGrpc::new(core.clone());
        let opened = service
            .open_session(Request::new(pb::ConnectRequest {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                auth_token: "idempotent-player".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let meta = |request_id: &str| pb::MutationMeta {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            request_id: request_id.into(),
            session_token: opened.session_token.clone(),
        };
        let party = service
            .create_party(Request::new(pb::CreatePartyRequest {
                meta: Some(meta("party")),
                name: "Idempotent1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let request = pb::EnqueueRequest {
            meta: Some(meta("same-enqueue")),
            party_id: party.entity_id,
            revision: party.revision,
            mode: pb::QueueMode::OneVOne as i32,
            allowed_regions: vec!["tw".into()],
        };
        let first = service
            .enqueue(Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
        let second = service
            .enqueue(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.entity_id, second.entity_id);
        assert_eq!(first.revision, second.revision);
        assert_eq!(core.call(|state| Ok(state.tickets.len())).await.unwrap(), 1);
    }
    #[tokio::test]
    async fn identical_rpc_sequences_produce_identical_domain_ids() {
        async fn execute() -> (String, String) {
            let service = MatchmakingGrpc::new(CoreHandle::spawn(ErpsConfig {
                deterministic_seed: 77,
                ..Default::default()
            }));
            let opened = service
                .open_session(Request::new(pb::ConnectRequest {
                    api: Some(pb::ApiVersion {
                        major: 1,
                        minor: 0,
                        capabilities: vec![],
                    }),
                    auth_token: "deterministic-player".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            let meta = |request_id: &str| pb::MutationMeta {
                api: Some(pb::ApiVersion {
                    major: 1,
                    minor: 0,
                    capabilities: vec![],
                }),
                request_id: request_id.into(),
                session_token: opened.session_token.clone(),
            };
            let party = service
                .create_party(Request::new(pb::CreatePartyRequest {
                    meta: Some(meta("party")),
                    name: "Deterministic1".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            let ticket = service
                .enqueue(Request::new(pb::EnqueueRequest {
                    meta: Some(meta("enqueue")),
                    party_id: party.entity_id.clone(),
                    revision: party.revision,
                    mode: pb::QueueMode::OneVOne as i32,
                    allowed_regions: vec!["tw".into()],
                }))
                .await
                .unwrap()
                .into_inner();
            (party.entity_id, ticket.entity_id)
        }
        assert_eq!(execute().await, execute().await);
    }
    #[test]
    fn drain_mode_rejects_mutations() {
        let state = AuthorityState::new(
            ErpsConfig {
                drain_mode: true,
                ..Default::default()
            },
            Arc::new(crate::metrics::Metrics::default()),
        );
        assert_eq!(
            state.meta(None).unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }
    #[tokio::test]
    async fn graceful_shutdown_transition_dynamically_rejects_new_mutations() {
        let core = CoreHandle::spawn(ErpsConfig::default());
        core.begin_drain().await;
        let code = core
            .call(|state| Ok(state.meta(None).unwrap_err().code()))
            .await
            .unwrap();
        assert_eq!(code, tonic::Code::Unavailable);
    }
    #[test]
    fn runtime_timeout_penalizes_only_missing_player_and_requeues_innocent_at_original_age() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let server_id = ServerId::new();
        state
            .registry
            .register(
                GameServer {
                    id: server_id,
                    generation: ServerGeneration(1),
                    endpoint: "game".into(),
                    region: "tw".into(),
                    modes: BTreeSet::from([QueueMode::OneVsOne]),
                    capacity_total: 10,
                    capacity_used: 0,
                    max_instances: 2,
                    mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                    last_heartbeat: 0,
                    health: Health::Healthy,
                    failures: 0,
                    instances: BTreeMap::new(),
                },
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 2,
                },
            )
            .unwrap();
        let players = [PlayerId::new(), PlayerId::new()];
        for (index, player) in players.into_iter().enumerate() {
            state.sessions.insert(format!("s{index}"), player);
            state.credit.insert(player, 100);
            let mut party = Party::new(player, &format!("P{index}")).unwrap();
            party.state = PartyState::Queued;
            let party_id = party.id;
            state.player_party.insert(player, party_id);
            state.tickets.insert(
                party_id,
                TicketRecord {
                    id: TicketId::new(),
                    mode: QueueMode::OneVsOne,
                    regions: vec!["tw".into()],
                    enqueued_at: 7,
                    queued_since_ms: 7,
                },
            );
            state.parties.insert(party_id, party);
        }
        let mut receivers = Vec::new();
        for player in players {
            let (sender, receiver) = broadcast::channel(8);
            state.events.insert(player, sender);
            receivers.push(receiver);
        }
        state.attempt_match_batch(QueueMode::OneVsOne, "tw");
        let proposal_id = *state.proposals.keys().next().unwrap();
        let deadline = state.proposals[&proposal_id].deadline;
        let reconnected = state.client_state(players[0]);
        assert_eq!(reconnected.proposal_id, proposal_id.to_string());
        assert_eq!(reconnected.queue_mode, pb::QueueMode::OneVOne as i32);
        assert_eq!(reconnected.allowed_regions, vec!["tw"]);
        assert_eq!(reconnected.proposal_deadline_ms, deadline as i64);
        assert!(reconnected.profile.is_some());
        state
            .proposals
            .get_mut(&proposal_id)
            .unwrap()
            .respond(proposal_id, players[0], true)
            .unwrap();
        state.tick(deadline + 1);
        assert_eq!(state.credit[&players[0]], 100);
        assert_eq!(state.credit[&players[1]], 95);
        assert_eq!(state.recent_credit_violations[&players[1]], 1);
        let innocent_party = state.player_party[&players[0]];
        assert_eq!(state.tickets[&innocent_party].enqueued_at, 7);
        assert_eq!(state.parties[&innocent_party].state, PartyState::Queued);
        assert_eq!(
            state.parties[&state.player_party[&players[1]]].state,
            PartyState::NotReady
        );
        for (index, receiver) in receivers.iter_mut().enumerate() {
            assert!(matches!(
                receiver.try_recv().unwrap().event,
                Some(pb::client_event::Event::ProposalId(_))
            ));
            let cancelled = receiver.try_recv().unwrap();
            let Some(pb::client_event::Event::ProposalCancelled(cancelled)) = cancelled.event
            else {
                panic!("player must receive an explicit proposal cancellation");
            };
            assert_eq!(cancelled.credit, if index == 0 { 100 } else { 95 });
            assert!(cancelled.eligible);
            assert_eq!(
                cancelled.reason,
                if index == 0 {
                    "other_player_failed"
                } else {
                    "timed_out"
                }
            );
            let update = receiver.try_recv().unwrap();
            let Some(pb::client_event::Event::Party(party)) = update.event else {
                panic!("player must receive a party update after ready timeout");
            };
            assert_eq!(party.members[0].credit, if index == 0 { 100 } else { 95 });
            assert_eq!(party.state, if index == 0 { "Queued" } else { "NotReady" });
        }
    }
    #[test]
    fn reported_instance_state_requires_the_ordered_lifecycle() {
        assert!(valid_reported_instance_transition(
            InstanceState::Reserved,
            InstanceState::Accepted
        ));
        assert!(valid_reported_instance_transition(
            InstanceState::Accepted,
            InstanceState::Ready
        ));
        assert!(valid_reported_instance_transition(
            InstanceState::Ready,
            InstanceState::Running
        ));
        assert!(!valid_reported_instance_transition(
            InstanceState::Running,
            InstanceState::Finished
        ));
        assert!(valid_reported_instance_transition(
            InstanceState::Running,
            InstanceState::Running
        ));
        assert!(!valid_reported_instance_transition(
            InstanceState::Reserved,
            InstanceState::Ready
        ));
        assert!(!valid_reported_instance_transition(
            InstanceState::Finished,
            InstanceState::Running
        ));
        assert!(!valid_reported_instance_transition(
            InstanceState::Running,
            InstanceState::ServerLost
        ));
    }

    #[test]
    fn authority_domain_id_allocator_is_seeded_and_repeatable() {
        let config = ErpsConfig {
            deterministic_seed: 42,
            ..Default::default()
        };
        let mut first =
            AuthorityState::new(config.clone(), Arc::new(crate::metrics::Metrics::default()));
        let mut second = AuthorityState::new(config, Arc::new(crate::metrics::Metrics::default()));
        assert_eq!(first.next_uuid(1), second.next_uuid(1));
        assert_eq!(first.next_uuid(2), second.next_uuid(2));
        assert_ne!(first.next_uuid(3), first.next_uuid(4));
    }
    #[test]
    fn authoritative_match_result_updates_elo_and_releases_reservation() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let players = [PlayerId::new(), PlayerId::new()];
        let candidate = crate::matching::Candidate {
            tickets: vec![TicketId::new(), TicketId::new()],
            teams: vec![vec![players[0]], vec![players[1]]],
            oldest_enqueued_at: 0,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut owners = BTreeMap::new();
        let mut player_events = Vec::new();
        for player in players {
            let mut party = Party::new(player, "ResultParty").unwrap();
            party.state = PartyState::Matched;
            owners.insert(player, party.id);
            state.player_party.insert(player, party.id);
            state.parties.insert(party.id, party);
            let (sender, receiver) = broadcast::channel(8);
            state.events.insert(player, sender);
            player_events.push(receiver);
        }
        let mut proposal = Proposal::from_candidate(&candidate, owners, 0, 15);
        proposal.state = ProposalState::AwaitingPlacement;
        let proposal_id = proposal.id;
        state.proposals.insert(proposal_id, proposal);
        let server_id = ServerId::new();
        state
            .registry
            .register(
                GameServer {
                    id: server_id,
                    generation: ServerGeneration(1),
                    endpoint: "game".into(),
                    region: "tw".into(),
                    modes: BTreeSet::from([QueueMode::OneVsOne]),
                    capacity_total: 10,
                    capacity_used: 0,
                    max_instances: 2,
                    mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                    last_heartbeat: 0,
                    health: Health::Healthy,
                    failures: 0,
                    instances: BTreeMap::new(),
                },
                ServerLimits {
                    max_capacity: 10,
                    max_instances: 2,
                },
            )
            .unwrap();
        let match_id = MatchId::new();
        crate::placement::reserve(&mut state.registry, match_id, QueueMode::OneVsOne, "tw")
            .unwrap();
        state.launches.insert(match_id, (server_id, proposal_id));
        let foreign_launch = state
            .launch_result(
                ServerId::new(),
                pb::LaunchResult {
                    match_id: match_id.to_string(),
                    state: "ready".into(),
                    endpoint: "foreign-game".into(),
                    connection_token: "foreign-token".into(),
                    reason: String::new(),
                },
            )
            .unwrap_err();
        assert_eq!(foreign_launch.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            state.registry.servers[&server_id].instances[&match_id].state,
            InstanceState::Reserved
        );
        let premature_ready = state
            .launch_result(
                server_id,
                pb::LaunchResult {
                    match_id: match_id.to_string(),
                    state: "ready".into(),
                    endpoint: "game".into(),
                    connection_token: "token".into(),
                    reason: String::new(),
                },
            )
            .unwrap_err();
        assert_eq!(premature_ready.code(), tonic::Code::FailedPrecondition);
        state
            .launch_result(
                server_id,
                pb::LaunchResult {
                    match_id: match_id.to_string(),
                    state: "accepted".into(),
                    endpoint: String::new(),
                    connection_token: String::new(),
                    reason: String::new(),
                },
            )
            .unwrap();
        let premature_result = state
            .finish_match(
                server_id,
                pb::MatchResult {
                    match_id: match_id.to_string(),
                    placements: vec![
                        pb::PlayerPlacement {
                            player_id: players[0].to_string(),
                            rank: 1,
                        },
                        pb::PlayerPlacement {
                            player_id: players[1].to_string(),
                            rank: 2,
                        },
                    ],
                },
            )
            .unwrap_err();
        assert_eq!(premature_result.code(), tonic::Code::FailedPrecondition);
        assert!(state.ratings.is_empty());
        assert_eq!(state.registry.servers[&server_id].capacity_used, 1);
        let incomplete_ready = state
            .launch_result(
                server_id,
                pb::LaunchResult {
                    match_id: match_id.to_string(),
                    state: "ready".into(),
                    endpoint: "game".into(),
                    connection_token: String::new(),
                    reason: String::new(),
                },
            )
            .unwrap_err();
        assert_eq!(incomplete_ready.code(), tonic::Code::InvalidArgument);
        let ready = pb::LaunchResult {
            match_id: match_id.to_string(),
            state: "ready".into(),
            endpoint: "game".into(),
            connection_token: "token".into(),
            reason: String::new(),
        };
        state.launch_result(server_id, ready.clone()).unwrap();
        assert_eq!(
            state.registry.servers[&server_id].instances[&match_id].state,
            InstanceState::Running
        );
        state.launch_result(server_id, ready).unwrap();
        assert_eq!(state.player_match.len(), players.len());
        let duplicate_result = state
            .finish_match(
                server_id,
                pb::MatchResult {
                    match_id: match_id.to_string(),
                    placements: vec![
                        pb::PlayerPlacement {
                            player_id: players[0].to_string(),
                            rank: 1,
                        },
                        pb::PlayerPlacement {
                            player_id: players[0].to_string(),
                            rank: 1,
                        },
                        pb::PlayerPlacement {
                            player_id: players[1].to_string(),
                            rank: 2,
                        },
                    ],
                },
            )
            .unwrap_err();
        assert_eq!(duplicate_result.code(), tonic::Code::InvalidArgument);
        assert_eq!(state.registry.servers[&server_id].capacity_used, 1);
        state
            .finish_match(
                server_id,
                pb::MatchResult {
                    match_id: match_id.to_string(),
                    placements: vec![
                        pb::PlayerPlacement {
                            player_id: players[0].to_string(),
                            rank: 1,
                        },
                        pb::PlayerPlacement {
                            player_id: players[1].to_string(),
                            rank: 2,
                        },
                    ],
                },
            )
            .unwrap();
        assert!(state.ratings[&(players[0], QueueMode::OneVsOne)] > 1000);
        assert!(state.ratings[&(players[1], QueueMode::OneVsOne)] < 1000);
        assert_eq!(state.registry.servers[&server_id].capacity_used, 0);
        for (index, receiver) in player_events.iter_mut().enumerate() {
            assert!(matches!(
                receiver.try_recv().unwrap().event,
                Some(pb::client_event::Event::Matched(_))
            ));
            let update = receiver.try_recv().unwrap();
            let Some(pb::client_event::Event::Party(party)) = update.event else {
                panic!("match completion must publish updated Elo and credit");
            };
            assert_eq!(party.state, "Idle");
            let own = &party.members[0];
            assert_eq!(own.player_id, players[index].to_string());
            assert_eq!(
                own.rating_one_v_one,
                state.ratings[&(players[index], QueueMode::OneVsOne)]
            );
        }
    }
    #[test]
    fn launch_uses_common_ticket_region_and_never_cross_region_server() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let players = [PlayerId::new(), PlayerId::new()];
        let tickets = [TicketId::new(), TicketId::new()];
        let candidate = crate::matching::Candidate {
            tickets: tickets.to_vec(),
            teams: vec![vec![players[0]], vec![players[1]]],
            oldest_enqueued_at: 0,
            quality_key: (0, 0, 0),
            owner_shard: 0,
        };
        let mut proposal = Proposal::from_candidate(&candidate, BTreeMap::new(), 0, 15);
        proposal.state = ProposalState::AwaitingPlacement;
        let proposal_id = proposal.id;
        state.proposals.insert(proposal_id, proposal);
        state.proposal_tickets.insert(
            proposal_id,
            vec![
                (
                    PartyId::new(),
                    TicketRecord {
                        id: tickets[0],
                        mode: QueueMode::OneVsOne,
                        regions: vec!["tw".into(), "us".into()],
                        enqueued_at: 0,
                        queued_since_ms: 0,
                    },
                ),
                (
                    PartyId::new(),
                    TicketRecord {
                        id: tickets[1],
                        mode: QueueMode::OneVsOne,
                        regions: vec!["tw".into()],
                        enqueued_at: 0,
                        queued_since_ms: 0,
                    },
                ),
            ],
        );
        let mut ids = BTreeMap::new();
        let mut control_receivers = Vec::new();
        for region in ["us", "tw"] {
            let id = ServerId::new();
            ids.insert(region, id);
            state
                .registry
                .register(
                    GameServer {
                        id,
                        generation: ServerGeneration(1),
                        endpoint: region.into(),
                        region: region.into(),
                        modes: BTreeSet::from([QueueMode::OneVsOne]),
                        capacity_total: 10,
                        capacity_used: 0,
                        max_instances: 2,
                        mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                        last_heartbeat: 0,
                        health: Health::Healthy,
                        failures: 0,
                        instances: BTreeMap::new(),
                    },
                    ServerLimits {
                        max_capacity: 10,
                        max_instances: 2,
                    },
                )
                .unwrap();
            let (tx, rx) = mpsc::channel(2);
            state.controls.insert(id, tx);
            control_receivers.push(rx);
        }
        let match_id = state.launch(proposal_id).unwrap();
        assert!(state.registry.servers[&ids["tw"]]
            .instances
            .contains_key(&match_id));
        assert!(!state.registry.servers[&ids["us"]]
            .instances
            .contains_key(&match_id));
    }
    #[test]
    fn one_ecs_snapshot_commits_many_disjoint_proposals() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let server_id = ServerId::new();
        state
            .registry
            .register(
                GameServer {
                    id: server_id,
                    generation: ServerGeneration(1),
                    endpoint: "game".into(),
                    region: "tw".into(),
                    modes: BTreeSet::from([QueueMode::OneVsOne]),
                    capacity_total: 100,
                    capacity_used: 0,
                    max_instances: 100,
                    mode_costs: BTreeMap::from([(QueueMode::OneVsOne, 1)]),
                    last_heartbeat: 0,
                    health: Health::Healthy,
                    failures: 0,
                    instances: BTreeMap::new(),
                },
                ServerLimits {
                    max_capacity: 100,
                    max_instances: 100,
                },
            )
            .unwrap();
        for index in 0..200 {
            let player = PlayerId::from_uuid(uuid::Uuid::from_u128(index + 1));
            let mut party = Party::new(player, &format!("Batch{index}")).unwrap();
            party.state = PartyState::Queued;
            let party_id = party.id;
            state.sessions.insert(format!("session-{index}"), player);
            state.player_party.insert(player, party_id);
            state.parties.insert(party_id, party);
            state.tickets.insert(
                party_id,
                TicketRecord {
                    id: TicketId::from_uuid(uuid::Uuid::from_u128(index + 10_000)),
                    mode: QueueMode::OneVsOne,
                    regions: vec!["tw".into()],
                    enqueued_at: 1,
                    queued_since_ms: 1,
                },
            );
        }
        assert_eq!(state.attempt_match_batch(QueueMode::OneVsOne, "tw"), 100);
        assert_eq!(state.proposals.len(), 100);
        assert!(state.tickets.is_empty());
    }
    #[test]
    fn runtime_disconnect_grace_preserves_then_cancels_queue() {
        let mut state = AuthorityState::new(
            ErpsConfig::default(),
            Arc::new(crate::metrics::Metrics::default()),
        );
        let player = PlayerId::new();
        let party = Party::new(player, "Grace1").unwrap();
        let party_id = party.id;
        state.parties.insert(party_id, party);
        state.player_party.insert(player, party_id);
        state.tickets.insert(
            party_id,
            TicketRecord {
                id: TicketId::new(),
                mode: QueueMode::OneVsOne,
                regions: vec!["tw".into()],
                enqueued_at: 1,
                queued_since_ms: 1,
            },
        );
        state.parties.get_mut(&party_id).unwrap().state = PartyState::Queued;
        state.offline.insert(player);
        state.disconnect_deadlines.insert(player, 31_000);
        state.tick(31_000);
        assert!(state.tickets.contains_key(&party_id));
        state.tick(31_001);
        assert!(!state.tickets.contains_key(&party_id));
        assert_eq!(state.parties[&party_id].state, PartyState::Idle);
    }
}
