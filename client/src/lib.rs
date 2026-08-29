#![forbid(unsafe_code)]
#![allow(clippy::result_large_err)] // Preserve tonic status metadata in the public SDK error.
//! Typed ERPS client. Generated tonic types are kept behind this API.

use erps_proto::v1::{self as pb, matchmaking_service_client::MatchmakingServiceClient};
use std::{pin::Pin, time::Duration};
use thiserror::Error;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

pub const API_MAJOR: u32 = 1;
pub const API_MINOR: u32 = 0;

#[derive(Debug, Error)]
pub enum Error {
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("server rejected operation {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("not connected")]
    NotConnected,
    #[error("protocol: {0}")]
    Protocol(&'static str),
}

#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub endpoint: String,
    pub auth_token: String,
    pub tls_domain: Option<String>,
    pub tls_ca_pem: Option<Vec<u8>>,
    pub connect_timeout: Duration,
}
impl ConnectOptions {
    pub fn plaintext_loopback(endpoint: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth_token: auth_token.into(),
            tls_domain: None,
            tls_ca_pem: None,
            connect_timeout: Duration::from_secs(5),
        }
    }
    pub fn tls(
        endpoint: impl Into<String>,
        auth_token: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth_token: auth_token.into(),
            tls_domain: Some(domain.into()),
            tls_ca_pem: None,
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub fn tls_with_ca(
        endpoint: impl Into<String>,
        auth_token: impl Into<String>,
        domain: impl Into<String>,
        ca_pem: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth_token: auth_token.into(),
            tls_domain: Some(domain.into()),
            tls_ca_pem: Some(ca_pem.into()),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueMode {
    OneVsOne,
    FiveVsFive,
    FreeForAll,
}
impl QueueMode {
    fn wire(self) -> i32 {
        match self {
            Self::OneVsOne => pb::QueueMode::OneVOne as i32,
            Self::FiveVsFive => pb::QueueMode::FiveVFive as i32,
            Self::FreeForAll => pb::QueueMode::FreeForAll as i32,
        }
    }
    fn from_wire(value: i32) -> Option<Self> {
        match pb::QueueMode::try_from(value).ok()? {
            pb::QueueMode::OneVOne => Some(Self::OneVsOne),
            pb::QueueMode::FiveVFive => Some(Self::FiveVsFive),
            pb::QueueMode::FreeForAll => Some(Self::FreeForAll),
            pb::QueueMode::Unspecified => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub entity_id: String,
    pub revision: u64,
}
#[derive(Clone, Debug)]
pub struct State {
    pub player_id: String,
    pub profile: Option<Player>,
    pub credit_suspended_until_ms: Option<i64>,
    pub queue_mode: Option<QueueMode>,
    pub allowed_regions: Vec<String>,
    pub proposal_deadline_ms: Option<i64>,
    pub party: Option<Party>,
    pub ticket_id: Option<String>,
    pub proposal_id: Option<String>,
    pub match_id: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Party {
    pub id: String,
    pub name: String,
    pub leader_id: String,
    pub members: Vec<String>,
    pub player_details: Vec<Player>,
    pub revision: u64,
    pub state: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Player {
    pub id: String,
    /// Legacy 1v1 rating retained for source compatibility.
    pub rating: i32,
    pub rating_one_v_one: i32,
    pub rating_five_v_five: i32,
    pub rating_free_for_all: i32,
    pub credit: u32,
}
#[derive(Clone, Debug)]
// Keep events as directly matchable values in the public SDK. Boxing only State
// would make consumers handle one event differently and would break the API.
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Party(Party),
    Proposal {
        proposal_id: String,
        deadline_ms: i64,
    },
    Matched {
        match_id: String,
        mode: QueueMode,
        teams: Vec<Vec<String>>,
        endpoint: String,
        connection_token: String,
    },
    ServerLost {
        match_id: String,
    },
    ProposalCancelled {
        proposal_id: String,
        reason: String,
        credit: u32,
        eligible: bool,
        credit_suspended_until_ms: Option<i64>,
    },
    State(State),
}
pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Error>> + Send + 'static>>;

#[derive(Clone)]
pub struct Client {
    options: ConnectOptions,
    rpc: MatchmakingServiceClient<Channel>,
    session_token: Option<String>,
    player_id: Option<String>,
}
impl Client {
    pub async fn connect(options: ConnectOptions) -> Result<Self, Error> {
        let mut endpoint = Endpoint::from_shared(options.endpoint.clone())?
            .connect_timeout(options.connect_timeout);
        if let Some(domain) = &options.tls_domain {
            endpoint = endpoint.tls_config(tls_config(domain, options.tls_ca_pem.as_deref()))?;
        }
        let rpc = MatchmakingServiceClient::new(endpoint.connect().await?);
        let mut client = Self {
            options,
            rpc,
            session_token: None,
            player_id: None,
        };
        client.open_session().await?;
        Ok(client)
    }
    async fn open_session(&mut self) -> Result<(), Error> {
        let response = self
            .rpc
            .open_session(pb::ConnectRequest {
                api: Some(api()),
                auth_token: self.options.auth_token.clone(),
            })
            .await?
            .into_inner();
        self.session_token = Some(response.session_token);
        self.player_id = Some(response.player_id);
        Ok(())
    }
    fn token(&self) -> Result<String, Error> {
        self.session_token.clone().ok_or(Error::NotConnected)
    }
    fn meta(&self) -> Result<pb::MutationMeta, Error> {
        Ok(pb::MutationMeta {
            api: Some(api()),
            request_id: uuid::Uuid::new_v4().to_string(),
            session_token: self.token()?,
        })
    }
    pub fn player_id(&self) -> Option<&str> {
        self.player_id.as_deref()
    }
    pub async fn reconnect(&mut self) -> Result<State, Error> {
        let mut endpoint = Endpoint::from_shared(self.options.endpoint.clone())?
            .connect_timeout(self.options.connect_timeout);
        if let Some(domain) = &self.options.tls_domain {
            endpoint =
                endpoint.tls_config(tls_config(domain, self.options.tls_ca_pem.as_deref()))?;
        }
        self.rpc = MatchmakingServiceClient::new(endpoint.connect().await?);
        // Re-authenticate to bind a fresh transport session to the same trusted player identity.
        // The authority keeps party/queue/proposal state during its disconnect grace window.
        self.open_session().await?;
        self.get_state().await
    }
    pub async fn create_party(&mut self, name: impl Into<String>) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .create_party(pb::CreatePartyRequest {
                    meta: Some(meta),
                    name: name.into(),
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn create_invite(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
        ttl_seconds: u32,
        uses: u32,
    ) -> Result<String, Error> {
        let meta = self.meta()?;
        Ok(self
            .rpc
            .create_invite(pb::InviteRequest {
                meta: Some(meta),
                party_id: party_id.into(),
                revision,
                ttl_seconds,
                uses,
            })
            .await?
            .into_inner()
            .token)
    }
    pub async fn join_party(&mut self, token: impl Into<String>) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .join_party(pb::JoinPartyRequest {
                    meta: Some(meta),
                    invite_token: token.into(),
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn leave_party(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
    ) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .leave_party(pb::PartyRequest {
                    meta: Some(meta),
                    party_id: party_id.into(),
                    revision,
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn kick_member(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
        player_id: impl Into<String>,
    ) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .kick_member(pb::MemberRequest {
                    meta: Some(meta),
                    party_id: party_id.into(),
                    revision,
                    player_id: player_id.into(),
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn rename_party(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
        name: impl Into<String>,
    ) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .rename_party(pb::RenamePartyRequest {
                    meta: Some(meta),
                    party_id: party_id.into(),
                    revision,
                    name: name.into(),
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn enqueue(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
        mode: QueueMode,
        regions: impl IntoIterator<Item = String>,
    ) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .enqueue(pb::EnqueueRequest {
                    meta: Some(meta),
                    party_id: party_id.into(),
                    revision,
                    mode: mode.wire(),
                    allowed_regions: regions.into_iter().collect(),
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn cancel_queue(
        &mut self,
        party_id: impl Into<String>,
        revision: u64,
    ) -> Result<Operation, Error> {
        let meta = self.meta()?;
        operation(
            self.rpc
                .cancel_queue(pb::PartyRequest {
                    meta: Some(meta),
                    party_id: party_id.into(),
                    revision,
                })
                .await?
                .into_inner(),
        )
    }
    pub async fn accept_match(
        &mut self,
        proposal_id: impl Into<String>,
    ) -> Result<Operation, Error> {
        self.proposal(proposal_id, true).await
    }
    pub async fn reject_match(
        &mut self,
        proposal_id: impl Into<String>,
    ) -> Result<Operation, Error> {
        self.proposal(proposal_id, false).await
    }
    async fn proposal(&mut self, id: impl Into<String>, accept: bool) -> Result<Operation, Error> {
        let request = pb::ProposalResponseRequest {
            meta: Some(self.meta()?),
            proposal_id: id.into(),
        };
        let value = if accept {
            self.rpc.accept_match(request).await?
        } else {
            self.rpc.reject_match(request).await?
        };
        operation(value.into_inner())
    }
    pub async fn get_state(&mut self) -> Result<State, Error> {
        let token = self.token()?;
        Ok(state(
            self.rpc
                .get_state(pb::StateRequest {
                    session_token: token,
                    api: Some(api()),
                })
                .await?
                .into_inner(),
        ))
    }
    pub async fn events(&mut self) -> Result<EventStream, Error> {
        self.token()?;
        let mut client = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            let mut reconnect_backoff = Duration::from_millis(100);
            loop {
                let token = match client.token() {
                    Ok(token) => token,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
                let opened = client
                    .rpc
                    .watch_events(pb::WatchEventsRequest {
                        session_token: token,
                        api: Some(api()),
                    })
                    .await;
                if let Ok(response) = opened {
                    reconnect_backoff = Duration::from_millis(100);
                    let mut stream = response.into_inner();
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(value) => {
                                if tx.send(event(value)).await.is_err() {
                                    return;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
                tokio::time::sleep(reconnect_backoff).await;
                match client.reconnect().await {
                    Ok(reconciled) => {
                        reconnect_backoff = Duration::from_millis(100);
                        if tx.send(Ok(Event::State(reconciled))).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
                        if tx.send(Err(error)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
    pub async fn shutdown(&mut self) {
        self.session_token = None;
        self.player_id = None;
    }
}

fn tls_config(domain: &str, ca_pem: Option<&[u8]>) -> ClientTlsConfig {
    let config = ClientTlsConfig::new()
        .with_enabled_roots()
        .domain_name(domain.to_owned());
    match ca_pem {
        Some(pem) => config.ca_certificate(Certificate::from_pem(pem)),
        None => config,
    }
}
fn api() -> pb::ApiVersion {
    pb::ApiVersion {
        major: API_MAJOR,
        minor: API_MINOR,
        capabilities: vec!["state-reconcile".into()],
    }
}
fn operation(v: pb::OperationResult) -> Result<Operation, Error> {
    if v.accepted {
        Ok(Operation {
            entity_id: v.entity_id,
            revision: v.revision,
        })
    } else {
        Err(Error::Rejected {
            code: v.code,
            message: v.message,
        })
    }
}
fn party(v: pb::PartyView) -> Party {
    let player_details: Vec<_> = v
        .members
        .into_iter()
        .map(|p| Player {
            id: p.player_id,
            rating: p.rating,
            rating_one_v_one: p.rating_one_v_one,
            rating_five_v_five: p.rating_five_v_five,
            rating_free_for_all: p.rating_free_for_all,
            credit: p.credit,
        })
        .collect();
    Party {
        id: v.party_id,
        name: v.name,
        leader_id: v.leader_id,
        members: player_details
            .iter()
            .map(|player| player.id.clone())
            .collect(),
        player_details,
        revision: v.revision,
        state: v.state,
    }
}
fn state(v: pb::ClientState) -> State {
    State {
        player_id: v.player_id,
        profile: v.profile.map(player),
        credit_suspended_until_ms: (v.credit_suspended_until_ms > 0)
            .then_some(v.credit_suspended_until_ms),
        queue_mode: QueueMode::from_wire(v.queue_mode),
        allowed_regions: v.allowed_regions,
        proposal_deadline_ms: (v.proposal_deadline_ms > 0).then_some(v.proposal_deadline_ms),
        party: v.party.map(party),
        ticket_id: nonempty(v.ticket_id),
        proposal_id: nonempty(v.proposal_id),
        match_id: nonempty(v.match_id),
    }
}
fn player(p: pb::PlayerView) -> Player {
    Player {
        id: p.player_id,
        rating: p.rating,
        rating_one_v_one: p.rating_one_v_one,
        rating_five_v_five: p.rating_five_v_five,
        rating_free_for_all: p.rating_free_for_all,
        credit: p.credit,
    }
}
fn nonempty(v: String) -> Option<String> {
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}
fn event(v: pb::ClientEvent) -> Result<Event, Error> {
    Ok(
        match v.event.ok_or(Error::Protocol("event payload missing"))? {
            pb::client_event::Event::Party(p) => Event::Party(party(p)),
            pb::client_event::Event::ProposalId(id) => Event::Proposal {
                proposal_id: id,
                deadline_ms: v.deadline_ms,
            },
            pb::client_event::Event::Matched(m) => Event::Matched {
                match_id: m.match_id,
                mode: QueueMode::from_wire(m.mode)
                    .ok_or(Error::Protocol("matched event queue mode missing"))?,
                teams: m.teams.into_iter().map(|team| team.player_ids).collect(),
                endpoint: m.endpoint,
                connection_token: m.connection_token,
            },
            pb::client_event::Event::ServerLostMatchId(id) => Event::ServerLost { match_id: id },
            pb::client_event::Event::State(s) => Event::State(state(s)),
            pb::client_event::Event::ProposalCancelled(cancelled) => Event::ProposalCancelled {
                proposal_id: cancelled.proposal_id,
                reason: cancelled.reason,
                credit: cancelled.credit,
                eligible: cancelled.eligible,
                credit_suspended_until_ms: (cancelled.credit_suspended_until_ms > 0)
                    .then_some(cancelled.credit_suspended_until_ms),
            },
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_state_ids_are_none() {
        let s = state(pb::ClientState::default());
        assert!(s.ticket_id.is_none());
        assert!(s.match_id.is_none());
        assert!(s.profile.is_none());
        assert!(s.credit_suspended_until_ms.is_none());
    }
    #[test]
    fn modes_are_stable() {
        assert_ne!(QueueMode::OneVsOne.wire(), QueueMode::FiveVsFive.wire())
    }
    #[test]
    fn matched_event_preserves_roster() {
        let decoded = event(pb::ClientEvent {
            event: Some(pb::client_event::Event::Matched(pb::MatchEvent {
                match_id: "match".into(),
                mode: pb::QueueMode::OneVOne as i32,
                teams: vec![pb::Team {
                    team_index: 0,
                    player_ids: vec!["player".into()],
                }],
                ..Default::default()
            })),
            deadline_ms: 0,
        })
        .unwrap();
        assert!(matches!(decoded, Event::Matched { teams, .. } if teams == vec![vec!["player"]]));
    }
    #[test]
    fn party_event_preserves_player_rating_and_credit() {
        let decoded = event(pb::ClientEvent {
            event: Some(pb::client_event::Event::Party(pb::PartyView {
                members: vec![pb::PlayerView {
                    player_id: "player".into(),
                    rating: 1234,
                    credit: 95,
                    rating_one_v_one: 1234,
                    rating_five_v_five: 1100,
                    rating_free_for_all: 900,
                }],
                ..Default::default()
            })),
            deadline_ms: 0,
        })
        .unwrap();
        assert!(
            matches!(decoded, Event::Party(Party { members, player_details, .. }) if members == vec!["player"] && player_details == vec![Player { id: "player".into(), rating: 1234, rating_one_v_one: 1234, rating_five_v_five: 1100, rating_free_for_all: 900, credit: 95 }])
        );
    }
    #[test]
    fn state_and_cancellation_preserve_player_facing_credit_details() {
        let profile = pb::PlayerView {
            player_id: "player".into(),
            rating: 1001,
            credit: 55,
            rating_one_v_one: 1001,
            rating_five_v_five: 1002,
            rating_free_for_all: 1003,
        };
        let decoded = state(pb::ClientState {
            player_id: "player".into(),
            profile: Some(profile),
            credit_suspended_until_ms: 12345,
            queue_mode: pb::QueueMode::FiveVFive as i32,
            allowed_regions: vec!["tw".into(), "us".into()],
            proposal_deadline_ms: 23456,
            ..Default::default()
        });
        assert_eq!(decoded.profile.unwrap().rating_free_for_all, 1003);
        assert_eq!(decoded.credit_suspended_until_ms, Some(12345));
        assert_eq!(decoded.queue_mode, Some(QueueMode::FiveVsFive));
        assert_eq!(decoded.allowed_regions, vec!["tw", "us"]);
        assert_eq!(decoded.proposal_deadline_ms, Some(23456));

        let cancelled = event(pb::ClientEvent {
            event: Some(pb::client_event::Event::ProposalCancelled(
                pb::ProposalCancelledEvent {
                    proposal_id: "proposal".into(),
                    reason: "timed_out".into(),
                    credit: 55,
                    eligible: false,
                    credit_suspended_until_ms: 12345,
                },
            )),
            deadline_ms: 0,
        })
        .unwrap();
        assert!(
            matches!(cancelled, Event::ProposalCancelled { reason, credit: 55, eligible: false, credit_suspended_until_ms: Some(12345), .. } if reason == "timed_out")
        );
    }
}
