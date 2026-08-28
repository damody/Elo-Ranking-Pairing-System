#![forbid(unsafe_code)]
#![allow(clippy::result_large_err)] // Preserve tonic status metadata in the public SDK error.
//! Typed ERPS client. Generated tonic types are kept behind this API.

use erps_proto::v1::{self as pb, matchmaking_service_client::MatchmakingServiceClient};
use std::{pin::Pin, time::Duration};
use thiserror::Error;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

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
    pub connect_timeout: Duration,
}
impl ConnectOptions {
    pub fn plaintext_loopback(endpoint: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth_token: auth_token.into(),
            tls_domain: None,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub entity_id: String,
    pub revision: u64,
}
#[derive(Clone, Debug)]
pub struct State {
    pub player_id: String,
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
    pub revision: u64,
    pub state: String,
}
#[derive(Clone, Debug)]
pub enum Event {
    Party(Party),
    Proposal {
        proposal_id: String,
        deadline_ms: i64,
    },
    Matched {
        match_id: String,
        teams: Vec<Vec<String>>,
        endpoint: String,
        connection_token: String,
    },
    ServerLost {
        match_id: String,
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
            endpoint = endpoint.tls_config(ClientTlsConfig::new().domain_name(domain.clone()))?;
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
            endpoint = endpoint.tls_config(ClientTlsConfig::new().domain_name(domain.clone()))?;
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
                tokio::time::sleep(Duration::from_millis(100)).await;
                match client.reconnect().await {
                    Ok(reconciled) => {
                        if tx.send(Ok(Event::State(reconciled))).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
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
    Party {
        id: v.party_id,
        name: v.name,
        leader_id: v.leader_id,
        members: v.members.into_iter().map(|p| p.player_id).collect(),
        revision: v.revision,
        state: v.state,
    }
}
fn state(v: pb::ClientState) -> State {
    State {
        player_id: v.player_id,
        party: v.party.map(party),
        ticket_id: nonempty(v.ticket_id),
        proposal_id: nonempty(v.proposal_id),
        match_id: nonempty(v.match_id),
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
                teams: m.teams.into_iter().map(|team| team.player_ids).collect(),
                endpoint: m.endpoint,
                connection_token: m.connection_token,
            },
            pb::client_event::Event::ServerLostMatchId(id) => Event::ServerLost { match_id: id },
            pb::client_event::Event::State(s) => Event::State(state(s)),
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
        assert!(s.match_id.is_none())
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
}
