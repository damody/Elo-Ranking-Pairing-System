//! Versioned C ABI poll facade over `erps-client`.
#![allow(clippy::missing_safety_doc)] // Pointer validity is specified in the public C header.

use erps_client::{Client, ConnectOptions, Event, QueueMode};
use parking_lot::Mutex;
use std::{
    ffi::{c_char, CStr, CString},
    panic::{catch_unwind, AssertUnwindSafe},
    ptr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError},
        Arc,
    },
    thread::ThreadId,
};

pub const ERPS_ABI_VERSION: u32 = 1;
pub const ERPS_OK: i32 = 0;
pub const ERPS_NO_EVENT: i32 = 1;
pub const ERPS_INVALID_ARGUMENT: i32 = -1;
pub const ERPS_RUNTIME_ERROR: i32 = -2;
pub const ERPS_PANIC: i32 = -3;
pub const ERPS_THREAD_MISUSE: i32 = -4;

pub struct ErpsClient {
    runtime: tokio::runtime::Runtime,
    client: Mutex<Client>,
    events: Receiver<ErpsEvent>,
    event_tx: SyncSender<ErpsEvent>,
    events_started: AtomicBool,
    event_stream_failed: Arc<AtomicBool>,
    poll_thread: Mutex<Option<ThreadId>>,
    last_error: Arc<Mutex<CString>>,
}
pub struct ErpsEvent {
    kind: u32,
    entity_id: CString,
    revision: u64,
    endpoint: CString,
    connection_token: CString,
    party_id: CString,
    ticket_id: CString,
    proposal_id: CString,
    match_id: CString,
    teams: Vec<Vec<CString>>,
}

fn ffi(f: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(ERPS_PANIC)
}
fn ffi_value<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(fallback)
}
unsafe fn text<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    CStr::from_ptr(p).to_str().ok()
}
fn cstring(v: impl Into<String>) -> CString {
    CString::new(v.into()).unwrap_or_else(|_| CString::new("invalid-string").unwrap())
}
fn mapped_event(event: Event) -> ErpsEvent {
    let empty = || cstring("");
    match event {
        Event::Party(v) => ErpsEvent {
            kind: 1,
            entity_id: cstring(&v.id),
            revision: v.revision,
            endpoint: cstring(""),
            connection_token: cstring(""),
            party_id: cstring(v.id),
            ticket_id: empty(),
            proposal_id: empty(),
            match_id: empty(),
            teams: Vec::new(),
        },
        Event::Proposal { proposal_id, .. } => ErpsEvent {
            kind: 2,
            entity_id: cstring(&proposal_id),
            revision: 0,
            endpoint: cstring(""),
            connection_token: cstring(""),
            party_id: empty(),
            ticket_id: empty(),
            proposal_id: cstring(&proposal_id),
            match_id: empty(),
            teams: Vec::new(),
        },
        Event::Matched {
            match_id,
            teams,
            endpoint,
            connection_token,
        } => ErpsEvent {
            kind: 3,
            entity_id: cstring(&match_id),
            revision: 0,
            endpoint: cstring(endpoint),
            connection_token: cstring(connection_token),
            party_id: empty(),
            ticket_id: empty(),
            proposal_id: empty(),
            match_id: cstring(&match_id),
            teams: teams
                .into_iter()
                .map(|team| team.into_iter().map(cstring).collect())
                .collect(),
        },
        Event::ServerLost { match_id } => ErpsEvent {
            kind: 4,
            entity_id: cstring(&match_id),
            revision: 0,
            endpoint: cstring(""),
            connection_token: cstring(""),
            party_id: empty(),
            ticket_id: empty(),
            proposal_id: empty(),
            match_id: cstring(&match_id),
            teams: Vec::new(),
        },
        Event::State(v) => ErpsEvent {
            kind: 5,
            entity_id: cstring(v.player_id),
            revision: 0,
            endpoint: cstring(""),
            connection_token: cstring(""),
            party_id: cstring(v.party.as_ref().map_or("", |party| party.id.as_str())),
            ticket_id: cstring(v.ticket_id.unwrap_or_default()),
            proposal_id: cstring(v.proposal_id.unwrap_or_default()),
            match_id: cstring(v.match_id.unwrap_or_default()),
            teams: Vec::new(),
        },
    }
}

#[no_mangle]
pub extern "C" fn erps_abi_version() -> u32 {
    ffi_value(0, || ERPS_ABI_VERSION)
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_create(
    endpoint: *const c_char,
    auth_token: *const c_char,
    out: *mut *mut ErpsClient,
) -> i32 {
    create_client(endpoint, auth_token, ptr::null(), out)
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_create_tls(
    endpoint: *const c_char,
    auth_token: *const c_char,
    tls_domain: *const c_char,
    out: *mut *mut ErpsClient,
) -> i32 {
    if tls_domain.is_null() {
        return ERPS_INVALID_ARGUMENT;
    }
    create_client(endpoint, auth_token, tls_domain, out)
}
unsafe fn create_client(
    endpoint: *const c_char,
    auth_token: *const c_char,
    tls_domain: *const c_char,
    out: *mut *mut ErpsClient,
) -> i32 {
    ffi(|| {
        if out.is_null() {
            return ERPS_INVALID_ARGUMENT;
        }
        let Some(endpoint) = text(endpoint) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(auth) = text(auth_token) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(v) => v,
            Err(_) => return ERPS_RUNTIME_ERROR,
        };
        let options = match text(tls_domain) {
            Some(domain) => ConnectOptions::tls(endpoint, auth, domain),
            None => ConnectOptions::plaintext_loopback(endpoint, auth),
        };
        let client = match runtime.block_on(Client::connect(options)) {
            Ok(v) => v,
            Err(_) => return ERPS_RUNTIME_ERROR,
        };
        let (tx, rx) = mpsc::sync_channel(1024);
        let value = Box::new(ErpsClient {
            runtime,
            client: Mutex::new(client),
            events: rx,
            event_tx: tx,
            events_started: AtomicBool::new(false),
            event_stream_failed: Arc::new(AtomicBool::new(false)),
            poll_thread: Mutex::new(None),
            last_error: Arc::new(Mutex::new(cstring(""))),
        });
        *out = Box::into_raw(value);
        ERPS_OK
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_destroy(client: *mut ErpsClient) {
    if !client.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(client))));
    }
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_shutdown(client: *mut ErpsClient) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        c.runtime.block_on(c.client.lock().shutdown());
        ERPS_OK
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_start_events(client: *mut ErpsClient) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        if c.events_started.swap(true, Ordering::AcqRel) {
            return ERPS_INVALID_ARGUMENT;
        }
        let mut network = c.client.lock().clone();
        let tx = c.event_tx.clone();
        let failed = c.event_stream_failed.clone();
        let last_error = c.last_error.clone();
        c.runtime.spawn(async move {
            if let Ok(mut stream) = network.events().await {
                use tokio_stream::StreamExt;
                loop {
                    match stream.next().await {
                        Some(Ok(event)) => {
                            if tx.try_send(mapped_event(event)).is_err() {
                                *last_error.lock() = cstring(
                                    "event stream stopped because the bounded local queue is full; drain events and call GetState",
                                );
                                failed.store(true, Ordering::Release);
                                break;
                            }
                        }
                        Some(Err(error)) => {
                            *last_error.lock() = cstring(format!(
                                "event stream stopped: {error}; call GetState"
                            ));
                            failed.store(true, Ordering::Release);
                            break;
                        }
                        None => {
                            *last_error.lock() =
                                cstring("event stream ended; call GetState before reconnecting");
                            failed.store(true, Ordering::Release);
                            break;
                        }
                    }
                }
            } else {
                *last_error.lock() = cstring("failed to open event stream; call GetState");
                failed.store(true, Ordering::Release);
            }
        });
        ERPS_OK
    })
}

fn operation_error(
    c: &ErpsClient,
    result: Result<erps_client::Operation, erps_client::Error>,
) -> i32 {
    match result {
        Ok(_) => ERPS_OK,
        Err(error) => {
            *c.last_error.lock() = cstring(error.to_string());
            ERPS_RUNTIME_ERROR
        }
    }
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_create_party(
    client: *mut ErpsClient,
    name: *const c_char,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(name) = text(name) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let mut network = c.client.lock();
        match c.runtime.block_on(network.create_party(name)) {
            Ok(_) => ERPS_OK,
            Err(error) => {
                *c.last_error.lock() = cstring(error.to_string());
                ERPS_RUNTIME_ERROR
            }
        }
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_create_invite(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
    ttl_seconds: u32,
    uses: u32,
    out_token: *mut c_char,
    out_token_size: usize,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(party_id) = text(party_id) else {
            return ERPS_INVALID_ARGUMENT;
        };
        if out_token.is_null() || out_token_size == 0 {
            return ERPS_INVALID_ARGUMENT;
        }
        let result = c.runtime.block_on(c.client.lock().create_invite(
            party_id,
            revision,
            ttl_seconds,
            uses,
        ));
        match result {
            Ok(token) if token.len() < out_token_size => {
                ptr::copy_nonoverlapping(token.as_ptr(), out_token.cast::<u8>(), token.len());
                *out_token.add(token.len()) = 0;
                ERPS_OK
            }
            Ok(_) => ERPS_INVALID_ARGUMENT,
            Err(error) => {
                *c.last_error.lock() = cstring(error.to_string());
                ERPS_RUNTIME_ERROR
            }
        }
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_join_party(
    client: *mut ErpsClient,
    invite_token: *const c_char,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(invite_token) = text(invite_token) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let result = c.runtime.block_on(c.client.lock().join_party(invite_token));
        operation_error(c, result)
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_leave_party(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
) -> i32 {
    party_mutation(client, party_id, revision, ptr::null(), 0)
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_kick_member(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
    player_id: *const c_char,
) -> i32 {
    party_mutation(client, party_id, revision, player_id, 1)
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_rename_party(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
    name: *const c_char,
) -> i32 {
    party_mutation(client, party_id, revision, name, 2)
}
unsafe fn party_mutation(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
    value: *const c_char,
    operation: u8,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(party_id) = text(party_id) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let value = text(value);
        if operation != 0 && value.is_none() {
            return ERPS_INVALID_ARGUMENT;
        }
        let mut network = c.client.lock();
        let result = match operation {
            0 => c.runtime.block_on(network.leave_party(party_id, revision)),
            1 => c
                .runtime
                .block_on(network.kick_member(party_id, revision, value.unwrap())),
            2 => c
                .runtime
                .block_on(network.rename_party(party_id, revision, value.unwrap())),
            _ => return ERPS_INVALID_ARGUMENT,
        };
        operation_error(c, result)
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_enqueue(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
    mode: u32,
    region: *const c_char,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let (Some(party), Some(region)) = (text(party_id), text(region)) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let mode = match mode {
            1 => QueueMode::OneVsOne,
            2 => QueueMode::FiveVsFive,
            3 => QueueMode::FreeForAll,
            _ => return ERPS_INVALID_ARGUMENT,
        };
        let mut network = c.client.lock();
        match c
            .runtime
            .block_on(network.enqueue(party, revision, mode, [region.to_owned()]))
        {
            Ok(_) => ERPS_OK,
            Err(error) => {
                *c.last_error.lock() = cstring(error.to_string());
                ERPS_RUNTIME_ERROR
            }
        }
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_cancel_queue(
    client: *mut ErpsClient,
    party_id: *const c_char,
    revision: u64,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        let Some(party_id) = text(party_id) else {
            return ERPS_INVALID_ARGUMENT;
        };
        let result = c
            .runtime
            .block_on(c.client.lock().cancel_queue(party_id, revision));
        operation_error(c, result)
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_get_state(client: *mut ErpsClient) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        match c.runtime.block_on(c.client.lock().get_state()) {
            Ok(state) => match c.event_tx.try_send(mapped_event(Event::State(state))) {
                Ok(()) => ERPS_OK,
                Err(error) => {
                    *c.last_error.lock() = cstring(format!("state event queue full: {error}"));
                    ERPS_RUNTIME_ERROR
                }
            },
            Err(error) => {
                *c.last_error.lock() = cstring(error.to_string());
                ERPS_RUNTIME_ERROR
            }
        }
    })
}
fn proposal(client: *mut ErpsClient, id: *const c_char, accept: bool) -> i32 {
    unsafe {
        ffi(|| {
            let Some(c) = client.as_ref() else {
                return ERPS_INVALID_ARGUMENT;
            };
            let Some(id) = text(id) else {
                return ERPS_INVALID_ARGUMENT;
            };
            let mut network = c.client.lock();
            let result = if accept {
                c.runtime.block_on(network.accept_match(id))
            } else {
                c.runtime.block_on(network.reject_match(id))
            };
            match result {
                Ok(_) => ERPS_OK,
                Err(error) => {
                    *c.last_error.lock() = cstring(error.to_string());
                    ERPS_RUNTIME_ERROR
                }
            }
        })
    }
}
#[no_mangle]
pub extern "C" fn erps_client_accept(client: *mut ErpsClient, proposal_id: *const c_char) -> i32 {
    proposal(client, proposal_id, true)
}
#[no_mangle]
pub extern "C" fn erps_client_reject(client: *mut ErpsClient, proposal_id: *const c_char) -> i32 {
    proposal(client, proposal_id, false)
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_poll(
    client: *mut ErpsClient,
    out: *mut *mut ErpsEvent,
) -> i32 {
    ffi(|| {
        let Some(c) = client.as_ref() else {
            return ERPS_INVALID_ARGUMENT;
        };
        if out.is_null() {
            return ERPS_INVALID_ARGUMENT;
        }
        let current = std::thread::current().id();
        let mut owner = c.poll_thread.lock();
        match *owner {
            Some(bound) if bound != current => return ERPS_THREAD_MISUSE,
            None => *owner = Some(current),
            _ => {}
        }
        drop(owner);
        match c.events.try_recv() {
            Ok(v) => {
                *out = Box::into_raw(Box::new(v));
                ERPS_OK
            }
            Err(TryRecvError::Empty) if c.event_stream_failed.load(Ordering::Acquire) => {
                ERPS_RUNTIME_ERROR
            }
            Err(TryRecvError::Empty) => ERPS_NO_EVENT,
            Err(TryRecvError::Disconnected) => ERPS_RUNTIME_ERROR,
        }
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_kind(event: *const ErpsEvent) -> u32 {
    ffi_value(0, || event.as_ref().map_or(0, |v| v.kind))
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_entity_id(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event.as_ref().map_or(ptr::null(), |v| v.entity_id.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_revision(event: *const ErpsEvent) -> u64 {
    ffi_value(0, || event.as_ref().map_or(0, |v| v.revision))
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_endpoint(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event.as_ref().map_or(ptr::null(), |v| v.endpoint.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_connection_token(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event
            .as_ref()
            .map_or(ptr::null(), |v| v.connection_token.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_party_id(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event.as_ref().map_or(ptr::null(), |v| v.party_id.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_ticket_id(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event.as_ref().map_or(ptr::null(), |v| v.ticket_id.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_proposal_id(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event
            .as_ref()
            .map_or(ptr::null(), |v| v.proposal_id.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_match_id(event: *const ErpsEvent) -> *const c_char {
    ffi_value(ptr::null(), || {
        event.as_ref().map_or(ptr::null(), |v| v.match_id.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_team_count(event: *const ErpsEvent) -> usize {
    ffi_value(0, || event.as_ref().map_or(0, |value| value.teams.len()))
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_team_player_count(
    event: *const ErpsEvent,
    team_index: usize,
) -> usize {
    ffi_value(0, || {
        event
            .as_ref()
            .and_then(|value| value.teams.get(team_index))
            .map_or(0, Vec::len)
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_team_player_id(
    event: *const ErpsEvent,
    team_index: usize,
    player_index: usize,
) -> *const c_char {
    ffi_value(ptr::null(), || {
        event
            .as_ref()
            .and_then(|value| value.teams.get(team_index))
            .and_then(|team| team.get(player_index))
            .map_or(ptr::null(), |player| player.as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_client_last_error(client: *const ErpsClient) -> *const c_char {
    ffi_value(ptr::null(), || {
        client
            .as_ref()
            .map_or(ptr::null(), |value| value.last_error.lock().as_ptr())
    })
}
#[no_mangle]
pub unsafe extern "C" fn erps_event_release(event: *mut ErpsEvent) {
    if !event.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(event))));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn abi_is_stable() {
        assert_eq!(erps_abi_version(), 1)
    }
    #[test]
    fn mapped_values_have_no_cross_allocator_ownership() {
        let event = mapped_event(Event::ServerLost {
            match_id: "m1".into(),
        });
        assert_eq!(event.kind, 4);
        assert_eq!(event.entity_id.to_str().unwrap(), "m1")
    }
    #[test]
    fn exported_value_guard_contains_panics() {
        assert_eq!(ffi_value(17_u32, || panic!("ffi test panic")), 17);
    }
}
