use erps::{
    components::{PartyState, QueueMode},
    id::PlayerId,
    party::Party,
    session::Sessions,
};

#[test]
fn cancel_unfreezes_party_and_disconnect_expires_after_grace() {
    let player = PlayerId::new();
    let mut party = Party::new(player, "隊伍1").unwrap();
    party.validate_enqueue(QueueMode::OneVsOne).unwrap();
    party.state = PartyState::Queued;
    party.state = PartyState::Idle;
    assert!(party.rename(player, party.revision, "隊伍2").is_ok());
    let mut sessions = Sessions::default();
    let session = sessions.connect(player);
    sessions.disconnect(session, 100);
    assert!(sessions.expired_players(130, 30).is_empty());
    assert_eq!(sessions.expired_players(131, 30), vec![player]);
}
