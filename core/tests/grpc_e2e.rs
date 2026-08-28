use erps::ErpsConfig;
use erps_client::{Client, ConnectOptions, Event, QueueMode};
use erps_proto::v1::{self as pb, game_server_service_client::GameServerServiceClient};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout, Duration},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

async fn proposal(stream: &mut erps_client::EventStream) -> String {
    loop {
        if let Event::Proposal { proposal_id, .. } = timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            return proposal_id;
        }
    }
}
async fn matched(stream: &mut erps_client::EventStream) -> (String, String) {
    loop {
        if let Event::Matched {
            endpoint,
            connection_token,
            ..
        } = timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            return (endpoint, connection_token);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_grpc_party_ready_launch_and_endpoint() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let config = ErpsConfig {
            allow_development_plaintext: true,
            ..ErpsConfig::default()
        };
        erps::grpc::serve(addr, config, async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap()
    });
    let endpoint = format!("http://{addr}");
    sleep(Duration::from_millis(100)).await;
    let mut game = GameServerServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let server_id = uuid::Uuid::new_v4().to_string();
    game.register(pb::RegisterServerRequest {
        api: Some(pb::ApiVersion {
            major: 1,
            minor: 0,
            capabilities: vec![],
        }),
        auth_token: "server-secret".into(),
        server_id: server_id.clone(),
        generation: 1,
        endpoint: "127.0.0.1:7000".into(),
        region: "tw".into(),
        capacity_total: 100,
        max_instances: 100,
        mode_costs: vec![pb::ModeCost {
            mode: pb::QueueMode::OneVOne as i32,
            cost: 1,
        }],
        instances: vec![],
        server_class: String::new(),
    })
    .await
    .unwrap();
    let (control_tx, control_rx) = mpsc::channel(16);
    let mut controls = game
        .control_stream(ReceiverStream::new(control_rx))
        .await
        .unwrap()
        .into_inner();
    control_tx
        .send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id: server_id.clone(),
            generation: 1,
            auth_token: "server-auth".into(),
            message: Some(pb::server_control::Message::Heartbeat(pb::Heartbeat {
                capacity_used: 0,
                running_instances: 0,
            })),
        })
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    let mut a = Client::connect(ConnectOptions::plaintext_loopback(&endpoint, "player-a"))
        .await
        .unwrap();
    let mut b = Client::connect(ConnectOptions::plaintext_loopback(&endpoint, "player-b"))
        .await
        .unwrap();
    let pa = a.create_party("隊伍甲1").await.unwrap();
    let pbp = b.create_party("隊伍乙2").await.unwrap();
    let mut ae = a.events().await.unwrap();
    let mut be = b.events().await.unwrap();
    a.enqueue(
        &pa.entity_id,
        pa.revision,
        QueueMode::OneVsOne,
        ["tw".into()],
    )
    .await
    .unwrap();
    b.enqueue(
        &pbp.entity_id,
        pbp.revision,
        QueueMode::OneVsOne,
        ["tw".into()],
    )
    .await
    .unwrap();
    let proposal_a = proposal(&mut ae).await;
    let proposal_b = proposal(&mut be).await;
    assert_eq!(proposal_a, proposal_b);
    a.accept_match(&proposal_a).await.unwrap();
    b.accept_match(&proposal_b).await.unwrap();
    let launch = timeout(Duration::from_secs(3), controls.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pb::erps_control::Message::Launch(launch) = launch.message.unwrap() else {
        panic!("expected launch")
    };
    control_tx
        .send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id: server_id.clone(),
            generation: 1,
            auth_token: "server-auth".into(),
            message: Some(pb::server_control::Message::LaunchResult(
                pb::LaunchResult {
                    match_id: launch.match_id.clone(),
                    state: "accepted".into(),
                    endpoint: String::new(),
                    connection_token: String::new(),
                    reason: String::new(),
                },
            )),
        })
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(100), ae.next())
        .await
        .is_err());
    control_tx
        .send(pb::ServerControl {
            api: Some(pb::ApiVersion {
                major: 1,
                minor: 0,
                capabilities: vec![],
            }),
            server_id,
            generation: 1,
            auth_token: "server-auth".into(),
            message: Some(pb::server_control::Message::LaunchResult(
                pb::LaunchResult {
                    match_id: launch.match_id,
                    state: "ready".into(),
                    endpoint: "127.0.0.1:7001".into(),
                    connection_token: "one-time-token".into(),
                    reason: String::new(),
                },
            )),
        })
        .await
        .unwrap();
    assert_eq!(
        matched(&mut ae).await,
        ("127.0.0.1:7001".into(), "one-time-token".into())
    );
    assert_eq!(
        matched(&mut be).await,
        ("127.0.0.1:7001".into(), "one-time-token".into())
    );
    let _ = stop_tx.send(());
}
