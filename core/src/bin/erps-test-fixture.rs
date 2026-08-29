use erps_proto::v1::{self as pb, game_server_service_client::GameServerServiceClient};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tokio_stream::wrappers::ReceiverStream;
fn api() -> pb::ApiVersion {
    pb::ApiVersion {
        major: 1,
        minor: 0,
        capabilities: vec![],
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:50059".parse()?;
    let config = erps::ErpsConfig {
        allow_development_plaintext: true,
        ..Default::default()
    };
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        erps::grpc::serve(addr, config, async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap()
    });
    sleep(Duration::from_millis(100)).await;
    let mut game = GameServerServiceClient::connect("http://127.0.0.1:50059").await?;
    let server_id = uuid::Uuid::new_v4().to_string();
    game.register(pb::RegisterServerRequest {
        api: Some(pb::ApiVersion {
            major: 1,
            minor: 0,
            capabilities: vec![],
        }),
        auth_token: "fixture-server".into(),
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
    .await?;
    let (tx, rx) = mpsc::channel(32);
    let mut controls = game
        .control_stream(ReceiverStream::new(rx))
        .await?
        .into_inner();
    tx.send(pb::ServerControl {
        api: Some(api()),
        server_id: server_id.clone(),
        generation: 1,
        auth_token: "fixture-server".into(),
        message: Some(pb::server_control::Message::Heartbeat(pb::Heartbeat {
            capacity_used: 0,
            running_instances: 0,
        })),
    })
    .await?;
    println!("ERPS_FIXTURE_READY");
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {value=controls.message()=>{let Some(control)=value? else{break};if let Some(pb::erps_control::Message::Launch(launch))=control.message{let match_id=launch.match_id;tx.send(pb::ServerControl{server_id:server_id.clone(),generation:1,auth_token:"fixture-server".into(),message:Some(pb::server_control::Message::LaunchResult(pb::LaunchResult{match_id:match_id.clone(),state:"accepted".into(),endpoint:String::new(),connection_token:String::new(),reason:String::new()})),api:Some(api())}).await?;tx.send(pb::ServerControl{server_id:server_id.clone(),generation:1,auth_token:"fixture-server".into(),message:Some(pb::server_control::Message::LaunchResult(pb::LaunchResult{match_id,state:"ready".into(),endpoint:"127.0.0.1:7001".into(),connection_token:uuid::Uuid::new_v4().to_string(),reason:String::new()})),api:Some(api())}).await?;}} _=heartbeat.tick()=>{tx.send(pb::ServerControl{server_id:server_id.clone(),generation:1,auth_token:"fixture-server".into(),message:Some(pb::server_control::Message::Heartbeat(pb::Heartbeat{capacity_used:0,running_instances:0})),api:Some(api())}).await?;} _=tokio::signal::ctrl_c()=>break}
    }
    let _ = stop_tx.send(());
    Ok(())
}
