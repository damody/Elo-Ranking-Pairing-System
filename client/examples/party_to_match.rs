use erps_client::{Client, ConnectOptions, Event, QueueMode};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options =
        ConnectOptions::plaintext_loopback("http://127.0.0.1:50051", "development-player");
    let mut client = Client::connect(options).await?;
    let party = client.create_party("台灣第一隊1").await?;
    let mut events = client.events().await?;
    client
        .enqueue(
            party.entity_id,
            party.revision,
            QueueMode::OneVsOne,
            ["tw".into()],
        )
        .await?;
    let proposal_id = loop {
        match events.next().await {
            Some(Ok(Event::Proposal { proposal_id, .. })) => break proposal_id,
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
            None => return Err("ERPS event stream ended before proposal".into()),
        }
    };
    client.accept_match(proposal_id).await?;
    loop {
        match events.next().await {
            Some(Ok(Event::Matched {
                match_id,
                teams,
                endpoint,
                connection_token,
            })) => {
                println!(
                    "match={match_id} teams={teams:?} endpoint={endpoint} token={connection_token}"
                );
                return Ok(());
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
            None => return Err("ERPS event stream ended before match ready".into()),
        }
    }
}
