use erps_client::{Client, ConnectOptions, Event, EventStream, QueueMode};
use tokio_stream::StreamExt;

async fn next_proposal(events: &mut EventStream) -> Result<String, Box<dyn std::error::Error>> {
    loop {
        match events.next().await {
            Some(Ok(Event::Proposal { proposal_id, .. })) => return Ok(proposal_id),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
            None => return Err("ERPS event stream ended before proposal".into()),
        }
    }
}

async fn next_match(
    events: &mut EventStream,
) -> Result<(String, Vec<Vec<String>>, String), Box<dyn std::error::Error>> {
    loop {
        match events.next().await {
            Some(Ok(Event::Matched {
                match_id,
                teams,
                endpoint,
                connection_token: _,
                mode: _,
            })) => return Ok((match_id, teams, endpoint)),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error.into()),
            None => return Err("ERPS event stream ended before match ready".into()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("ERPS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50059".into());
    let mut first =
        Client::connect(ConnectOptions::plaintext_loopback(&endpoint, "example-a")).await?;
    let mut second =
        Client::connect(ConnectOptions::plaintext_loopback(&endpoint, "example-b")).await?;
    let first_party = first.create_party("台灣第一隊1").await?;
    let second_party = second.create_party("台灣第二隊2").await?;
    let mut first_events = first.events().await?;
    let mut second_events = second.events().await?;
    first
        .enqueue(
            first_party.entity_id,
            first_party.revision,
            QueueMode::OneVsOne,
            ["tw".into()],
        )
        .await?;
    second
        .enqueue(
            second_party.entity_id,
            second_party.revision,
            QueueMode::OneVsOne,
            ["tw".into()],
        )
        .await?;

    let first_proposal = next_proposal(&mut first_events).await?;
    let second_proposal = next_proposal(&mut second_events).await?;
    if first_proposal != second_proposal {
        return Err("players received different proposals".into());
    }
    first.accept_match(first_proposal).await?;
    second.accept_match(second_proposal).await?;

    let first_match = next_match(&mut first_events).await?;
    let second_match = next_match(&mut second_events).await?;
    if first_match.0 != second_match.0 || first_match.1 != second_match.1 {
        return Err("players received inconsistent match assignments".into());
    }
    if first_match.1.len() != 2 || first_match.1.iter().any(|team| team.len() != 1) {
        return Err("1v1 match did not contain two singleton teams".into());
    }
    // Connection tokens are intentionally not printed or logged.
    println!(
        "match={} teams={:?} endpoint={}",
        first_match.0, first_match.1, first_match.2
    );
    Ok(())
}
