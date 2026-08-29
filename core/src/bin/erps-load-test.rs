#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = erps::load_test::ScenarioConfig::default();
    let mut args = std::env::args().skip(1);
    let mut grpc = false;
    let mut baseline = None;
    let mut output = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!(
                    "ERPS deterministic load validator\n\nUsage: erps-load-test [OPTIONS]\n\nOptions:\n  --players <COUNT>       Player count (minimum 20; default 100000)\n  --seed <SEED>           Deterministic seed\n  --workers <COUNT>       Rayon worker count (minimum 1)\n  --grpc                  Exercise the full loopback gRPC path\n  --baseline <PATH>       Compare against a JSON report\n  --output <PATH>         Save the JSON report\n  -h, --help              Print this help"
                );
                return Ok(());
            }
            "--players" => {
                config.players = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--players requires a value"))?
                    .parse()?
            }
            "--seed" => {
                config.seed = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--seed requires a value"))?
                    .parse()?
            }
            "--workers" => {
                config.workers = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--workers requires a value"))?
                    .parse()?
            }
            "--grpc" => grpc = true,
            "--baseline" => {
                baseline = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--baseline requires a JSON report path"))?,
                )
            }
            "--output" => {
                output = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--output requires a JSON report path"))?,
                )
            }
            unknown => return Err(anyhow::anyhow!("unknown argument {unknown}")),
        }
    }
    let transport = if grpc {
        Some(
            erps::load_test::run_grpc(&config)
                .await
                .map_err(anyhow::Error::msg)?,
        )
    } else {
        None
    };
    let mut report = erps::load_test::run(config).map_err(anyhow::Error::msg)?;
    report.transport = transport;
    if let Some(path) = baseline {
        let value: serde_json::Value = serde_json::from_slice(&tokio::fs::read(path).await?)?;
        report.baseline = Some(erps::load_test::compare_baseline(&report, &value));
    }
    let serialized = serde_json::to_string_pretty(&report)?;
    if let Some(path) = output {
        tokio::fs::write(path, serialized.as_bytes()).await?;
    }
    println!("{serialized}");
    Ok(())
}
