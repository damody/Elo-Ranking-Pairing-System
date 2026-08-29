#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if matches!(args.first().map(String::as_str), Some("-h" | "--help")) {
        println!(
            "ERPS matchmaking server\n\nUsage: erps-server [CONFIG_PATH]\n\nEnvironment:\n  ERPS_LISTEN                 Listen address (default 127.0.0.1:50051)\n  ERPS_AUTH_TOKEN_MAP         Player token-to-UUID JSON object content\n  ERPS_SERVER_AUTH_TOKEN_MAP  Game-server token-to-UUID JSON object content\n\nThe two token-map variables must be configured together. Start production from a protected copy of erps/config/production.example.toml."
        );
        return Ok(());
    }
    if args.len() > 1 {
        anyhow::bail!("expected at most one CONFIG_PATH; use --help for usage");
    }
    let config = args
        .first()
        .map(erps::ErpsConfig::load)
        .transpose()?
        .unwrap_or_default();
    config.validate()?;
    let address = std::env::var("ERPS_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".into())
        .parse()?;
    let auth_maps = (
        std::env::var("ERPS_AUTH_TOKEN_MAP").ok(),
        std::env::var("ERPS_SERVER_AUTH_TOKEN_MAP").ok(),
    );
    if auth_maps.0.is_some() != auth_maps.1.is_some() {
        anyhow::bail!(
            "ERPS_AUTH_TOKEN_MAP and ERPS_SERVER_AUTH_TOKEN_MAP must be configured together"
        );
    }
    println!("ERPS listening on {address}");
    if let (Some(path), Some(server_path)) = auth_maps {
        let validator =
            std::sync::Arc::new(erps::grpc::StaticTokenValidator::from_json_file(path)?);
        let server_validator = std::sync::Arc::new(
            erps::grpc::StaticServerTokenValidator::from_json_file(server_path)?,
        );
        erps::grpc::serve_with_validators(address, config, validator, server_validator, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    } else {
        erps::grpc::serve(address, config, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }
}
