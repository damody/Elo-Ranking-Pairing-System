#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = std::env::args()
        .nth(1)
        .map(erps::ErpsConfig::load)
        .transpose()?
        .unwrap_or_default();
    config.validate()?;
    let address = std::env::var("ERPS_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".into())
        .parse()?;
    println!("ERPS listening on {address}");
    if let (Ok(path), Ok(server_path)) = (
        std::env::var("ERPS_AUTH_TOKEN_MAP"),
        std::env::var("ERPS_SERVER_AUTH_TOKEN_MAP"),
    ) {
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
