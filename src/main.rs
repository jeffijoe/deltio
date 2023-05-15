use clap::Parser;
use deltio::make_server_builder;
use log::LevelFilter;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// The hostname + port to listen on.
    #[arg(short, long, value_name = "ADDR", default_value = "0.0.0.0:8085")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .format_target(false)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();
    let args = Cli::parse();
    log::info!("Deltio starting, listening on {}", &args.bind);

    make_server_builder().serve(args.bind).await?;

    log::info!("Deltio stopped");

    Ok(())
}
