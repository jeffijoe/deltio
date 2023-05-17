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
    // Configure the logger.
    env_logger::builder()
        .format_target(false)
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    // Parse the CLI arguments.
    let args = Cli::parse();

    // Shutdown signal (Ctrl + C)
    let signal = async {
        // Ignore errors.
        let _ = tokio::signal::ctrl_c().await;
    };

    // Create the server.
    let server = make_server_builder();

    // Start listening (TCP).
    log::info!("Deltio starting, listening on {}", &args.bind);
    server.serve_with_shutdown(args.bind, signal).await?;

    log::info!("Deltio stopped");

    Ok(())
}
