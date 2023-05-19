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

    /// The log level to use.
    #[arg(short, long, value_name = "LEVEL", default_value = "info")]
    log: LogLevelArg,
}

#[derive(clap::ValueEnum, Clone)]
enum LogLevelArg {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the CLI arguments.
    let args = Cli::parse();

    // Configure the logger.
    env_logger::builder()
        .format_target(false)
        .filter_level(map_log_level(args.log))
        // Turn off noisy library logs.
        .filter_module("h2", LevelFilter::Off)
        .filter_module("hyper", LevelFilter::Off)
        .parse_default_env()
        .init();

    // Shutdown signal (Ctrl + C)
    let signal = async {
        // Ignore errors from the signal.
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

/// Maps the log level argument to the `LevelFilter` enum.
fn map_log_level(level: LogLevelArg) -> LevelFilter {
    match level {
        LogLevelArg::Off => LevelFilter::Off,
        LogLevelArg::Error => LevelFilter::Error,
        LogLevelArg::Warn => LevelFilter::Warn,
        LogLevelArg::Info => LevelFilter::Info,
        LogLevelArg::Debug => LevelFilter::Debug,
        LogLevelArg::Trace => LevelFilter::Trace,
    }
}
