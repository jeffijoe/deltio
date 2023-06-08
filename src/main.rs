use clap::Parser;
use deltio::Deltio;
use log::LevelFilter;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// The hostname + port to listen on.
    #[arg(short, long, value_name = "ADDR", default_value = "0.0.0.0:8085")]
    bind: SocketAddr,

    /// The log level to use.
    #[arg(short, long, value_name = "LEVEL", default_value = "info")]
    log: LogLevelArg,

    /// How frequently (in milliseconds) to check push subscriptions for new messages.
    #[arg(long, value_name = "MILLIS", default_value = "1000")]
    push_loop_interval: u64,

    /// Whether to run Deltio on a single thread instead of a worker pool of threads (one per CPU)
    #[arg(short, long)]
    single_thread: bool,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut builder = if args.single_thread {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    };

    builder
        .enable_time()
        .enable_io()
        .thread_name("deltio worker");

    let runtime = builder.build()?;
    runtime.block_on(main_core(args))
}

async fn main_core(args: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Configure the logger.
    env_logger::builder()
        .format_target(false)
        .filter_level(LevelFilter::Off)
        .filter_module("deltio", map_log_level(args.log))
        .parse_default_env()
        .init();

    // Shutdown signal (Ctrl + C)
    let signal = async {
        // Ignore errors from the signal.
        let _ = tokio::signal::ctrl_c().await;
    };

    // Create the server.
    let app = Deltio::new();
    log::info!(
        "Deltio v{} starting, listening on {}",
        clap::crate_version!(),
        &args.bind
    );

    let server = app.server_builder();
    let push_loop_fut = app
        .push_loop(Duration::from_millis(args.push_loop_interval))
        .run();

    // Start listening (TCP).
    let server_fut = server.serve_with_shutdown(args.bind, signal);
    tokio::select! {
        server_result = server_fut => server_result?,
        _ = push_loop_fut => {}
    }

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
