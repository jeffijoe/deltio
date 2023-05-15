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
        .init();
    let args = Cli::parse();
    log::info!("Deltio started, listening on {}", &args.bind);

    let serve_fut = make_server_builder().serve(args.bind);
    let join_handle = tokio::spawn(serve_fut);
    let _ = tokio::join!(join_handle);

    log::info!("Deltio stopped");

    Ok(())
}
