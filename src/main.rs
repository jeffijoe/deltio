use deltio::make_server_builder;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = std::env::args().collect::<Vec<String>>();
    let port = args
        .get(1)
        .expect("Expected first argument to be a port number");
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("Deltio listening on {}", &addr);

    let serve_fut = make_server_builder().serve(addr);
    let join_handle = tokio::spawn(serve_fut);
    let _ = tokio::join!(join_handle);
    // tokio::select! {
    //     r = join_handle => r??,
    //     _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {}
    // }

    println!("Deltio exiting");

    Ok(())
}
