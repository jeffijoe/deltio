use deltio::make_server_builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<String>>();
    let port = args
        .get(1)
        .expect("Expected first argument to be a port number");
    let addr = format!("127.0.0.1:{}", port).parse()?;
    println!("Deltio listening on {}", addr);

    make_server_builder().serve(addr).await?;

    println!("Deltio exiting");

    Ok(())
}
