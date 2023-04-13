use deltio::make_server_builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Deltio starting");

    let addr = "[::1]:8086".parse()?;
    make_server_builder().serve(addr).await?;

    println!("Deltio exiting");

    Ok(())
}
