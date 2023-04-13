fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the PubSub gRPC service definitions.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &["src/proto/googleapis/google/pubsub/v1/pubsub.proto"],
            &["src/proto/googleapis"],
        )?;
    Ok(())
}
