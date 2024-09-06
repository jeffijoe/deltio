fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the PubSub gRPC service definitions.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .field_attribute(".", "#[allow(clippy::all)]")
        .compile(
            &["proto/googleapis/google/pubsub/v1/pubsub.proto"],
            &["proto/googleapis"],
        )?;

    // If we set CARGO_PKG_VERSION this way, then it will override the default value, which is
    // taken from the `version` in Cargo.toml.
    if let Ok(val) = std::env::var("DELTIO_RELEASE_VERSION") {
        println!("cargo:rustc-env=CARGO_PKG_VERSION={}", val);
    }
    println!("cargo:rerun-if-env-changed=DELTIO_RELEASE_VERSION");
    Ok(())
}
