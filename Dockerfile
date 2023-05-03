FROM rust:1.69 as build

# Install Protocol Buffers.
RUN apt-get update && apt-get install -y protobuf-compiler

# Create a new empty project.
RUN cargo new --bin deltio
WORKDIR /deltio

# Copy manifests.
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Build the shell project first to get a dependency cache.
RUN cargo build --release

# Remove the shell project's code files.
RUN rm src/*.rs

# Copy the actual source.
COPY ./build.rs ./build.rs
COPY ./proto ./proto
COPY ./src ./src

# Build for release
RUN rm ./target/release/deps/deltio*
RUN cargo build --release

# Our final base image.
FROM rust:1.69

# Copy the build artifact from the build stage
COPY --from=build /deltio/target/release/deltio .

# Expose the default port.
EXPOSE 8085

# Set the startup command to run the binary.
CMD ["./deltio", "8085"]
