FROM --platform=$BUILDPLATFORM rust:1.69 as build

# Install Protocol Buffers.
RUN apt-get update && apt-get install -y protobuf-compiler

# Create a new empty project.
RUN cargo new --bin deltio
WORKDIR /deltio

# The target platform we are compiling for.
# Populated by BuildX
ARG BUILDPLATFORM
ARG TARGETPLATFORM

# Install the required cross-compiler toolchain based on the target platform.
# Basically, if the target platform is ARM, then we'll need 
# the `gcc-arm-linux-gnueabihf` linker, otherwise we'll need
# the `gcc-multilib`.
# IMPORTANT: This only seems to work on a x86_64 Linux build platform. 
RUN <<EOF
  set -e;
  
  # If the build platform is the same as the target platform, we don't
  # need any more packages.
  if [ "$BUILDPLATFORM" = "$TARGETPLATFORM" ]; then
    echo "Build and target platform are the same, won't install extra stuff"
    exit 0
  fi

  echo "Build platform: $BUILDPLATFORM"
  echo "Target platform: $TARGETPLATFORM"
  apt-get update
  if [ "$TARGETPLATFORM" = "linux/arm64" ]; then
    apt-get install -y gcc-aarch64-linux-gnu
    rustup target add aarch64-unknown-linux-gnu
    echo -n "aarch64-unknown-linux-gnu" > .target
  else
    apt-get install -y gcc-multilib
    if [ "$TARGETPLATFORM" = "linux/amd64" ]; then
      rustup target add x86_64-unknown-linux-gnu
      echo -n "x86_64-unknown-linux-gnu" > .target
    elif [ "$TARGETPLATFORM" = "linux/386" ]; then
      rustup target add i686-unknown-linux-gnu
      echo -n "i686-unknown-linux-gnu" > .target
    fi
  fi
EOF

# Copy manifests.
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Build the shell project first to get a dependency cache.
RUN <<EOF
  set -e;

  # If the build platform is the same as the target platform, we don't
  # to use any target.
  if [ "$BUILDPLATFORM" = "$TARGETPLATFORM" ]; then
    cargo build --release
    rm ./target/release/deps/deltio*
  else
    TARGET=$(cat .target)
    cargo build --target "$TARGET" --release
    rm ./target/*/release/deps/deltio*
  fi

  # Remove the shell project's code files.
  rm src/*.rs
EOF

# Copy the actual source.
COPY ./build.rs ./build.rs
COPY ./proto ./proto
COPY ./src ./src

# Build for release
RUN <<EOF
  set -e;
  # If the build platform is the same as the target platform, we don't
  # to use any target.
  if [ "$BUILDPLATFORM" = "$TARGETPLATFORM" ]; then
    cargo build --release
    exit 0
  fi

  TARGET=$(cat .target)
  cargo build --target "$TARGET" --release
  mv "target/$TARGET/release/deltio" "target/release/deltio"
EOF

# Our final base image.
FROM --platform=$TARGETPLATFORM debian:buster-slim as deltio

# Copy the build artifact from the build stage
COPY --from=build /deltio/target/release/deltio .

# Expose the default port.
EXPOSE 8085

# Set the startup command to run the binary.
ENV RUST_LOG=info
CMD ["./deltio", "--bind", "0.0.0.0:8085"]
