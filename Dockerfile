FROM --platform=$BUILDPLATFORM rust:1.69 as build

# Install Protocol Buffers.
RUN apt-get update && apt-get install -y protobuf-compiler musl-tools musl-dev

# Create a new empty project.
RUN cargo new --bin deltio
WORKDIR /deltio

# The target platform we are compiling for.
# Populated by BuildX
ARG TARGETPLATFORM

# The build platform we are compiling on.
# Populated by BuildX
ARG BUILDPLATFORM

# Install the required cross-compiler toolchain based on the target platform.
# Basically, if the target platform is ARM, then we'll need 
# the `gcc-arm-linux-gnueabihf` linker, otherwise we'll need
# the `gcc-multilib`.
# IMPORTANT: This only seems to work on a x86_64 Linux build platform. 
RUN <<EOF
  set -e;

  # This is the file we will be writing the compilation target to for
  # subsequent steps.
  touch .target
  
  if [ "$TARGETPLATFORM" = "linux/arm64" ]; then
    # musl-cross isn't available via apt-get, so have to download and install it manually.
    mkdir /opt/musl-cross
    wget -P /opt/musl-cross https://musl.cc/aarch64-linux-musl-cross.tgz
    tar -xvf /opt/musl-cross/aarch64-linux-musl-cross.tgz -C "/opt/musl-cross"
    rustup target add aarch64-unknown-linux-musl
    echo -n "aarch64-unknown-linux-musl" > .target
  else
    if [ "$TARGETPLATFORM" = "linux/amd64" ]; then
      rustup target add x86_64-unknown-linux-musl
      echo -n "x86_64-unknown-linux-musl" > .target
    elif [ "$TARGETPLATFORM" = "linux/386" ]; then
      rustup target add i686-unknown-linux-musl
      echo -n "i686-unknown-linux-musl" > .target
    fi
  fi
EOF

# In case we installed the musl-cross tools, add it to the path.
ENV PATH="/opt/musl-cross/aarch64-linux-musl-cross/bin:${PATH}"

# Copy manifests.
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Build the shell project first to get a dependency cache.
RUN <<EOF
  set -e;

  # If the build platform is the same as the target platform, we don't
  # need to use any target.
  TARGET=$(cat .target)
  if [ -z "$TARGET" ]; then
    cargo build --release
    rm ./target/release/deps/deltio*
  else
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
  # need to use any target.
  TARGET=$(cat .target)
  if [ -z "$TARGET" ]; then
    cargo build --release
    exit 0
  fi

  cargo build --target "$TARGET" --release
  mv "target/$TARGET/release/deltio" "target/release/deltio"
EOF

# Our final base image.
FROM --platform=$TARGETPLATFORM scratch as deltio

# Copy the build artifact from the build stage
COPY --from=build /deltio/target/release/deltio .

# Expose the default port.
EXPOSE 8085

# Set the startup command to run the binary.
CMD ["./deltio", "--bind", "0.0.0.0:8085"]
