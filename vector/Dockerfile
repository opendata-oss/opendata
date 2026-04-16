# Use cargo-chef for dependency caching
# Pin to multi-arch manifest list digest for reproducible builds (supports amd64/arm64)
# To update: docker manifest inspect lukemathwalker/cargo-chef:latest-rust-1.94
FROM lukemathwalker/cargo-chef:latest-rust-1.94@sha256:d5a1cca12f21de999e5b221b5c6ff5635080f4b8d5e58053241ff169e0fc60f6 AS chef
WORKDIR /app

FROM chef AS planner
# Copy the entire workspace since this is a workspace member
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source code
COPY . .

# Build the vector binary with http-server feature in release mode
# Use --locked to ensure reproducible builds from Cargo.lock
RUN cargo build --release --locked --manifest-path vector/Cargo.toml --features http-server

# Runtime stage
# Pin to multi-arch manifest list digest for reproducible builds (supports amd64/arm64)
# To update: docker buildx imagetools inspect debian:trixie-slim
FROM debian:trixie-slim@sha256:4ffb3a1511099754cddc70eb1b12e50ffdb67619aa0ab6c13fcd800a78ef7c7a

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder stage
COPY --from=builder /app/target/release/opendata-vector /app/opendata-vector

# Create a non-root user and prepare the data directory for volume mounts
RUN useradd -r -u 1000 opendata-vector \
    && mkdir -p /data/store \
    && chown opendata-vector:opendata-vector /data/store
USER opendata-vector

# Expose HTTP port
EXPOSE 8080

# Environment variables for documentation/convention
ENV VECTOR_PORT=8080
ENV VECTOR_CONFIG=/config/vector.yaml
ENV RUST_LOG=info

# Default to read-write server mode with a mounted config file.
# Override the command to run reader mode or to use a different config path.
ENTRYPOINT ["/app/opendata-vector"]
CMD ["vector", "--config", "/config/vector.yaml"]
