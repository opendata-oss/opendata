# Use cargo-chef for dependency caching
# Pin to multi-arch manifest list digest for reproducible builds (supports amd64/arm64)
# To update: docker manifest inspect lukemathwalker/cargo-chef:latest-rust-1.89
FROM lukemathwalker/cargo-chef:latest-rust-1.89@sha256:abbe80c8000f4e1b6969b4d84d5ec7ad86616be7e6322ba0e3b451c2eee6f280 AS chef
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

# Build the log binary with http-server feature in release mode
# Use --manifest-path to disambiguate from the crates.io log crate
# Use --locked to ensure reproducible builds from Cargo.lock
RUN cargo build --release --locked --manifest-path log/Cargo.toml --features http-server

# Runtime stage
# Pin to multi-arch manifest list digest for reproducible builds (supports amd64/arm64)
# To update: docker manifest inspect debian:bookworm-slim
FROM debian:bookworm-slim@sha256:56ff6d36d4eb3db13a741b342ec466f121480b5edded42e4b7ee850ce7a418ee

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder stage
COPY --from=builder /app/target/release/opendata-log /app/opendata-log

# Create a non-root user
RUN useradd -r -u 1000 opendata-log
USER opendata-log

# Expose HTTP port
EXPOSE 8080

# Environment variables for configuration
ENV LOG_PORT=8080
ENV LOG_DATA_DIR=/data
ENV RUST_LOG=info

# Note: No Docker HEALTHCHECK defined here because:
# 1. The port is configurable via LOG_PORT, and Docker HEALTHCHECK doesn't support env var interpolation
# 2. Kubernetes liveness/readiness probes are the primary health check mechanism in production
# For local Docker testing, use: docker run --health-cmd="curl -f http://localhost:${LOG_PORT}/-/healthy"

# Run the server
ENTRYPOINT ["/app/opendata-log"]
