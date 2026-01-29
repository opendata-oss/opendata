# Use cargo-chef for dependency caching
FROM lukemathwalker/cargo-chef:latest-rust-1.89 AS chef
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

# Build the timeseries binary with remote-write feature in release mode
RUN cargo build --release --manifest-path timeseries/Cargo.toml --features remote-write

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder stage
COPY --from=builder /app/target/release/timeseries /app/timeseries

# Create a non-root user
RUN useradd -r -u 1000 timeseries
USER timeseries

# Expose Prometheus-compatible port
EXPOSE 9090

# Environment variables for configuration
ENV OPEN_TSDB_PORT=9090
ENV RUST_LOG=info

# Run the server
ENTRYPOINT ["/app/timeseries"]
