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

# Build the log binary with http-server feature in release mode
RUN cargo build --release -p log --features http-server

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder stage
COPY --from=builder /app/target/release/log /app/opendata-log

# Create a non-root user
RUN useradd -r -u 1000 opendata-log
USER opendata-log

# Expose HTTP port
EXPOSE 8080

# Environment variables for configuration
ENV LOG_PORT=8080
ENV LOG_DATA_DIR=/data
ENV RUST_LOG=info

# Run the server
ENTRYPOINT ["/app/opendata-log"]
