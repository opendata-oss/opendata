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

# Build the vector binary with http-server feature in release mode
# Use --locked to ensure reproducible builds from Cargo.lock
RUN cargo build --release --locked --manifest-path vector/Cargo.toml --features http-server

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
COPY --from=builder /app/target/release/opendata-vector /app/opendata-vector

# Create a non-root user
RUN useradd -r -u 1000 opendata-vector
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
