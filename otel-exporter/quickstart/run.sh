#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="/tmp/opendata-otel-quickstart"

echo "=== OpenData OTel Exporter Quickstart ==="
echo ""

# Clean previous data
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# Ensure the cdylib is built
echo "Building Rust cdylib..."
cargo build --package opendata-ingest-bindings --release --manifest-path "$REPO_ROOT/Cargo.toml"

# Build the collector binary
echo "Building collector binary..."
cd "$REPO_ROOT/otel-exporter"
CGO_ENABLED=1 go build -o ./cmd/collector/opendata-collector ./cmd/collector/

echo ""
echo "Starting mock metrics server on :8080..."
python3 "$REPO_ROOT/timeseries/quickstart/mock_metrics_server.py" --port 8080 &
MOCK_PID=$!

# Give the mock server a moment to start
sleep 1

echo "Starting OTel Collector..."
echo "  Config: $SCRIPT_DIR/collector-config.yaml"
echo "  Data dir: $DATA_DIR"
echo ""
echo "Press Ctrl+C to stop."
echo ""

cleanup() {
    echo ""
    echo "Shutting down..."
    kill $MOCK_PID 2>/dev/null || true
    wait $MOCK_PID 2>/dev/null || true
    echo ""
    echo "Data written to: $DATA_DIR"
    echo "Inspect with: ls -la $DATA_DIR/"
}
trap cleanup EXIT

DYLD_LIBRARY_PATH="$REPO_ROOT/target/release" \
    ./cmd/collector/opendata-collector \
    --config "$SCRIPT_DIR/collector-config.yaml"
