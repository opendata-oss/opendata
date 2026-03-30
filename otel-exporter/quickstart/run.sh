#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-opendata-ingest}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"

echo "=== OpenData OTel Exporter Quickstart ==="
echo ""

# Ensure MinIO is running
if ! curl -sf "$MINIO_ENDPOINT/minio/health/live" > /dev/null 2>&1; then
    echo "Starting MinIO via docker..."
    docker rm -f minio-otel-test 2>/dev/null || true
    docker run -d --name minio-otel-test \
        -p 9000:9000 -p 9001:9001 \
        -e MINIO_ROOT_USER="$MINIO_USER" \
        -e MINIO_ROOT_PASSWORD="$MINIO_PASS" \
        minio/minio server /data --console-address ":9001"
    sleep 2
    docker exec minio-otel-test mc alias set local http://localhost:9000 "$MINIO_USER" "$MINIO_PASS"
    docker exec minio-otel-test mc mb --ignore-existing local/"$MINIO_BUCKET"
    echo "MinIO started at $MINIO_ENDPOINT (console at http://localhost:9001)"
else
    echo "MinIO already running at $MINIO_ENDPOINT"
    docker exec minio-otel-test mc alias set local http://localhost:9000 "$MINIO_USER" "$MINIO_PASS" 2>/dev/null || true
    docker exec minio-otel-test mc mb --ignore-existing local/"$MINIO_BUCKET" 2>/dev/null || true
fi
echo ""

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
sleep 1

echo "Starting OTel Collector..."
echo "  Config: $SCRIPT_DIR/collector-config.yaml"
echo "  MinIO:  $MINIO_ENDPOINT/$MINIO_BUCKET"
echo ""
echo "Press Ctrl+C to stop."
echo ""

cleanup() {
    echo ""
    echo "Shutting down..."
    kill $MOCK_PID 2>/dev/null || true
    wait $MOCK_PID 2>/dev/null || true
    echo ""
    echo "Inspect data with:"
    echo "  docker exec minio-otel-test mc ls --recursive local/$MINIO_BUCKET/"
}
trap cleanup EXIT

AWS_ENDPOINT="$MINIO_ENDPOINT" \
AWS_ACCESS_KEY_ID="$MINIO_USER" \
AWS_SECRET_ACCESS_KEY="$MINIO_PASS" \
AWS_ALLOW_HTTP="true" \
DYLD_LIBRARY_PATH="$REPO_ROOT/target/release" \
    ./cmd/collector/opendata-collector \
    --config "$SCRIPT_DIR/collector-config.yaml"
