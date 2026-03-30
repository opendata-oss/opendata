#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-opendata-ingest}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"

echo "=== OpenData OTel Exporter End-to-End Quickstart ==="
echo ""
echo "Pipeline: Mock Metrics → OTel Collector → Ingest (MinIO) → Timeseries → PromQL"
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
fi
docker exec minio-otel-test mc alias set local http://localhost:9000 "$MINIO_USER" "$MINIO_PASS" 2>/dev/null
docker exec minio-otel-test mc mb --ignore-existing local/"$MINIO_BUCKET" 2>/dev/null
echo "MinIO running at $MINIO_ENDPOINT"

# Build Rust components
echo ""
echo "Building Rust components..."
cargo build --package opendata-ingest-bindings --release --manifest-path "$REPO_ROOT/Cargo.toml"
cargo build --package opendata-timeseries --features http-server,ingest-consumer --manifest-path "$REPO_ROOT/Cargo.toml"

# Build Go collector
echo "Building Go collector..."
cd "$REPO_ROOT/otel-exporter"
CGO_ENABLED=1 go build -o ./cmd/collector/opendata-collector ./cmd/collector/

PIDS=()

cleanup() {
    echo ""
    echo "Shutting down..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo ""
    echo "Inspect MinIO data:"
    echo "  docker exec minio-otel-test mc ls --recursive local/$MINIO_BUCKET/"
}
trap cleanup EXIT

# 1. Start mock metrics server
echo ""
echo "Starting mock metrics server on :8080..."
python3 "$REPO_ROOT/timeseries/quickstart/mock_metrics_server.py" --port 8080 &
PIDS+=($!)
sleep 1

# 2. Start OTel Collector (writes to MinIO via ingest)
echo "Starting OTel Collector (Prometheus receiver → OpenData exporter)..."
AWS_ENDPOINT="$MINIO_ENDPOINT" \
AWS_ACCESS_KEY_ID="$MINIO_USER" \
AWS_SECRET_ACCESS_KEY="$MINIO_PASS" \
AWS_ALLOW_HTTP="true" \
DYLD_LIBRARY_PATH="$REPO_ROOT/target/release" \
    ./cmd/collector/opendata-collector \
    --config "$SCRIPT_DIR/collector-config.yaml" 2>&1 | sed 's/^/  [collector] /' &
PIDS+=($!)
sleep 2

# 3. Start timeseries server (reads from MinIO via ingest consumer)
echo "Starting timeseries server on :9090 (ingest consumer → TSDB)..."
AWS_ENDPOINT="$MINIO_ENDPOINT" \
AWS_ACCESS_KEY_ID="$MINIO_USER" \
AWS_SECRET_ACCESS_KEY="$MINIO_PASS" \
AWS_ALLOW_HTTP="true" \
    "$REPO_ROOT/target/debug/opendata-timeseries" \
    --config "$SCRIPT_DIR/timeseries-config.yaml" \
    --port 9090 2>&1 | sed 's/^/  [timeseries] /' &
PIDS+=($!)

echo ""
echo "All components running. Waiting for data to flow..."
sleep 10

echo ""
echo "=== Querying timeseries for ingested metrics ==="
echo ""
echo "Instant query: mock_requests_total"
curl -s "http://localhost:9090/api/v1/query?query=mock_requests_total" | python3 -m json.tool 2>/dev/null || echo "(query returned no data yet)"

echo ""
echo "Instant query: mock_cpu_usage_percent"
curl -s "http://localhost:9090/api/v1/query?query=mock_cpu_usage_percent" | python3 -m json.tool 2>/dev/null || echo "(query returned no data yet)"

echo ""
echo "Press Ctrl+C to stop all components."
echo ""

# Wait forever until Ctrl+C
wait
