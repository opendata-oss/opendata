# Local Ingest End-to-End Test (MinIO)

Verify the ingest pipeline end-to-end: ingestor writes OTLP
batches to MinIO, the timeseries consumer reads them, and the
metrics appear on `/metrics`.

## Prerequisites

- Docker (for MinIO)
- Rust toolchain

## Steps

### 1. Start MinIO

```bash
docker run -d --name minio-ingest-test \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data --console-address ":9001"

# Wait for startup, then create the bucket
sleep 3
docker exec minio-ingest-test mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio-ingest-test mc mb local/ingest-test
```

### 2. Build

```bash
cargo build -p opendata-timeseries --features "http-server,otel" --example ingest_produce
```

### 3. Write the config

Save to `/tmp/ingest-e2e-test/prometheus.yaml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs: []

otel:
  include_resource_attrs: true
  include_scope_attrs: true

storage:
  type: SlateDb
  path: data
  object_store:
    type: Local
    path: /tmp/ingest-e2e-test/tsdb-data

flush_interval_secs: 5

ingest_consumer:
  object_store:
    type: Aws
    region: us-east-1
    bucket: ingest-test
  manifest_path: ingest/manifest
  poll_interval: 500ms
```

```bash
mkdir -p /tmp/ingest-e2e-test/tsdb-data
```

### 4. Start the timeseries server

`AmazonS3Builder::from_env()` picks up `AWS_SESSION_TOKEN` /
`AWS_PROFILE` if set, so unset them first.

```bash
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE

AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_ENDPOINT=http://localhost:9000 \
AWS_ALLOW_HTTP=true \
AWS_REGION=us-east-1 \
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
RUST_LOG=info \
./target/debug/opendata-timeseries \
  --config /tmp/ingest-e2e-test/prometheus.yaml --port 9099
```

Confirm the log shows:
```
Ingest consumer started manifest_path=ingest/manifest poll_interval=500ms
```

### 5. Produce data

In a second terminal (same `unset` + env vars):

```bash
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE

AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_ENDPOINT=http://localhost:9000 \
AWS_ALLOW_HTTP=true \
AWS_REGION=us-east-1 \
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
./target/debug/examples/ingest_produce
```

Expected output:
```
batch 0 flushed
batch 1 flushed
batch 2 flushed
batch 3 flushed
batch 4 flushed
ingestor closed, 5 batches produced
```

### 6. Verify metrics

```bash
sleep 3
curl -s http://localhost:9099/metrics | grep ingest
```

Sample output (values will differ):

```
ingest_batches_collected 5
ingest_entries_collected 10
ingest_bytes_collected 920
ingest_acks 5
ingest_collector_lag_seconds 0.597
ingest_queue_length 5
ingest_fetch_duration_seconds_count 5
ingest_manifest_writes{role="consumer"} 1
```

Query the ingested data:

```bash
curl -s 'http://localhost:9099/api/v1/query?query=cpu_temperature'
```

### 7. Cleanup

```bash
# Stop the server (Ctrl-C)
docker rm -f minio-ingest-test
rm -rf /tmp/ingest-e2e-test
```

## Troubleshooting

**403 InvalidTokenId from MinIO**: Stale AWS credentials in the
shell. Run `unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE`
before starting the server and producer.

**Ingest consumer fails to start**: Check that the MinIO bucket
exists and that `manifest_path` in the config matches the
ingestor's `manifest_path`.
