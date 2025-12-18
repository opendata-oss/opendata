# OpenTSDB

OpenTSDB is a time series database with prometheus-like APIs which uses SlateDB
as the underlying storage engine.

## Quickstart

This quickstart runs OpenTSDB locally, scraping metrics from a mock server and
storing them in a local SlateDB instance on disk. You can then query the data
using the Prometheus-compatible API.

### Prerequisites

- Python 3 (for the mock metrics server)
- Rust/Cargo (for building OpenTSDB)

### 1. Start the Mock Metrics Server

In a terminal, start the mock server which exposes simulated metrics (CPU usage,
memory, request counts, histograms, etc.) in Prometheus text format:

```bash
python3 timeseries/etc/mock_metrics_server.py --port 8080
```

### 2. Start OpenTSDB

In another terminal, start OpenTSDB with the provided config. This tells OpenTSDB
to scrape the mock server every 15 seconds. Data is persisted to a local SlateDB
instance in the `./.data` directory (created automatically):

```bash
cargo run -p timeseries -- --config timeseries/etc/prometheus.yaml --port 9090
```

### 3. Query the Data

Wait about 15-30 seconds for OpenTSDB to scrape and index the metrics, then query
the data using curl.

**List all label names:**

```bash
curl 'http://localhost:9090/api/v1/labels' | jq .
```

**Get values for a specific label:**

```bash
curl 'http://localhost:9090/api/v1/label/job/values' | jq .
```

**Instant query (current value of a metric):**

```bash
curl 'http://localhost:9090/api/v1/query?query=mock_uptime_seconds' | jq .
```

**Range query (values over the last 5 minutes):**

```bash
curl "http://localhost:9090/api/v1/query_range?query=mock_cpu_usage_percent&start=$(date -v-5M +%s)&end=$(date +%s)&step=15s" | jq .
```

### Cleanup

To start fresh, stop the servers and delete the `./.data` directory.

## Storage Design

OpenTSDB uses SlateDB's LSM tree to store time series data organized into time
buckets. Each bucket contains indexes for efficient label-based querying
(inverted index mapping label/value pairs to series IDs) and Gorilla-compressed
time series samples. For detailed storage specifications, see [RFC 0001: TSDB
Storage](rfcs/0001-tsdb-storage.md).
