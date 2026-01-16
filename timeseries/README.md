# OpenData Timeseries

OpenData Timeseries is a time series database with prometheus-like APIs which
uses SlateDB as the underlying storage engine.

## Quickstart

This quickstart runs Timeseries locally, scraping metrics from a mock server and
prometheus node exporter, and storing them in a local SlateDB instance on disk.
You can then query the data using the Prometheus-compatible API. It also runs
Grafana so that you can visualize the collected metrics.

### Prerequisites

- Docker and Docker Compose installed

### 1. Start the quickstart stack

```bash
$ docker compose -f ./quickstart/docker-compose.yml up -d --build
$ docker ps
CONTAINER ID   IMAGE                           COMMAND                  CREATED          STATUS                    PORTS                       NAMES
09cda30803cb   quickstart-timeseries-server    "./timeseries --conf…"   16 seconds ago   Up 15 seconds (healthy)   0.0.0.0:9090->9090/tcp      quickstart-timeseries-server-1
c404ef4b389a   grafana/grafana-oss:latest      "/run.sh"                16 seconds ago   Up 9 seconds              0.0.0.0:3001->3000/tcp      quickstart-grafana-1
b13121a0f5ab   quickstart-mock-metrics         "python3 mock_metric…"   16 seconds ago   Up 15 seconds (healthy)   0.0.0.0:8080->8080/tcp      quickstart-mock-metrics-1
af78174f4c32   prom/node-exporter:latest       "/bin/node_exporter …"   16 seconds ago   Up 15 seconds             0.0.0.0:9100->9100/tcp      quickstart-node-exporter-1
```

### 2. Query the Data

Wait about 15-30 seconds for Timeseries to scrape and index the metrics, then query
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
curl "http://localhost:9090/api/v1/query_range?query=node_cpu_seconds&start=$(date -v-5M +%s)&end=$(date +%s)&step=15s" | jq .
```

### 3. Open Grafana and Explore Metrics

Open the address `localhost:3001` in a browser and login with username `admin` and password `admin`. You can then navigate to `http://localhost:3001/a/grafana-metricsdrilldown-app/drilldown` to see an overview of all the scraped metrics. Navigate to `http://localhost:3001/d/node-exporter-overview/node-exporter-overview` to see an example dashboard that plots some metrics collected from the Prometheus node exporter.

### 4. Cleanup

```bash
$ docker compose -f ./quickstart/docker-compose.yml down
```

## Storage Design

Timeseries uses SlateDB's LSM tree to store time series data organized into time
buckets. Each bucket contains indexes for efficient label-based querying
(inverted index mapping label/value pairs to series IDs) and Gorilla-compressed
time series samples. For detailed storage specifications, see [RFC 0001: Timeseries
Storage](rfcs/0001-tsdb-storage.md).
