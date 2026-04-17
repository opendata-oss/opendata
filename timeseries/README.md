# OpenData Timeseries

OpenData Timeseries is a Prometheus-compatible time series database built on
[SlateDB](https://www.slatedb.io), an object-store-native LSM tree. It
supports PromQL, Prometheus scraping, Prometheus remote write, OTLP metrics
ingest, and Grafana through the Prometheus data source.

Most Prometheus-compatible systems still rely on stateful storage nodes and the
operational work that comes with them. Timeseries takes the object-store-native
route instead: durable state lives in object storage, local disks are caches,
and you can separate the writer from the reader without building around a
replicated TSDB cluster.

For development, that means you can run a single server locally and work
against the same API surface you would use in production.

## Quickstart

Use the docs quickstart for the fastest path from install to querying real
metrics:

- [Timeseries quickstart](https://www.opendata.dev/docs/timeseries/quickstart)

That guide installs Timeseries locally, scrapes metrics, walks through the
built-in query UI, and shows how to add an external target.

## Local Development

Run the server directly from source:

```bash
cargo run -p opendata-timeseries --features remote-write -- --port 9090
```

To enable OTLP/HTTP metrics ingest (`POST /v1/metrics`), start the server with
the `otel` feature and configure OTEL label handling in `prometheus.yaml`:

```bash
cargo run -p opendata-timeseries --features "http-server,otel" -- --config ./prometheus.yaml --port 9090
```

```yaml
otel:
  include_resource_attrs: true
  include_scope_attrs: true
```

## Learn More

- [Introducing OpenData Timeseries](https://www.opendata.dev/blog/introducing-timeseries)
- [Storage design](rfcs/0001-tsdb-storage.md)
- [Write API](rfcs/0002-write-api.md)

## Roadmap

- [ ] Full PromQL compatibility
- [ ] Full Prometheus API compatibility
- [ ] Retention policies and rollups
- [ ] Bucket compaction
- [ ] Recording rules
- [ ] Service discovery for targets
- [ ] OTLP export protocol ingestion
- [ ] Log-native ingestion
- [ ] Read-only clients
