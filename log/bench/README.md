# log-bench

Benchmarks for the log database.

## Usage

Run all benchmarks:

```sh
cargo run -p log-bench
```

Run a specific benchmark:

```sh
cargo run -p log-bench -- -b ingest
```

Set benchmark duration (default: 5 seconds):

```sh
cargo run -p log-bench -- -d 10
```

Use a config file:

```sh
cargo run -p log-bench -- -c config.toml
```

## Configuration

Sample config for S3:

```toml
[data.storage]
type = "SlateDb"
path = "bench-data"

[data.storage.object_store]
type = "Aws"
region = "us-west-2"
bucket = "my-bucket"
```

## Benchmarks

- **ingest** - Measures log append throughput with various batch sizes and value sizes
- **scan** - Measures single-key catch-up scans (`scan(key, cursor..)`), comparing
  storage access paths over identical data

### scan

The scan benchmark prefills records to keys drawn from a seeded PRNG (so each
SST holds a random subset of the keyspace), then issues sequential scans of
distinct keys starting `scan_lag` entries behind each key's tail. It reports
latency percentiles and object-store GETs per scan, diffed around each scan
for exact attribution.

Each cell measures both access paths interleaved **over the same store**
(even sample slots scan with `range`, odd with `prefix`), reporting
`range_*` and `prefix_*` metrics side by side. Comparing paths across
separately prefilled cells does not work: per-scan cost is dominated by the
number of sources (L0 SSTs + sorted runs) in the segments the cursor
covers, and independently prefilled stores settle into different shapes
depending on ingest/seal/compaction timing.

- `range` — bounded range scan; the backend prunes SSTs/blocks by key-range
  metadata and starts at the cursor, but consults no bloom filters.
- `prefix` — prefix scan over `(segment, key)`; prefix bloom filters skip
  SSTs without the key, but reads start at the key's first entry in each
  segment regardless of the cursor.

Run it against a local-filesystem object store so the database is reopened
after prefill and scans run from a cold block cache:

```sh
cargo run --release -p log-bench -- -b scan -c log/bench/configs/scan-local.toml -d 120
```

`configs/scan-local.toml` points `settings_path` at
`configs/slatedb-small-sst.toml`, which lowers `l0_sst_size_bytes` so the
prefill spreads across many SSTs; run from the repo root so the relative
path resolves. With an in-memory object store (`configs/scan-smoke.toml`)
the data cannot survive a reopen, so scans stay on the warm handle and GET
counts read ~0; use that config only as a correctness smoke test.
