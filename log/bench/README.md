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
- **burst** - Measures append throughput under a high-cardinality, bursty write
  pattern: a large key population (`num_keys`) of which only a small, drifting
  active set (`active_keys`) is written at any moment, each active key receiving
  a fixed `burst_size` run of records before churning out. Reproducible via
  `seed`. Set `active_keys == num_keys` to recover the flat `ingest` baseline.
