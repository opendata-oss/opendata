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

### Overriding the full LogDb config

The `data.storage` section above produces a LogDb config with default
segmentation, retention, compaction, and read-visibility settings. To override
those, point the bench at a YAML file containing a complete `log::Config` via
the `log_config` param. When set, the file is the source of truth — its
`storage` is used as-is and `data.storage` is ignored.

```toml
[[params.ingest]]
batch_size = "100"
value_size = "256"
key_length = "16"
num_keys = "10"
log_config = "log.yaml"
```

Example `log.yaml`:

```yaml
storage:
  type: SlateDb
  path: bench-data
  object_store:
    type: Local
    path: .data
segmentation:
  seal_interval: 3600000
read_visibility: remote
compaction:
  min_l0_per_compaction: 4
  max_l0_per_compaction: 8
```

## Benchmarks

- **ingest** - Measures log append throughput with various batch sizes and value sizes
