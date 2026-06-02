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

- **ingest** — Measures log append throughput with various batch sizes and value sizes.
- **follow** — RFC 0006 cardinality benchmark: many independent per-key logs, each
  followed by its own consumer polling for what is new since it last read. Measures
  poll latency and throughput as a function of consumer lag and key cardinality.

## follow

Models a large population of key-addressable logs (mailboxes, per-entity event
streams, agent transcripts) where each key has a consumer that tracks a cursor and
polls for records appended since it last read. Polls are scheduled **open-loop**
(each consumer on its own cadence, independent of how long a poll takes), so the
results expose queueing and saturation honestly.

A run proceeds through three phases over one live database:

1. **Pre-fill** — append a per-key backlog (parallel + batched); reports ingest
   throughput.
2. **Warm-up** — run arrivals and polls for `warmup_secs`, discarding metrics while
   the cache reaches steady state.
3. **Measure** — record everything for the `--duration` window.

Poll latency and service time are bucketed by **lag at poll time** (the lag bucket
is carried as a `lag` metric label), which is the analysis axis the read cost is
reported against. Lag itself is a workload condition (set by the polling cadence),
not a backend result. The console summary reports throughput, the lag distribution
(`polls_lag_*`), the **dispatch drop fraction** (the keeping-up signal — drops
happen only when the bounded execution pool is full), and pre-fill ingest
throughput. Per-bucket latency percentiles require a configured `[reporter]`.

> GETs/poll (the RFC's LogDb cost metric) is **not yet recorded** — object-store
> GET counting is deferred to a later milestone.

### Parameters

Workload (backend-agnostic):

| Param | Meaning |
|-------|---------|
| `key_cardinality` | number of independently followed logs |
| `key_length` | width of the zero-padded key strings |
| `value_size` | record payload size in bytes |
| `page_size` | max records returned per poll (bounds per-poll cost; deep catch-up drains over several polls) |
| `arrival_rate_per_key` | per-key append rate (records/s); aggregate is `cardinality × this` |
| `poll_interval_mean_ms` | mean of the exponential inter-poll interval (Poisson polling) |
| `offline_prob` / `offline_duration_ms` | probability a poll is replaced by an offline gap, and its length |
| `seed` | seeds the workload PRNG, so the cadence is reproducible |

Phase / harness sizing:

| Param | Meaning |
|-------|---------|
| `prefill_per_key` | records pre-loaded per key before measurement |
| `prefill_concurrency` | parallel tasks used during pre-fill |
| `warmup_secs` | warm-up window (metrics discarded) |
| `num_scheduler_shards` | open-loop scheduler shards (keys partitioned round-robin) |
| `exec_concurrency` | bounded execution-pool size (the concurrency the db sees) |
| `num_writer_tasks` | arrivals writer tasks |
| `dispatch_queue_capacity` | bounded dispatch queue depth before polls are dropped |

### Scenarios

- **A — Cardinality scaling** (`configs/scenario-a-cardinality.toml`): scale
  cardinality 1K → 1M at a fixed aggregate load; per-poll latency and throughput
  should stay approximately flat. The high-cardinality points are long runs (set
  `--duration` to several poll intervals of the largest point). Example:

  ```sh
  cargo run -p log-bench --release -- \
      -c log/bench/configs/scenario-a-cardinality.toml -b follow -d 600
  ```

Scenarios B (lag asymmetry) and C (seal-size) are deferred until GET counting
lands, since their headline result is GETs/poll.

A quick smoke run uses the built-in default parameters (1K keys, short windows):

```sh
cargo run -p log-bench --release -- -b follow -d 5
```
