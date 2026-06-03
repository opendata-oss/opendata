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
streams, agent transcripts) where each key is **actively followed** by a consumer
that tracks a cursor and tails it. Following is **closed-loop**: a reader issues a
poll, waits for the response, and polls again — immediately if a full page came
back (a backlog remains, so drain as fast as possible), or after `idle_poll_interval`
once caught up to the tail. The idle delay is the only polling cadence and keeps
caught-up followers from busy-spinning empty reads.

A run proceeds through three phases over one live database:

1. **Pre-fill** — append a per-key backlog (parallel + batched); reports ingest
   throughput.
2. **Warm-up** — run arrivals and follows for `warmup_secs`, discarding metrics
   while the cache reaches steady state.
3. **Measure** — record everything for the `--duration` window.

Poll latency and service time are bucketed by **lag at poll time** (the lag bucket
is carried as a `lag` metric label) — the analysis axis the read cost is reported
against. Lag itself is a workload consequence (how far a follower fell behind), not
a backend result. The console summary reports throughput, the lag distribution
(`polls_lag_*`), residual backlog, pre-fill ingest throughput, and the
measure-phase ingest throughput (`records_per_sec` / `bytes_per_sec`, the
offered write load actually achieved); the keeping-up
signal is the lag distribution shifting into deeper buckets (with `scheduling_lag_us`
growing) when runners can't keep followers near the tail. Per-bucket latency
percentiles require a configured `[reporter]`.

> GETs/poll (the RFC's LogDb cost metric) is **not yet recorded** — object-store
> GET counting is deferred to a later milestone.

### Parameters

Workload (backend-agnostic):

| Param | Meaning |
|-------|---------|
| `key_cardinality` | number of independently followed logs |
| `key_length` | width of the zero-padded key strings |
| `value_size` | record payload size in bytes |
| `page_size` | max records returned per poll (bounds per-poll cost; a backlog drains over several back-to-back polls) |
| `arrival_rate_per_key` | per-key append rate (records/s); aggregate is `cardinality × this` |
| `idle_poll_interval_ms` | delay before a caught-up follower re-polls (the only polling cadence) |

Phase / harness sizing:

| Param | Meaning |
|-------|---------|
| `prefill_per_key` | records pre-loaded per key before measurement |
| `prefill_concurrency` | parallel tasks used during pre-fill |
| `warmup_secs` | warm-up window (metrics discarded) |
| `reader_concurrency` | number of follower runners (the read concurrency the db sees; keys partitioned round-robin, one poll in flight per runner) |
| `num_writer_tasks` | arrivals writer tasks |

### Scenarios

- **A — Cardinality scaling** (`configs/scenario-a-cardinality.toml`): scale
  cardinality 1K → 1M at a fixed aggregate load (per-key arrival rate scaled down
  and `idle_poll_interval` scaled up with cardinality); per-poll latency and
  throughput should stay approximately flat. The high-cardinality points are long
  runs (set `--duration` to several idle intervals of the largest point). Example:

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
