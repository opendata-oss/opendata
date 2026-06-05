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
- **follow** — RFC 0006 cardinality benchmark: many independent per-key logs
  followed by a population of **sessions** that wake, catch a log up to its tail,
  and sleep. Measures how many concurrent sessions a store sustains, and per-poll
  read cost, as key cardinality scales.

## follow

Models a large population of key-addressable logs (mailboxes, per-entity event
streams, agent transcripts) followed not by permanent per-key consumers but by
**sessions**. A session is one reader waking for one log: it picks up where the
last session on that key left off (the per-key cursor persists across sessions, as
a mailbox remembers what you last read) and polls it for a while.

The session itself is **bare bones** — a poll loop bounded by a duration:

```text
while within session_duration: poll(key, cursor, page_size); wait poll_interval
```

There is no catch-up / tail detection: a poll drains up to `page_size`, and the
session just keeps polling until `session_duration` elapses. Richer behavior
(end-on-catch-up, idle backoff, deep-catch-up handling) is intentionally left out,
to be built up as the benchmark needs it.

The model is **open-loop across the population, closed-loop within a session**:

- **Session arrivals** are a global open-loop Poisson stream at `session_rate`,
  each picking a **uniform-random** key. Open-loop means sessions wake on schedule
  regardless of how busy the database is — a slow database shows up as session
  scheduling lag, not as suppressed load (no coordinated omission).
- **One session per key** is enforced: if an arrival lands on a key that already has
  an active session, it is dropped and counted (`skipped_busy_sessions`) rather than
  doubling up. So occupancy is "distinct keys being read right now," each key's
  cursor has a single reader at a time, and realized rate = `session_rate` − skips −
  refusals.
- **Within a session**, polling is closed-loop: a real reader blocks on each
  response before issuing the next poll.
- Each arrival executes on a bounded pool of `max_active_sessions` slots — the read
  concurrency the database sees. This is the point of the model: **the number of
  concurrently active sessions can meet a target bound even as the key population
  grows to millions.** Time waiting for a slot is the session's scheduling lag.

`session_duration = -1` polls forever; continuous following is that case with
`max_active_sessions ≥ key_cardinality` and `poll_interval = 0`, so every key
acquires a permanent session.

A run proceeds through three phases over one live database:

1. **Pre-fill** — append a per-key backlog (parallel + batched); reports ingest
   throughput.
2. **Warm-up** — run arrivals and sessions for `warmup_secs`, discarding metrics
   while the cache reaches steady state.
3. **Measure** — record everything for the `--duration` window.

**"Sustain"** is the active-session occupancy at the **aggregate-goodput knee**:
sweep `session_rate` up at fixed cardinality; aggregate read goodput climbs with
occupancy, then plateaus as sessions contend (per-session goodput falling as
~1/occupancy). The occupancy at the knee is the sustainable number of concurrent
sessions. Session scheduling lag diverging is the hard-saturation backstop.

The console summary reports:

- **Occupancy** — `mean_active_sessions` (time-integral of active sessions ÷
  elapsed, i.e. Little's law) and `peak_active_sessions`. The headline "how many
  sessions can we sustain" number.
- **Aggregate goodput** — `aggregate_goodput_records_per_sec` / `_bytes_per_sec`
  (records drained across all sessions). The knee axis.
- **Per-session progress, bucketed by backlog at session start** — `db_rate_*`
  (records/s over DB-poll time, the contention signal: as sessions contend each poll
  takes longer and this drops), plus `sessions_backlog_*` and
  `avg_polls_per_session_backlog_*`. Backlog buckets reuse the lag-bucket boundaries.
  Bucketing by backlog removes the fixed-overhead confound (a near-tail session
  draining a handful of records shows a low rate from scan setup, not contention).
- **Per-poll service latency** — `service_us_lag_<bucket>_p50/p90/p99/max`
  (microseconds), bucketed by lag at poll time (the RFC read-cost axis). Comparing
  the same bucket across cardinalities shows whether per-poll cost stays flat.
- **Saturation** — `session_sched_lag_us_*` (scheduled-arrival → slot-acquisition),
  `refused_sessions` (overflowed the in-flight backstop), and `skipped_busy_sessions`
  (arrivals dropped because the key already had an active session).
- **Object-store GET activity** — `object_store_gets`, **`gets_per_poll`** (the
  RFC's LogDb cost metric), `get_bytes_total`.
- Pre-fill and measure-phase ingest throughput (`records_per_sec` / `bytes_per_sec`,
  the offered write load actually achieved), the per-poll lag distribution
  (`polls_lag_*`), and `residual_backlog_records` — kept only as a drain
  sanity-check, **not** a health signal (it is dominated by idle keys that no
  session visited).

The fuller per-bucket histogram set (`poll_service_us`, `session_sched_lag_us`)
still requires a configured `[reporter]`.

> GET counts are process-global over the measure window: the writer's poll-path
> reads (when they miss the in-memory state — e.g. under `read_visibility=remote`)
> plus its background compaction reads. Per-component GET attribution and cache
> hit/miss are a later refinement.

### Parameters

Workload (backend-agnostic):

| Param | Meaning |
|-------|---------|
| `key_cardinality` | number of independently followed logs |
| `key_length` | width of the zero-padded key strings |
| `value_size` | record payload size in bytes |
| `page_size` | max records returned per poll (bounds per-poll cost; a backlog drains over several back-to-back polls within a session) |
| `arrival_rate_per_key` | per-key append rate (records/s); aggregate is `cardinality × this` |
| `session_rate` | aggregate rate (sessions/s) of the global open-loop arrival stream; each session picks a uniform-random key |
| `max_active_sessions` | bounded session pool size — the read concurrency the db sees, and the ceiling on active sessions regardless of cardinality |
| `session_duration_ms` | how long a session keeps polling, from session start (default `100`); `-1` polls forever (with a pool ≥ cardinality and `poll_interval = 0`, reduces to continuous following) |
| `poll_interval_ms` | gap between successive polls within a session (default `0` = poll back-to-back) |
| `seed` | PRNG seed for arrivals + key selection (default `1`); workload is seed-reproducible |
| `max_inflight_sessions` | overload backstop on queued-but-not-yet-running sessions (default `16 × max_active_sessions`); overflow counts as `refused_sessions` |

Phase / harness sizing:

| Param | Meaning |
|-------|---------|
| `prefill_per_key` | records pre-loaded per key before measurement |
| `prefill_concurrency` | parallel tasks used during pre-fill |
| `warmup_secs` | warm-up window (metrics discarded) |
| `num_writer_tasks` | arrivals writer tasks |

Read path (optional):

| Param | Meaning |
|-------|---------|
| `read_visibility` | `memory` (default) serves polls from the writer's in-memory state as soon as writes land; `remote` restricts reads to object-store-durable data, forcing the poll path through the object store — the lever for exercising real GET / read-amplification cost on a single node |

> Polls are always served by the writer's own `LogDb` handle (a single node serves
> many concurrent scans). An earlier version could fan polls out to a pool of
> independent `LogDbReader`s over the shared object store; that was removed, since on
> one node a separate reader pool just multiplies cache/GET work without modeling
> anything the writer's own reads don't already. Under `read_visibility=remote`,
> note that reads only see object-store-durable data, so live arrivals are not
> visible until the writer's next flush; tunable flush cadence is a later milestone.

### Scenarios

- **A — Cardinality scaling** (`configs/scenario-a-cardinality.toml`): scale
  cardinality 1K → 1M at a **fixed** `session_rate` and fixed aggregate write rate
  (per-key arrival scaled down with cardinality). Because the backlog a session
  wakes with is `aggregate_writes / session_rate` — independent of cardinality —
  the sustainable occupancy (`mean_active_sessions`) and per-session progress
  should stay approximately flat as the population grows, while the wall-clock gap
  between a key's visits widens (the segment-fan-out effect on `gets_per_poll`).
  `max_active_sessions` is set generously so the knee, not the ceiling, governs.
  The high-cardinality points are long runs (a key is visited rarely, so run long
  enough to accumulate sessions per backlog bucket). Example:

  ```sh
  cargo run -p log-bench --release -- \
      -c log/bench/configs/scenario-a-cardinality.toml -b follow -d 600
  ```

  To find the knee directly, fix cardinality and sweep `session_rate` up until
  `aggregate_goodput_records_per_sec` plateaus and `session_sched_lag_us` diverges.

Scenarios B (lag asymmetry) and C (seal-size) are deferred until GET counting
lands, since their headline result is GETs/poll.

A quick smoke run uses the built-in default parameters (1K keys, short windows):

```sh
cargo run -p log-bench --release -- -b follow -d 5
```
