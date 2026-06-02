# RFC 0006: Log Cardinality Benchmark

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC defines a benchmark for LogDb built around the use cases it is designed to
serve: large populations of independent, key-addressable logs, each *followed* by its
own consumer at its own pace. The model is a mailbox — messages arrive throughout the
day, and each user polls now and then to catch up on what is new. The benchmark gives
every key a consumer that tracks a cursor and polls for records appended since it last
read; a distribution over polling frequency, plus occasional offline lag, produces a
realistic mix of caught-up tail reads and deeper catch-up reads.

The workload runs in real time under an open-loop scheduler, so its results are
directly interpretable: poll latency, sustainable throughput, and object-store GET
requests per poll — reported as a function of how far behind each consumer is. The two
primary knobs are the **number of keys** and the **distribution over polling
frequency**.

## Motivation

LogDb is designed for use cases that maintain many independent logs, each addressed by
a key and followed at its own pace — the dominant pattern is "show me what's new in
this one log since I last looked." These use cases recur across domains:

- **Mailboxes and notification feeds** — one log per user; clients poll periodically
  for new messages, and a client that was offline catches up on a backlog.
- **Event-sourced aggregates** — each aggregate instance is a log of events; consumers
  follow a single aggregate's stream to stay current.
- **User-centric histories and audit logs** — one log per user, account, or resource,
  tailed for recent activity.
- **IoT device timelines** — one log per device; a collector follows each device's
  stream of readings.
- **Per-session AI agent transcripts** — each session is its own log of steps, tool
  calls, and messages; a follower reconstructs or tails one session.

These workloads share a characteristic profile, and it is that profile the benchmark
sets out to stress:

- **High key cardinality** — thousands to millions of distinct logs.
- **Independent per-key consumers** — each log has its own follower with its own
  cursor; the breadth comes from the *population* of consumers, not from any single
  read spanning many keys.
- **Intermittent following with variable lag** — most consumers stay near the tail;
  some fall behind (slow cadence or offline) and catch up later. Recency is not
  imposed — it emerges from how consumers poll.
- **Append-only writes** spread across the whole key population.

LogDb fits this profile by construction. Sequences are global and segments partition
the log into sealed ranges (RFC 0002); within a segment, entries are keyed
`user_key | relative_seq` (RFC 0001), so a single key's records are contiguous within
each segment and group into dense, locality-friendly storage blocks. Compaction and
segmentation (RFC 0005) expose explicit knobs for trading read amplification against
write amplification.

The existing `ingest` benchmark covers the write path; this RFC covers the read path
under a workload representative of these use cases.

### The read-cost model

A poll is `scan(key, cursor..tail)` — read what has accumulated since the consumer
last caught up. Its cost has two levels of fan-out:

```
scan(key, cursor..tail) cost ≈
   Σ over segments overlapping [cursor, tail]   (segment fan-out)
        × per-segment cost to locate `key`       (block / sorted-run fan-out)
```

- **Segment fan-out** is governed by `seal_size` versus how far the consumer is behind.
  Because sequences are global, a key's records are spread across every segment that
  was live during the key's activity, so a deeper catch-up touches more segments.
- **Per-segment fan-out** is governed by compaction state. Within a segment, a key's
  records are contiguous; the cost is how many blocks / sorted runs must be consulted
  to reach them.

The workload fits the LSM shape, and a consumer's **lag** — how far its cursor trails
the tail — places its reads on opposite ends of it:

- **Caught-up consumers** read the small, moving tail. Per-poll read amplification is
  poor — a poll for one key's handful of new records fetches a whole block of many
  keys. But all caught-up consumers read the *same* recent tail blocks, so the cache
  absorbs them and amortized GETs/poll collapses. Caching dominates.
- **Lagged consumers** do catch-up reads spanning many older segments. Caching helps
  little (each touches different cold blocks), but those segments are fully compacted
  into dense blocks, so read amplification is close to ideal — roughly one GET per
  block of relevant data. A bounded page size spreads a deep catch-up over several
  polls.

The distribution of lag — set by the polling-frequency distribution and offline
behavior — determines the mix of these two regimes. Making that asymmetry visible, and
letting us tune it, is the point of the benchmark.

## Goals

- **Demonstrate high-cardinality following.** Show that per-poll latency and cost stay
  approximately flat as the number of independently followed logs scales (e.g. 1K →
  1M). This is the headline result: a log stays cheap to follow as the population
  grows.
- **Make the lag asymmetry measurable.** Report poll latency and GETs/poll as a
  function of consumer lag, so the caught-up-cheap (cache-dominated) vs.
  deep-catch-up-near-ideal (compaction-dominated) behavior is explicit.
- **Report interpretable, real-time metrics.** Poll latency percentiles, sustainable
  throughput (where scheduling lag diverges), object-store GETs per poll, and ingest
  throughput.
- **Cover the parameters that matter.** Number of keys and polling-frequency
  distribution (primary); arrival rate, page size, segment size, and compaction
  (secondary / contrast).
- **Reuse the existing harness.** Build on the `bencher` framework (RFC 0003),
  mirroring the conventions of `vector-bench` and the existing `log-bench` `ingest`
  benchmark.

## Non-Goals

- **Multi-key / fan-out queries.** Each poll targets one key; breadth comes from the
  population of consumers. A multi-key query type (built on `list_keys` + fan-out) is
  deferred to a future RFC.
- **Direct read-amplification measurement.** We measure GET counts (the cost-facing
  observable) rather than bytes-read ratios, and block-cache hit rate only indirectly
  (see Design).
- **Built-in regression analysis.** As with RFC 0003, the benchmark produces data;
  analysis is external.
- **End-to-end pipeline throughput.** We do not measure log shipping or
  producer-to-consumer streaming throughput. That is the domain of messaging-pipeline
  benchmarks (see Alternatives) and of opendata-buffer; it is not the workload LogDb
  serves.

## Design

### System-under-test interface

The workload is specified against a narrow interface rather than LogDb's APIs directly,
so the same offered load and the same metrics can later be replayed against other
systems users reach for — a comparison this RFC aims to *enable*, not to carry out:

```rust
trait LogStore {
    // Append one record to a key's log; the arrival path.
    async fn append(&self, key: Key, payload: Bytes) -> Result<()>;

    // Read up to `max` records for `key` after `cursor`; the follow path.
    // Returns the records and the opaque cursor to resume from next time.
    async fn poll(&self, key: Key, cursor: Cursor, max: usize)
        -> Result<(Vec<Record>, Cursor)>;
}
```

`Cursor` is opaque: the benchmark stores whatever the implementation returns and hands
it back on the next poll, never interpreting it (a global sequence for LogDb, an offset
for a partitioned log, a stream ID for an in-memory cache, a row id for a relational
table). Likewise, **lag is defined in terms of the offered load, not any backend's
addressing**: the benchmark generates the arrivals, so it knows how many records it has
appended to each key and how many each consumer has acknowledged, and lag is their
difference in records. The primary analysis axis is therefore backend-agnostic.

LogDb is the first and primary implementation (`append` → `try_append`, `poll` →
`scan(key, cursor..)`). The rest of this Design analyzes how LogDb serves the workload;
the interface exists so comparison is possible without re-specifying it. Choosing,
tuning, and running competitor implementations — and the cross-system cost model that
would require — is out of scope (see Non-Goals).

### Workload model

A run drives two concurrent processes against a single live `LogStore` in real time,
for a fixed wall-clock duration.

- **Arrivals.** A writer `append`s records to keys at a target arrival rate. The
  aggregate write rate is `cardinality × arrival_rate_per_key` and must stay within the
  system's ingest capacity — at high cardinality this forces a low per-key rate (e.g. a
  message every few minutes), which is realistic for the target use cases. Arrival is
  uniform across keys in the first pass; per-key skew is a possible later knob.
- **Polls.** Each key has a consumer holding a `cursor`. A consumer polls on its own
  schedule, drawn from the polling-frequency distribution, calling
  `poll(key, cursor, page_size)` and storing the returned cursor; a deeply lagged
  consumer drains over several polls rather than one unbounded read. An offline event
  pauses a consumer's polling for a span, letting lag accumulate before it resumes and
  catches up.

Because the benchmark tracks appended-minus-acknowledged per key, it always knows each
consumer's **lag** in records, and the arrival rate converts that to lag-in-seconds for
interpretation. Poll latency and GETs/poll are bucketed by lag at poll time — the
analysis axis that replaces any notion of imposed query "age," and which needs no time
index and no deterministic segment boundaries.

### Scheduling

Polls are scheduled **open-loop**: a consumer's poll times follow its own cadence,
independent of how long any poll takes — matching real users who poll on their own
schedule regardless of server speed. (Closed-loop generation, where the next poll waits
on the previous to complete, self-limits throughput and hides tail latency under load;
see Alternatives.)

- **Sharded schedulers.** Keys are partitioned across scheduler threads. Each shard
  owns a min-heap of `(due_time, consumer)` for its keys and runs its own clock. Since
  consumers are independent, sharding avoids a single global heap lock at high poll
  rates.
- **Clock separate from execution.** Each shard peeks the next due event, sleeps until
  it is due, **dispatches** the poll onto a shared bounded execution pool, immediately
  reschedules that consumer (`due + next_interval`), and continues — it never blocks on
  execution. The bounded pool sizes the concurrency the database sees and is where
  backpressure appears.
- **Coordinated-omission-correct latency.** Poll latency is measured from the
  *scheduled* due time to completion, so queueing under load is counted rather than
  hidden. Pure service time (start → completion) is recorded separately to isolate the
  database's intrinsic cost.
- **Saturation is a first-class result.** As offered load approaches capacity, the
  dispatch backlog and scheduling lag diverge; that divergence point is the sustainable
  throughput, and it tells us whether the latency numbers were taken below saturation.
  A bounded `page_size` keeps individual polls short so schedulers stay responsive.

A later revision may give less predictable query types (e.g. deep historical scans)
their own schedulers, so they don't perturb the steady polling schedulers.

### Segmentation

Because reads are bucketed by lag — a consumer-side quantity the benchmark tracks
exactly — the analysis does **not** depend on segment boundaries falling at
deterministic sequence positions. `seal_size` still governs read amplification and
remains a knob (below), but its boundaries only affect the *magnitude* of catch-up
cost, not the validity of the lag buckets.

Size-based sealing (sealing a segment once its accumulated raw key+value bytes reach a
threshold) is a long-planned LogDb feature and would make cost numbers more reproducible
run-to-run; it is not a prerequisite. Until it lands the benchmark can use the existing
wall-clock `seal_interval`, accepting that segment boundaries land at ingest-dependent
sequence positions.

### Object store and the GET metric

For LogDb, the cost metric is the number of object-store GET requests per poll, since
that is what a user pays for on object storage. GET *counts* depend only on the access
pattern and storage layout, not on the object-store implementation, so we run the
parameter sweeps against the `InMemory` object store (deterministic counts, fast
iteration, zero S3 variance) and reserve a `Local`-filesystem or real-S3 run for
representative latency. The cache effect falls out of the same metric: caught-up
consumers reading the shared hot tail report GETs/poll well below one, while deep
catch-ups approach one GET per dense block.

GETs/poll is specific to LogDb's object-store backend; it is an internal cost probe, not
part of the backend-agnostic comparison surface (see Metrics).

### Parameters

Ranked by relevance to the thesis. The workload parameters (`key_cardinality`,
`poll_freq_dist`, `arrival_rate`, `page_size`) define the offered load and apply to any
`LogStore`; `cache_size`, `seal_size`, and `compaction` are LogDb storage tuning.

**Primary**
- `key_cardinality` ∈ {1K, 100K, 1M} — the number of independently followed logs; the
  primary scaling axis.
- `poll_freq_dist` — the distribution over per-consumer polling cadence, plus an offline
  model (how often and how long consumers go offline). This shapes the lag distribution
  and therefore the mix of caught-up and catch-up reads.
- `cache_size` — an absolute byte budget for the block cache (deterministic and
  machine-independent, unlike a fraction of system memory). The lever for the
  caught-up-cheap vs. lagged-miss asymmetry; the output reports it as a fraction of the
  tail working set for interpretability.

**Secondary**
- `arrival_rate` — per-key append rate; sets how fast the tail moves and converts lag to
  seconds. Uniform across keys in the first pass; bounded by ingest capacity.
- `page_size` — maximum records returned per poll; bounds per-poll cost and keeps
  schedulers responsive, with deep catch-up draining over several polls.
- `seal_size` — segment size threshold, governing block density vs. segment fan-out for
  catch-up reads.

**Contrast only (not in the main grid)**
- `compaction` (`l0_only` vs. full, per RFC 0005) — at *low* cardinality `l0_only` is a
  write-throughput win; at high cardinality it is a read-amplification loss (many L0
  sorted runs to merge), confirming why high-cardinality following wants full
  consolidation.
- Per-key arrival/poll skew (zipf) — "busy mailboxes" that receive and are polled more
  than others.

### Phases

A run proceeds in three stages over the same live database:

1. **Pre-fill.** Append an initial per-key history so consumers have a backlog behind
   the tail and segments/compaction exist before measurement. Record ingest throughput
   and write-side request counts here.
2. **Warm-up.** Run arrivals and polls for a warm-up window; discard its metrics while
   the cache reaches steady state.
3. **Measure.** Continue for the measurement window, recording all metrics bucketed by
   lag.

### Metrics

- **Poll latency** — p50/p90/p99/p999, bucketed by lag, reported both as scheduled →
  completion (includes queueing) and as service time. Tail percentiles matter because
  fan-out drives the tail.
- **Sustainable throughput** — the offered load at which scheduling lag diverges.
- **GETs per poll** — bucketed by lag. LogDb's primary cost metric (LogDb-specific).
- **Throughput** — polls/s, records consumed/s, and arrivals/s.
- **Ingest throughput** — records/s and bytes/s during pre-fill (existing `ingest`
  conventions).

Poll latency, sustainable throughput, and the throughput counts are universal — they
apply to any `LogStore` and form the cross-system comparison surface. GETs/poll is
specific to LogDb's object-store backend; a cross-system *cost* comparison would need a
per-system resource/$ denominator, which is out of scope here.

These are emitted through the existing `bencher` counter/histogram/summary API
(RFC 0003); the lag bucket is carried as a metric label.

### Scenarios

Three scenarios tell the story rather than enumerating the full cross-product:

- **A — Cardinality scaling (centerpiece).** Scale `key_cardinality` 1K → 1M at a fixed
  per-key arrival rate and polling distribution; show per-poll latency and cost stay
  approximately flat and report the sustainable aggregate throughput at each point.
- **B — Lag asymmetry.** Fix cardinality; vary `poll_freq_dist` and the offline model to
  generate caught-up-heavy vs. lag-heavy populations; plot latency and GETs/poll against
  lag. The LSM-shape demonstration: caught-up polls are cheap via caching, deep
  catch-ups cost ~1 dense GET per block.
- **C — Seal-size sweep.** Vary `seal_size` at fixed cardinality; show the
  block-density / segment-fan-out sweet spot for catch-up reads.

## Alternatives

- **An OpenMessaging-style pipeline benchmark.** The established benchmark for log-based
  systems, the [OpenMessaging Benchmark](https://openmessaging.cloud/) (OMB), models
  messaging *pipelines*: producers write to topics and consumer groups read the stream
  end to end. Its metrics are end-to-end throughput and latency, and its workload is
  parameterized by topics, partitions, and subscriptions rather than by key population.
  We rejected an OMB-shaped workload because it measures a pipeline, not a store of
  independently followed logs — and for pipeline use cases opendata-buffer is the better
  fit and the more appropriate OMB target. Millions of independent per-key consumers
  polling intermittently is a different problem that aggregate pipeline throughput says
  little about.
- **Closed-loop load generation.** Scheduling each consumer's next poll only after its
  previous poll completes would be simpler, but it self-limits throughput (a slow
  database simply gets polled less), never reveals true saturation, and suffers
  coordinated omission — hiding tail latency exactly when the system is stressed. The
  open-loop model matches users who poll on their own schedule.
- **A logical, non-real-time simulation.** Driving the workload in logical "rounds" or
  sequence ticks instead of wall-clock time would be perfectly reproducible, but its
  results (latency "per round") do not map to anything a user can act on. Real time
  yields interpretable latency and throughput at the cost of machine-dependent numbers;
  we keep the *workload* seed-reproducible and accept that measured performance varies.
- **Imposing a recency-biased query distribution over a static corpus.** An earlier
  design issued a stream of recency-weighted single-key scans. It required mapping query
  "age" onto sequence ranges (with no time index) and deterministic segment boundaries
  for stable age bands. The consumer-follow model generates recency organically and
  tracks lag directly, removing both requirements.
- **Measuring read amplification directly (bytes read ÷ bytes returned).** More precise
  in principle, but the cost a user actually pays on object storage is request count, so
  GETs/poll is the closer fit to the cost model.
- **Running every parameter as a full cross-product.** Rejected in favor of three
  curated scenarios; the cross-product is large and most cells are uninformative.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-06-01 | Initial draft |
