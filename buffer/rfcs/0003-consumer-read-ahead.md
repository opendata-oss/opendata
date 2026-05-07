# RFC 0003: Consumer Read-Ahead and `ack_through`

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

Add three methods to `buffer::Consumer` so high-throughput consumers can
fetch and decode many data batches concurrently while keeping manifest
mutation serialized:

```rust
impl Consumer {
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    pub async fn fetch_descriptor(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
}

pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}
```

`next_descriptors` reads the manifest once and returns a contiguous run
of entries past the consumer's read-ahead cursor without performing any
object-store fetch. `fetch_descriptor` performs the object fetch and
batch decode for a single descriptor. It takes `&self` so multiple
fetches can run concurrently. `ack_through(seq)` advances the durable
ack frontier through `seq` in one call, replacing the per-sequence loop
the high-throughput runtime would otherwise need.

The existing `next_batch`, `ack`, `flush`, and `close` API is preserved
unchanged; `next_batch` becomes a thin compatibility wrapper over
`next_descriptors(1)` + `fetch_descriptor`. Epoch fencing semantics are
unchanged.

This RFC is the Buffer-side companion to opendata-contrib RFC 0002
(generic ingest runtime). Phase 2 of the high-throughput plan
(`plans/odb-high-throughput`) implements the API; Phase 6 of that plan
is what consumes it for parallel fetch and decode.

## Motivation

The shipped `Consumer` API (RFC 0001) is single-threaded and serial:

```rust
pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>>;
pub async fn ack(&mut self, sequence: u64) -> Result<()>;
pub async fn flush(&mut self) -> Result<()>;
```

`next_batch` does three things in one async call: read the manifest,
look up the next sequence in it, and fetch the data object from object
storage. Each call therefore performs at least one manifest read (or
metadata cache hit) plus one object-store GET, even when the manifest
has hundreds of contiguous entries past the cursor.

For the high-throughput ingestor (opendata-contrib RFC 0002):

- **Object fetch is the dominant async wait** for the consumer. Running
  fetches in parallel is the obvious throughput lever, but the current
  API does not let multiple in-flight fetches share manifest state.
  Calling `next_batch` from N tasks would interleave manifest reads and
  cursor advances unpredictably, and `&mut self` rules it out anyway.
- **Manifest reads are wasted** when the consumer pulls `K` consecutive
  sequences. Each call re-fetches the manifest object and re-iterates
  it; the per-sequence `read(seq)` path is `O(N)` over the manifest
  entries (`queue.rs` line 738-742).
- **Acks loop one sequence at a time**. After a commit group covering
  500 sequences, the runtime calls `consumer.ack(seq).await` 500 times
  in sequence. Each call updates an in-memory cursor and trips
  `dequeue` every 100 acks. Even on the happy path this is wasted
  ceremony; the runtime's invariant is "advance the durable frontier
  through `high`," not "ack each sequence individually."
- **Decompression and decode happen on the same task as the fetch.**
  `fetch_batch` calls `decode_batch` synchronously after the GET,
  inside the consumer's async task. CPU-bound decompression that should
  run on a blocking pool blocks the I/O task instead.

Fixing all four with one bigger refactor (e.g. an internal worker pool)
would couple the buffer crate to a specific runtime model. The minimal
change that unblocks the high-throughput design is to **split the read
path into a manifest step and a fetch step**, expose the split in the
public API, and add a bulk ack primitive. Existing consumers do not
have to change.

## Goals

- Add a public read-ahead API that returns multiple
  contiguous descriptors per manifest read.
- Add a public concurrent-safe `fetch_descriptor(&self, ...)` so callers
  can run object-store GETs in parallel without sharing `&mut Consumer`.
- Add `ack_through(sequence)` so callers can advance the durable
  ack frontier in one call.
- Preserve the existing `next_batch`, `ack`, `flush`, `close` API
  unchanged. `next_batch` becomes a thin wrapper over the new API.
- Preserve epoch fencing exactly. Read-ahead must surface a `Fenced`
  error the same way the serial path does.
- Keep manifest mutation (epoch increment, dequeue) serialized through
  one owner.
- Document the concurrency contract for `fetch_descriptor` so callers
  can rely on it without inspecting the implementation.

## Non-Goals

- A Buffer-side worker pool, parallel-fetch helper, or
  decode/decompression scheduler. The new API hands the caller the
  primitives; orchestration belongs in `opendata-ingest-runtime` (RFC
  0002 in opendata-contrib).
- Multi-consumer fanout from one manifest. Buffer keeps "one active
  consumer per manifest" enforced by epoch fencing. Read-ahead does
  not change that.
- A new manifest format, or any change to the `Manifest` /
  `ManifestEntry` / `Metadata` shapes.
- Changing the producer side (`QueueProducer` / `Producer`).
- Out-of-order acks. `ack_through(n)` advances monotonically; there is
  still no API to ack a non-contiguous sequence and have the gaps
  later acked.
- Replay tooling (operator-directed re-read past the durable frontier).
  Consumers already accept `last_acked_sequence` on construction; the
  new API does not extend that.
- A blocking-CPU decode helper inside the buffer crate. Optional raw-
  bytes hook (Tier 2 below) is the seam, not a scheduler.

## Background

The shipped contract from RFC 0001:

```rust
impl Consumer {
    pub async fn new(config: ConsumerConfig, last_acked_sequence: Option<u64>) -> Result<Self>;
    pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>>;
    pub async fn ack(&mut self, sequence: u64) -> Result<()>;
    pub async fn flush(&mut self) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}

pub struct ConsumedBatch {
    pub entries: Vec<Bytes>,
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}

pub struct Metadata {
    pub start_index: u32,
    pub ingestion_time_ms: i64,
    pub payload: Bytes,
}
```

Internals relevant to the read-ahead design:

- `Consumer` holds `QueueConsumer`, an `Arc<dyn ObjectStore>`, the GC
  task, and three private cursors: `last_acked_sequence`,
  `last_fetched_sequence`, `ack_count` (used to amortize `dequeue`
  every `DEQUEUE_INTERVAL = 100` acks).
- `QueueConsumer::peek` and `QueueConsumer::read(sequence)` both read
  the manifest object and iterate its entries. They are
  `pub(crate)` today; `next_batch` is the only public read entry.
- `QueueConsumer::dequeue(through_sequence)` performs a CAS write
  against the manifest. It is the only mutating call on the read
  side.
- Epoch fencing: `peek` and `read` return `Error::Fenced` if the
  manifest's epoch differs from the consumer's epoch. The new API
  must do the same.
- `decode_batch` is `pub(crate)`. It does length-decoding and (when
  configured) decompression. It runs synchronously inside `next_batch`.

Two properties the new API depends on:

1. **Reading the manifest does not mutate it**, so multiple readers can
   safely call `read_manifest` concurrently. The current
   `pub(crate)` calls `peek` and `read` do exactly that.
2. **`dequeue` is the only manifest mutation on the read side**, so as
   long as `dequeue` (i.e. `flush`) stays serialized through one owner,
   parallel fetch/decode is safe.

## Design

### Public API Surface

Add `BatchDescriptor` and three methods to `Consumer`:

```rust
/// Lightweight pointer to one manifest entry. Carries everything a
/// caller needs to fetch the batch without re-reading the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}

impl Consumer {
    /// Read the manifest once and return up to `max` contiguous
    /// descriptors past the consumer's read-ahead cursor.
    ///
    /// Does not perform any object-store GET. Does not mutate the
    /// durable ack frontier. Advances an in-memory `last_handed_out`
    /// cursor by the number of descriptors returned.
    ///
    /// Returns an empty `Vec` if no new entries are available;
    /// returns `Err(Error::Fenced)` if the consumer's epoch no longer
    /// matches the manifest's.
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    /// Fetch and decode a single batch from object storage.
    ///
    /// Takes `&self` so callers can run multiple fetches concurrently
    /// (e.g. behind an `Arc<Consumer>` or by cloning a fetch handle —
    /// see "Concurrency Model" below). Does not consult the manifest;
    /// the descriptor must come from a prior `next_descriptors` call
    /// on this consumer.
    pub async fn fetch_descriptor(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Equivalent to calling `ack(seq)` for every sequence
    /// from `last_acked_sequence + 1` through `sequence` followed by
    /// `flush()`, but performed as one in-memory update plus one
    /// `dequeue(sequence)` manifest write.
    ///
    /// Returns an error if `sequence` is less than or equal to the
    /// current `last_acked_sequence`. The frontier is monotonic.
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
}
```

The existing methods stay with their current signatures.

### `next_batch` Becomes a Compatibility Wrapper

```rust
pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>> {
    let mut descriptors = self.next_descriptors(1).await?;
    match descriptors.pop() {
        Some(d) => Ok(Some(self.fetch_descriptor(d).await?)),
        None => Ok(None),
    }
}
```

Existing callers (the timeseries consumer, the vector consumer, the
ClickHouse alpha) get the same return type and the same per-call cost.
The lag gauge, latency histogram, and counter increments that
`next_batch` records today move to `fetch_descriptor` so they fire
exactly once per fetched batch under both code paths. The
`ConsumerConfig` struct is unchanged.

### Concurrency Model

The Consumer is split into two roles:

- **Manifest owner** (`&mut self`): `next_descriptors`, `ack`,
  `ack_through`, `flush`, `close`. These mutate the in-memory cursor or
  perform manifest CAS writes. One holder at a time.
- **Fetcher** (`&self`): `fetch_descriptor`. Performs an object-store
  GET and `decode_batch` against a descriptor that is already
  determined. Does not touch manifest state.

Two patterns that callers can rely on:

1. **Single-task pipeline**: one task owns `&mut Consumer`, calls
   `next_descriptors(K)`, hands the resulting descriptors to a `Vec`
   of futures (each calling `consumer.fetch_descriptor(d)`),
   `try_join_all`s them, then calls `ack_through` after sink commit.
   This is the high-throughput runtime's day-one shape.
2. **Shared fetcher**: callers may put the consumer behind
   `Arc<Consumer>` (after a one-time mutable construction) and clone
   the `Arc` into N fetch tasks. The `&self` signature on
   `fetch_descriptor` makes this safe. Manifest-owning methods
   (`next_descriptors`, `ack`, `ack_through`, `flush`, `close`) take
   `&mut Consumer` and therefore cannot be called through an `Arc`
   without interior mutability; that is intentional.

Two non-obvious safety arguments:

- **Re-fetching a descriptor is safe.** Two tasks calling
  `fetch_descriptor` with the same descriptor both perform an object-
  store GET and return identical `ConsumedBatch` values. There is no
  mutation. Callers should not normally do this (it wastes a GET) but
  doing so is not a correctness bug.
- **A descriptor outliving the manifest entry is safe to fetch.** If
  the manifest is dequeued through `descriptor.sequence` between
  `next_descriptors` and `fetch_descriptor`, the data object on object
  storage is still there until GC runs; the GET still succeeds. If GC
  has already deleted the object (because the grace period elapsed),
  the GET returns a 404, which `fetch_descriptor` surfaces as
  `Error::Storage`. Callers control this gap by acking *after* fetch +
  process, not before, which is already the consumer pattern.

### Cursor Semantics

The Consumer maintains three monotonic cursors:

| Cursor | Mutator | Meaning |
|---|---|---|
| `last_acked_sequence` | `ack`, `ack_through` | Highest sequence the manifest has been instructed to dequeue (subject to amortized flush). |
| `last_fetched_sequence` | `fetch_descriptor` | Highest sequence whose data object the consumer has read. |
| `last_handed_out_sequence` (new) | `next_descriptors` | Highest sequence the consumer has handed out as a descriptor. |

The new cursor `last_handed_out_sequence` is what `next_descriptors`
uses to find the next contiguous run on a manifest re-read. It is
distinct from `last_fetched_sequence` because callers may hand out
descriptors before fetching the corresponding objects.

Initial value of all three is `last_acked_sequence` from construction
(`None` for a fresh consumer; `Some(seq)` when the caller passed
`last_acked_sequence: Some(seq)`).

`next_batch` advances `last_handed_out_sequence` and
`last_fetched_sequence` together, preserving today's behavior.

`fetch_descriptor` advances **only** `last_fetched_sequence`, and only
to `descriptor.sequence` if `descriptor.sequence` is greater than the
current value. (Multiple fetches may resolve out of order; the cursor
records the maximum so the lag gauge stays meaningful.) It does **not**
mutate `last_handed_out_sequence` (the descriptor was already handed
out) or `last_acked_sequence` (the caller acks separately).

`ack_through(seq)` sets `last_acked_sequence = seq` and calls
`QueueConsumer::dequeue(seq)` once. The `ack_count`-based amortization
that `ack` uses today is replaced with an explicit per-call dequeue;
callers that want amortization can either keep using `ack` or call
`ack_through` less often (e.g. once per commit group, which is what the
high-throughput runtime does). Concretely:

- `ack(seq)` keeps its existing per-sequence loop semantics with
  amortized dequeue every `DEQUEUE_INTERVAL = 100` acks.
- `ack_through(seq)` always performs one dequeue at call time. It is
  the right primitive when the caller already batches.

### Epoch Fencing

`next_descriptors` returns `Error::Fenced` if the consumer's epoch no
longer matches the manifest. The check happens inside
`QueueConsumer::read_manifest` exactly as it does today for
`peek`/`read`.

`fetch_descriptor` does **not** consult the manifest, so it does not
itself detect fencing. Callers that want to fail fast on fence can
either:

1. Keep one `next_descriptors` task running on the consumer; it will
   hit `Fenced` on its next manifest read.
2. Treat repeated `Storage` errors on `fetch_descriptor` (404s after
   GC) as a fence signal in operations.

The most common case is path (1): the high-throughput runtime drives
`next_descriptors` continuously to keep the descriptor queue full, so
a fence surfaces within one polling interval.

### Object Fetch Path

`fetch_descriptor` performs the same work `Consumer::fetch_batch` does
today:

1. `object_store.get(&Path::from(descriptor.location.as_str())).await`
2. `data.bytes().await`
3. `decode_batch(data)` to produce `Vec<Bytes>` entries
4. Construct `ConsumedBatch { entries, sequence, location, metadata }`

The metrics that `fetch_batch` emits today
(`BATCHES_COLLECTED`, `ENTRIES_COLLECTED`, `BYTES_COLLECTED`,
`FETCH_DURATION_SECONDS`, `CONSUMER_LAG_SECONDS`) move to
`fetch_descriptor`, so per-batch metrics are emitted exactly once per
fetched batch under both old and new APIs.

`decode_batch` runs on the calling task. Callers that want to put it
on a blocking pool can call `tokio::task::spawn_blocking` around the
decode step (see "Optional: Raw Decode Hook" below).

### `next_descriptors` Implementation

The body adds one new path to `QueueConsumer`:

```rust
// queue.rs
impl QueueConsumer {
    pub(crate) async fn descriptors_after(
        &self,
        after_sequence: Option<u64>,
        max: usize,
    ) -> Result<Vec<QueueEntry>> {
        let (manifest, _) = self.read_manifest().await?;
        if manifest.epoch != self.epoch.load(Ordering::Relaxed) {
            return Err(Error::Fenced);
        }
        let mut out = Vec::with_capacity(max);
        for entry in manifest.iter() {
            let entry = entry?;
            if let Some(after) = after_sequence
                && entry.sequence <= after
            {
                continue;
            }
            out.push(entry);
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }
}
```

`Consumer::next_descriptors` wraps this:

```rust
pub async fn next_descriptors(&mut self, max: usize)
    -> Result<Vec<BatchDescriptor>>
{
    if max == 0 {
        return Ok(Vec::new());
    }
    let entries = self
        .consumer
        .descriptors_after(self.last_handed_out_sequence, max)
        .await?;
    metrics::gauge!(m::QUEUE_LENGTH).set(self.consumer.len() as f64);
    if let Some(last) = entries.last() {
        self.last_handed_out_sequence = Some(last.sequence);
    }
    Ok(entries
        .into_iter()
        .map(|e| BatchDescriptor {
            sequence: e.sequence,
            location: e.location,
            metadata: e.metadata,
        })
        .collect())
}
```

This is `O(handed_out_offset_within_manifest + max)` per call, dominated
by the manifest read latency. It is strictly better than `K` calls to
`next_batch` for `K` contiguous entries when `K > 1`, and equivalent
when `K = 1`.

A future optimization can teach `Manifest::iter` to skip to a target
sequence in `O(log N)` (the manifest entries are sorted by sequence).
That is out of scope for this RFC; the linear scan is already fine for
manifests of the sizes we run.

### `ack_through` Implementation

```rust
pub async fn ack_through(&mut self, sequence: u64) -> Result<()> {
    if let Some(last) = self.last_acked_sequence
        && sequence <= last
    {
        return Err(Error::Storage(format!(
            "non-monotonic ack_through: last_acked={}, requested={}",
            last, sequence,
        )));
    }
    let count_advanced = match self.last_acked_sequence {
        None => sequence + 1,
        Some(last) => sequence - last,
    };
    self.last_acked_sequence = Some(sequence);
    self.ack_count = self.ack_count.wrapping_add(count_advanced);
    metrics::counter!(m::ACKS).increment(count_advanced);
    self.consumer.dequeue(sequence).await?;
    Ok(())
}
```

The dequeue is unconditional. `ack` keeps its amortized-every-100-acks
behavior; the high-throughput runtime opts into immediate dequeue by
calling `ack_through`.

`Consumer::flush` keeps its meaning: if the caller used `ack` and is
between amortization boundaries, `flush` forces the dequeue. After
`ack_through`, `flush` is a no-op (the dequeue already ran), which
matches what callers want.

### Optional: Raw Decode Hook (Tier 2)

The high-throughput runtime wants to put `decode_batch` on a blocking
CPU pool. Two ways to expose that:

**Option A (chosen):** make `decode_batch` and `CompressionType` public,
and add a sibling `fetch_descriptor_raw` that returns the raw object
bytes plus the descriptor without decoding:

```rust
pub use crate::model::{decode_batch, CompressionType};

impl Consumer {
    pub async fn fetch_descriptor_raw(&self, descriptor: BatchDescriptor)
        -> Result<RawConsumedBatch>;
}

pub struct RawConsumedBatch {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
    pub raw_bytes: Bytes,
}
```

Callers wrap the decode in `spawn_blocking`:

```rust
let raw = consumer.fetch_descriptor_raw(d).await?;
let entries = tokio::task::spawn_blocking(move || decode_batch(raw.raw_bytes))
    .await
    .map_err(|e| Error::Storage(e.to_string()))??;
let batch = ConsumedBatch {
    entries,
    sequence: raw.sequence,
    location: raw.location,
    metadata: raw.metadata,
};
```

**Option B:** keep `decode_batch` private and add an explicit
`Consumer::fetch_descriptor_blocking_decode` that internally spawns
blocking. Rejected because it bakes a Tokio scheduling decision into
the buffer crate — callers running on a different scheduler (e.g.
async-std, smol, or Tokio with a specific blocking-pool size) lose
control.

Option A is small, additive, and gives the runtime exactly the seam it
wants without committing the buffer crate to a scheduling model.

The Tier 2 surface ships behind a feature flag in v0.x or as a separate
release; v1 of this RFC requires only the three core methods.

### Metrics

Carry over the existing buffer metrics with one rename:

- `buffer.batches_collected` (counter): increments inside
  `fetch_descriptor`. Same as today's `next_batch` increment.
- `buffer.bytes_collected`, `buffer.entries_collected`,
  `buffer.fetch_duration_seconds`, `buffer.consumer_lag_seconds`:
  all move into `fetch_descriptor`.
- `buffer.acks` (counter): now also incremented by `ack_through`
  by the number of sequences the call advances over. So a counter rate
  reads "sequences acked per second" under both APIs.
- `buffer.queue_length` (gauge): updated inside `next_descriptors`
  the same way it is inside `next_batch` today.

Add one new metric:

- `buffer.descriptors_handed_out` (counter): increments by
  `descriptors.len()` on each `next_descriptors` call. Lets operators
  see read-ahead activity directly.

No metrics renames; existing dashboards keep working.

### Compatibility

API compatibility:

- `next_batch`, `ack`, `flush`, `close`, `len`, `is_empty`,
  `conflict_rate`, `Consumer::new`, `Consumer::with_object_store`,
  `ConsumerConfig`, `ConsumedBatch`, `Metadata`: all unchanged.
- New types: `BatchDescriptor`, `RawConsumedBatch` (Tier 2 only).
- New methods: `next_descriptors`, `fetch_descriptor`, `ack_through`,
  and (Tier 2) `fetch_descriptor_raw`.
- New public re-exports (Tier 2): `decode_batch`, `CompressionType`.

Behavioral compatibility:

- `next_batch` returns the same `ConsumedBatch` for the same input
  manifest state. The wrapper does the same work, just composed.
- Cursor advance under `next_batch` is unchanged.
- `ack`'s amortization behavior is unchanged.
- `flush` semantics are unchanged.
- Epoch fencing surfaces from `next_descriptors` exactly as it does
  from `next_batch`.

Wire/format compatibility:

- No manifest format change.
- No data batch format change.

Existing consumers (`timeseries`, `vector` BufferConsumer,
`clickhouse-ingestor`) compile against the new buffer crate without
changes. They get no throughput benefit until they migrate to the new
methods, which is fine — the migration is per-consumer.

## Failure Modes

- **Manifest read fails (storage error)**: `next_descriptors` returns
  `Err(Error::Storage(_))`, mirroring today's `next_batch`. The
  consumer's cursor does not advance; a retry succeeds against a
  recovered store.
- **Object fetch fails on `fetch_descriptor`**: returns
  `Err(Error::Storage(_))`. The cursor (`last_fetched_sequence`) does
  not advance for that descriptor. Callers may retry the same
  descriptor.
- **Object fetch returns 404 (GC race)**: surfaces as `Error::Storage`
  with a "404"-shaped message. Documented in
  `fetch_descriptor`'s rustdoc; the runbook tells operators to extend
  `gc_grace_period` or to investigate over-aggressive ack flushing if
  this fires in production.
- **Caller calls `ack_through(seq)` with `seq <= last_acked_sequence`**:
  returns `Err(Error::Storage("non-monotonic ack_through: ..."))`. The
  caller's cursor does not advance.
- **Caller calls `fetch_descriptor` with a sequence that is no longer
  in the manifest** (descriptor predates a recent dequeue or restart):
  the GET still succeeds against the still-present data object until
  GC deletes it; identical to the "descriptor outliving manifest entry"
  case. After GC, falls back to the 404 path above.
- **Fence between `next_descriptors` and `fetch_descriptor`**: the
  fetch is unaffected (it does not consult the manifest). The next
  `next_descriptors` or `ack_through` returns `Error::Fenced`. The
  fetched batch is correct but should not be acked by a fenced
  consumer. The caller's runtime layer is responsible for not advancing
  ack state after a fence; today's `BufferConsumerRuntime` already
  treats `Fenced` as fatal.

## Alternatives Considered

### Add a Streamed `next_batch_stream(max: usize)` Returning Decoded Batches

A `futures::Stream` that yields `ConsumedBatch` values would hide the
manifest-vs-fetch split from callers. Rejected because:

- It makes `decode_batch` placement implicit (inside the stream's poll
  task), which defeats the point of giving callers a blocking-decode
  seam.
- It leaks the manifest-read cadence into the iteration cadence; a
  caller that wants to pre-fetch 1000 descriptors but decode 4 at a
  time has no clean way to express that.
- It gives the buffer crate a scheduling responsibility (concurrent
  fetch under the hood) that the RFC explicitly leaves to callers.

### Keep Read Path Serial, Add Only `ack_through`

`ack_through` alone removes the per-sequence ack loop and is a clear
win even for serial consumers. Rejected as an isolated change because
it does not solve the dominant cost: serial object fetch. Shipping
both at once is cheaper to review and reason about.

### Introduce a New `BufferReader` Type Separate from `Consumer`

A separate read-only handle could enforce the manifest-owner-vs-fetcher
split through types instead of `&mut self` vs `&self`. Rejected
because:

- The split is already visible in the method signatures.
- It would force every existing caller to choose a type at construction.
- An `Arc<Consumer>` with `&self` fetch is a smaller change, behaves
  identically, and keeps the type surface narrow.

### Make `next_descriptors` Return a Borrowed Iterator

`next_descriptors` could return `impl Iterator<Item = &ManifestEntry>`
borrowing from the manifest. Rejected because the manifest object is
loaded inside the call and the borrowed iterator would force callers
to either copy out the descriptors anyway or hold the consumer
mutably while iterating, which kills the parallel-fetch use case.
Returning `Vec<BatchDescriptor>` is one allocation per read-ahead
window and is dominated by the manifest read latency.

### Make `next_descriptors` Take an Explicit `start_sequence`

Callers could pass `start_sequence` to read from any point in the
manifest. Rejected because:

- The cursor is internal state the buffer crate already owns; exposing
  it leaks an invariant.
- An explicit `start_sequence` opens the door to non-monotonic
  read-ahead, which makes ack tracking harder to reason about.
- The `last_acked_sequence` argument on `Consumer::new` already covers
  the legitimate "resume from a specific sequence" case.

### Make `ack_through` Take `&self` (Allow Concurrent Acking)

Multiple sinks could `ack_through` independently if it took `&self`.
Rejected because manifest mutation must be serialized; concurrent acks
would race on `dequeue`. The high-throughput runtime's `AckCoordinator`
already serializes acks at the source level (RFC 0002 in
opendata-contrib, "Per-Source Ack Coordinator").

### Push Decompression into the Object Store Layer

Pre-decompress object-store responses inside `slatedb::object_store`.
Rejected as out of scope and as the wrong layer; decompression policy
varies per buffer signal and per workload, and should stay near the
batch-decode boundary.

## Implementation Notes

- The new `descriptors_after` method on `QueueConsumer` re-uses
  `read_manifest`. No CAS, no mutation.
- `Consumer::last_handed_out_sequence` is a new private field
  initialized from `last_acked_sequence` in `with_object_store`.
- The metric move (`fetch_batch` → `fetch_descriptor`) is mechanical;
  `next_batch` becomes a wrapper that calls `next_descriptors(1)`,
  pops the single descriptor, and calls `fetch_descriptor`.
- `ack_through(seq)` does not need to call `Consumer::flush`; the
  unconditional `dequeue` it performs is the durable boundary.
- The Tier 2 raw-decode hook should land behind a feature flag
  (`raw-fetch`) so consumers that do not need it do not pay for the
  added types in their public API surface.
- Tests must cover: `next_descriptors` returning < `max` when the
  manifest is shorter; `fetch_descriptor` from two tasks against an
  `Arc<Consumer>`; `ack_through` skipping over a contiguous range;
  `ack_through` rejecting non-monotonic input; fence detection on
  `next_descriptors`; `next_batch` parity with the wrapper.

## Migration Plan

1. **Phase 2.1–2.5**: implement `BatchDescriptor`, `next_descriptors`,
   `fetch_descriptor`, `ack_through`, and rewrite `next_batch` as a
   wrapper. Tests in 2.6.
2. **opendata-contrib RFC 0002 / Phase 4**: the runtime extraction
   uses `next_descriptors` + `fetch_descriptor` from day one (with
   `max = 1` until Phase 6 turns concurrency on). `ack_through` is
   used wherever the runtime would otherwise loop `ack`. Other
   consumers (timeseries, vector) keep using `next_batch` and `ack`
   unchanged.
3. **opendata-contrib Phase 6**: the runtime increases `max` and
   parallelizes `fetch_descriptor`. No further buffer changes.
4. **Tier 2 raw-decode hook**: lands when the high-throughput runtime
   is ready to put `decode_batch` on a blocking pool (Phase 6+). Until
   then, the feature flag stays off and the public surface is just the
   three core methods.

The buffer crate version bumps to a minor (additive API). No SemVer
breakage.

## Validation Criteria

### API and Behavior

- `next_descriptors(0)` returns `Ok(Vec::new())` without manifest read.
- `next_descriptors(K)` on an empty queue returns
  `Ok(Vec::new())`.
- `next_descriptors(K)` on a manifest with `M < K` available entries
  returns `M` descriptors and advances `last_handed_out_sequence`
  through the last returned sequence.
- `next_batch` after a fresh `Consumer::new` returns the same
  `ConsumedBatch.sequence` and the same `entries` as in v0.x for the
  same manifest state.
- `fetch_descriptor(d)` against a `d` from `next_descriptors` returns a
  `ConsumedBatch` with `sequence == d.sequence`,
  `location == d.location`, and `metadata == d.metadata`.
- Two concurrent `fetch_descriptor` calls (under `Arc<Consumer>`)
  against distinct descriptors complete with no manifest contention.
- `ack_through(seq)` advances `last_acked_sequence` to `seq` and
  removes manifest entries `<= seq`. `flush()` after `ack_through` is
  a no-op.
- `ack_through(seq)` errors when `seq <= last_acked_sequence`.

### Fencing

- A fenced consumer's `next_descriptors` returns `Error::Fenced` on the
  next call after the fence happens.
- `fetch_descriptor` against a descriptor obtained before fencing
  succeeds (the data object is still readable until GC).

### Compatibility

- `cargo test -p buffer` is green against the new crate without
  changes to existing tests.
- Downstream consumers (timeseries, vector, clickhouse-ingestor) build
  unchanged.

### Metrics

- `buffer.batches_collected` increments exactly once per
  `fetch_descriptor` call (and therefore once per `next_batch` under
  the wrapper).
- `buffer.acks` increments by the number of sequences `ack_through`
  advances over.
- `buffer.descriptors_handed_out` increments by `descriptors.len()` on
  every `next_descriptors` call.

### Tier 2 (when feature is enabled)

- `fetch_descriptor_raw(d)` returns a `RawConsumedBatch` whose
  `decode_batch(raw_bytes)` produces an `entries` vector identical to
  the one returned by `fetch_descriptor(d)`.
- `decode_batch` is callable from `tokio::task::spawn_blocking` and
  produces the same output as the in-line decode.

## Revision History

| Date | Description |
|---|---|
| 2026-05-07 | Initial draft. Adds `BatchDescriptor`, `Consumer::next_descriptors`, `Consumer::fetch_descriptor`, `Consumer::ack_through`, and an optional Tier 2 raw-decode hook behind a feature flag. Preserves epoch fencing and existing API/behavior. |
