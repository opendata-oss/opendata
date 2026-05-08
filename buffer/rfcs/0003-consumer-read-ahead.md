# RFC 0003: Consumer Read-Ahead and `ack_through`

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

Add a fetch-handle type and three methods to `buffer::Consumer` so
high-throughput consumers can fetch and decode many data batches
concurrently while keeping manifest mutation serialized:

```rust
impl Consumer {
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    /// Convenience wrapper for serial callers. The high-throughput
    /// runtime uses [`fetch_handle`] instead.
    pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    /// Cheap, cloneable handle to the read path. Holds an `Arc` to
    /// the object store; carries no manifest state and no mutable
    /// cursor. The owning `Consumer` keeps `&mut`-only access to
    /// manifest operations (`next_descriptors`, `ack`, `ack_through`,
    /// `flush`).
    pub fn fetch_handle(&self) -> ConsumerFetchHandle;

    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
}

#[derive(Clone)]
pub struct ConsumerFetchHandle { /* Arc<dyn ObjectStore> + manifest_path */ }

impl ConsumerFetchHandle {
    /// Stateless fetch + decode. Takes `&self`; safe to call
    /// concurrently from many tasks. Does not touch the consumer's
    /// cursors.
    pub async fn fetch(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;
}

pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
    /// Object size in bytes when known. Reserved for byte-budget
    /// accounting in `opendata-ingest-runtime` (RFC 0002 in
    /// `opendata-contrib`). v1 manifests do not carry size, so this
    /// is always `None` until a future manifest format extension
    /// fills it in. Callers that observe `None` use a pessimistic
    /// reservation per their own configuration; see "BatchDescriptor
    /// and Object Size" below.
    pub object_bytes: Option<u64>,
}
```

`next_descriptors` reads the manifest once and returns a contiguous run
of entries past the consumer's read-ahead cursor without performing any
object-store fetch. `ConsumerFetchHandle::fetch` performs the object
fetch and batch decode for a single descriptor; it is stateless and
clone-shareable across worker tasks. `Consumer::fetch_descriptor` is a
convenience wrapper for serial callers; under the hood it constructs a
local handle and calls `fetch` (it takes `&mut self` only because
it forwards lag-metric updates to the consumer's serial cursor —
parallel callers should use the handle directly). `ack_through(seq)`
advances the durable ack frontier through `seq` in one call,
replacing the per-sequence loop the high-throughput runtime would
otherwise need; durable-dequeue happens **before** any in-memory
state advances.

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
- Add a cloneable, concurrency-safe `ConsumerFetchHandle` whose
  `fetch(&self, ...)` runs object-store GETs in parallel without
  sharing `&mut Consumer`.
- Add `ack_through(sequence)` so callers can advance the durable
  ack frontier in one call, with dequeue-first ordering so the
  in-memory cursor and the metrics counter only advance after the
  durable manifest mutation succeeds.
- Preserve the existing `next_batch`, `ack`, `flush`, `close` API
  unchanged. `next_batch` becomes a thin wrapper over the new API.
- Preserve epoch fencing exactly. Read-ahead must surface a `Fenced`
  error the same way the serial path does.
- Keep manifest mutation (epoch increment, dequeue) serialized through
  one owner.
- Document the concurrency contract for `ConsumerFetchHandle::fetch`
  and the descriptor-handout contract on `next_descriptors` so callers
  can rely on them without inspecting the implementation.

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

Add `BatchDescriptor`, `ConsumerFetchHandle`, and four methods to
`Consumer`:

```rust
/// Lightweight pointer to one manifest entry. Carries everything a
/// caller needs to fetch the batch without re-reading the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
    /// Object size in bytes when known. v1 always emits `None`
    /// because the current manifest format does not carry size.
    /// Reserved for a future manifest extension and for the runtime's
    /// byte-budget accounting.
    pub object_bytes: Option<u64>,
}

/// Cheap, cloneable handle to the read path. Construction is O(1)
/// (clones an `Arc<dyn ObjectStore>` and copies the manifest path
/// string). Carries no manifest state and no mutable cursor; safe to
/// share across many fetch worker tasks.
#[derive(Clone)]
pub struct ConsumerFetchHandle {
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,  // for tracing/labels only
}

impl ConsumerFetchHandle {
    /// Fetch and decode one batch. Stateless: does not touch any
    /// `Consumer` cursor. Two concurrent calls against distinct
    /// descriptors are fully independent; two concurrent calls
    /// against the same descriptor are safe but waste a GET.
    pub async fn fetch(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;
}

impl Consumer {
    /// Read the manifest once and return up to `max` contiguous
    /// descriptors past the consumer's read-ahead cursor.
    ///
    /// Does not perform any object-store GET. Does not mutate the
    /// durable ack frontier. Advances an in-memory `last_handed_out`
    /// cursor by the number of descriptors returned. See "Descriptor
    /// Handout Contract" below for caller obligations.
    ///
    /// Returns an empty `Vec` if no new entries are available;
    /// returns `Err(Error::Fenced)` if the consumer's epoch no longer
    /// matches the manifest's.
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    /// Construct a cloneable fetch handle. Cheap; returns a new
    /// handle on each call (callers that need many handles can call
    /// once and clone, or call repeatedly — both are O(1)).
    pub fn fetch_handle(&self) -> ConsumerFetchHandle;

    /// Convenience wrapper for serial callers. Equivalent to
    /// `self.fetch_handle().fetch(descriptor).await`, but also
    /// updates the consumer's serial lag cursor (used by the
    /// `consumer_lag_seconds` gauge under the legacy `next_batch`
    /// path). Parallel callers should use `fetch_handle()` and
    /// drive the gauge themselves if they want it.
    pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Performs `dequeue(sequence)` against the manifest,
    /// then updates in-memory state on success. If `dequeue` returns
    /// an error (storage failure, fence), the consumer's
    /// `last_acked_sequence` does not advance and the metrics counter
    /// does not increment.
    ///
    /// Returns an error if `sequence` is less than or equal to the
    /// current `last_acked_sequence`. The frontier is monotonic.
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
}
```

The existing methods stay with their current signatures.

#### Why a separate handle (not just `Arc<Consumer>`)?

A previous draft of this RFC suggested putting the consumer behind
`Arc<Consumer>` and calling `fetch_descriptor(&self, ...)` from
worker tasks. That does not work in practice: every other useful
method on `Consumer` (`next_descriptors`, `ack`, `ack_through`,
`flush`, `close`) takes `&mut self`. Holding any of those calls open
while fetch tasks run requires either:

- An interior-mutability rewrite of every cursor on `Consumer`, which
  changes the safety story across the whole API surface, or
- A second instance of `Consumer` (which is impossible — Buffer
  enforces one active consumer per manifest via epoch fencing), or
- A separate handle whose only job is reading object storage.

The handle is the cheapest option. It owns nothing manifest-related,
clones via `Arc`, and disappears when the consumer is dropped (the
`Arc<dyn ObjectStore>` it holds is the same one the consumer uses, so
a stale handle has no exclusive resource).

The high-throughput runtime's expected pattern is:

```rust
// One owner task drives the manifest:
let consumer = Consumer::new(...).await?;
let fetcher  = consumer.fetch_handle();

// Spawn N fetch workers, each holding a clone:
for _ in 0..fetch_concurrency {
    let f = fetcher.clone();
    let rx = descriptor_rx.clone();
    tokio::spawn(async move {
        while let Some(d) = rx.recv().await {
            let batch = f.fetch(d).await?;
            // ...send to decode stage...
        }
        Ok::<(), Error>(())
    });
}

// Owner task: poll, ack, flush.
loop {
    let descriptors = consumer.next_descriptors(K).await?;
    for d in descriptors { descriptor_tx.send(d).await?; }
    // ...wait for completions, then:
    consumer.ack_through(high).await?;
}
```

The `&mut Consumer` in the owner task and the `&self` on the handle
do not interact at the borrow-checker level: the handle holds an
`Arc<dyn ObjectStore>`, not a borrow of the consumer.

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

`fetch_descriptor` itself is the wrapper that drives the serial lag
cursor. Internally:

```rust
pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor)
    -> Result<ConsumedBatch>
{
    let handle = self.fetch_handle();
    let batch  = handle.fetch(descriptor).await?;
    if let Some(last_meta) = batch.metadata.last() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let lag_s = (now_ms - last_meta.ingestion_time_ms) as f64 / 1000.0;
        metrics::gauge!(m::CONSUMER_LAG_SECONDS).set(lag_s.max(0.0));
    }
    self.last_fetched_sequence = match self.last_fetched_sequence {
        Some(prev) => Some(prev.max(batch.sequence)),
        None => Some(batch.sequence),
    };
    Ok(batch)
}
```

The `&mut self` is required because the lag-cursor update belongs to
the consumer; the handle path leaves that observation to the caller.

Existing callers (the timeseries consumer, the vector consumer, the
ClickHouse alpha) get the same return type and the same per-call cost.
The lag gauge, latency histogram, and counter increments that
`next_batch` records today move to `fetch_descriptor` so they fire
exactly once per fetched batch under both code paths. The
`ConsumerConfig` struct is unchanged.

### Concurrency Model

The Consumer is split into two roles, each with its own type:

- **Manifest owner** (`Consumer` with `&mut self` access):
  `next_descriptors`, `fetch_descriptor`, `ack`, `ack_through`,
  `flush`, `close`. These mutate the in-memory cursor or perform
  manifest CAS writes. **One holder at a time.** Sharing `Consumer`
  across tasks requires the caller to pick one owner task and route
  manifest operations through it (channels, actor pattern, etc.); the
  RFC does not provide cross-task coordination primitives.
- **Fetcher** (`ConsumerFetchHandle` with `&self` access): `fetch`.
  Performs an object-store GET and `decode_batch` against a
  descriptor that is already determined. Stateless. Does not touch
  manifest state. **Cloneable; safe to share across many tasks.**

Two patterns that callers can rely on:

1. **Owner + N fetch workers** (high-throughput runtime, Phase 6):
   one task owns `&mut Consumer` and drives the manifest. It calls
   `next_descriptors(K)` to refill a bounded channel of descriptors;
   N fetch workers each hold a clone of `consumer.fetch_handle()`
   and pop descriptors off the channel. Workers send completed
   batches back through another channel. The owner task observes
   completions and calls `ack_through(high)` periodically. The
   manifest owner runs in parallel with the fetch workers because
   their state is disjoint (the handle holds no `Consumer` borrow).
2. **Single-task serial** (legacy callers): one task calls
   `next_batch` (or `next_descriptors(1)` + `fetch_descriptor`) and
   `ack` in a loop. No handle needed. This is what the timeseries
   and vector consumers do today; they do not change.

Three non-obvious safety arguments:

- **Re-fetching a descriptor is safe.** Two `fetch` calls against the
  same descriptor both perform an object-store GET and return
  identical `ConsumedBatch` values. There is no mutation. Callers
  should not normally do this (it wastes a GET) but doing so is not a
  correctness bug.
- **A descriptor outliving the manifest entry is safe to fetch.** If
  the manifest is dequeued through `descriptor.sequence` between
  `next_descriptors` and `fetch`, the data object on object storage
  is still there until GC runs; the GET still succeeds. If GC has
  already deleted the object (because the grace period elapsed), the
  GET returns a 404, which `fetch` surfaces as `Error::Storage`.
  Callers control this gap by acking *after* fetch + process, not
  before, which is already the consumer pattern.
- **The handle never observes fence on its own.** Fence is detected
  inside `next_descriptors` (the next manifest read after fencing
  returns `Error::Fenced`). A fetch worker that holds a stale handle
  for a fenced consumer keeps fetching successfully against object
  storage; the manifest owner is the one that learns about the fence
  and propagates it to workers (e.g. by closing the descriptor
  channel). This is intentional: fetch workers do not need
  manifest-level state to do their job. A future RFC could add a
  `handle.is_active() -> bool` if the runtime needs an out-of-band
  fence signal that doesn't go through the descriptor channel.

### Cursor Semantics

The Consumer maintains three monotonic cursors. **All three live on
`Consumer` and are mutated only through `&mut self` methods.**
`ConsumerFetchHandle` is stateless and never touches them.

| Cursor | Mutator | Meaning |
|---|---|---|
| `last_acked_sequence` | `ack` (success), `ack_through` (after dequeue success) | Highest sequence durably dequeued from the manifest. |
| `last_fetched_sequence` | `fetch_descriptor` (the `&mut self` wrapper) | Highest sequence whose data object the **serial wrapper path** has read. Used by the `consumer_lag_seconds` gauge. |
| `last_handed_out_sequence` (new) | `next_descriptors` | Highest sequence the consumer has handed out as a descriptor. |

`last_handed_out_sequence` is what `next_descriptors` uses to find the
next contiguous run on a manifest re-read. It is distinct from
`last_fetched_sequence` because callers may hand out descriptors before
the corresponding fetches resolve.

Initial value of all three is `last_acked_sequence` from construction
(`None` for a fresh consumer; `Some(seq)` when the caller passed
`last_acked_sequence: Some(seq)`).

`next_batch` advances `last_handed_out_sequence` and
`last_fetched_sequence` together (via the wrapped
`next_descriptors(1)` and `fetch_descriptor` calls), preserving
today's behavior.

`fetch_descriptor` (the `&mut self` wrapper) advances
`last_fetched_sequence` to `max(current, descriptor.sequence)` after
the fetch succeeds. It does **not** mutate `last_handed_out_sequence`
(the descriptor was already handed out) or `last_acked_sequence` (the
caller acks separately).

**`ConsumerFetchHandle::fetch` does not advance any cursor on the
consumer.** Parallel-fetch callers that want a `consumer_lag_seconds`-
equivalent gauge observe it themselves from `batch.metadata.last()`
and emit their own metric, or wire the runtime's metrics layer to do
so. Earlier drafts of this RFC proposed an interior-mutable max
cursor on the handle; that was rejected because it adds atomic state
to the hot fetch path for a metric that the runtime already wants to
own (the runtime's `runtime_stage_latency_seconds{stage=fetch}`
histogram is finer-grained than a single max-sequence gauge).

`ack_through(seq)` performs **dequeue first, then in-memory state
update**. If `dequeue` returns an error (storage failure, fence), the
consumer's `last_acked_sequence` does not advance and the metrics
counter does not increment. Concretely:

- `ack(seq)` keeps its existing per-sequence loop semantics with
  amortized dequeue every `DEQUEUE_INTERVAL = 100` acks.
- `ack_through(seq)` always performs one dequeue at call time, and
  applies its in-memory updates only after dequeue succeeds. It is
  the right primitive when the caller already batches.

### Descriptor Handout Contract

`next_descriptors` advances `last_handed_out_sequence` *before* the
caller has fetched the corresponding objects. This is intentional —
read-ahead is the whole point — but it puts a contract on the caller
that does not exist for the serial `next_batch` API. The contract is:

> **Once `next_descriptors` returns a descriptor, the caller is
> responsible for either (a) successfully fetching and processing it,
> or (b) accepting that it will be re-handed-out only via process
> restart (which restarts read-ahead from the durable
> `last_acked_sequence`, dropping any in-flight descriptors).**

Specifically:

- **Lost descriptors are not reissued within a process.** If a worker
  task panics, drops a channel send, or otherwise discards a
  descriptor without acking the corresponding sequence, the
  consumer's `last_handed_out_sequence` has already advanced past it.
  The next `next_descriptors` call will not return the lost
  descriptor; it will return descriptors strictly after it.
- **Acking a sequence that was lost is forbidden.** The caller's own
  bookkeeping (commit-group high watermark, route completion record)
  must not mark a lost sequence complete. If it does, `ack_through`
  will durably ack a sequence whose data was never processed, and the
  data is lost.
- **Recovery is process restart.** A new `Consumer::new(config,
  last_acked_sequence)` initializes `last_handed_out_sequence =
  last_acked_sequence`, so `next_descriptors` re-emits everything the
  durable frontier hasn't acked. This is the only supported path for
  reissuing lost descriptors.
- **Out-of-order completion is fine, gaps are not.** Workers may
  complete fetches in any order. The runtime's ack coordinator (RFC
  0002 in opendata-contrib) tracks contiguous completion and only
  calls `ack_through` for the highest contiguous complete sequence.
  A gap (one descriptor in the run is permanently lost) means the
  ack frontier never advances past that gap until restart.

The high-throughput runtime in opendata-contrib RFC 0002 enforces
this contract by making the descriptor channel bounded and by
treating dropped descriptors as a runtime fatal — workers that lose
a descriptor halt the runtime and force a restart. The buffer crate
itself does not detect lost descriptors; that is the caller's job.

A future "return descriptor" API (let the caller hand a descriptor
back so it can be reissued without restart) is *not* in scope for
this RFC. It would require maintaining a per-descriptor "handed
out / returned / fetched" state inside `Consumer` that does not
exist today, and the runtime can already recover via the documented
restart path.

### BatchDescriptor and Object Size

`BatchDescriptor.object_bytes: Option<u64>` is reserved for the
runtime's byte-budget accounting (RFC 0002 in opendata-contrib).
The current Buffer manifest format does not carry per-entry object
size:

```rust
pub struct ManifestEntry {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}
```

So v1 of this RFC always emits `object_bytes: None`. The field
exists in the public type so:

1. The runtime can adopt the field today and use a configured
   pessimistic reservation (`source.estimated_max_batch_bytes`) when
   the value is `None`.
2. A future manifest format extension (or an opt-in producer-side
   metadata payload) can populate the field without breaking the
   public API.

Changing the manifest format to carry object size is **out of scope
for this RFC** (declared a non-goal above) but is a natural
follow-up after the read-ahead path is stable. Until then, the
runtime overcounts in-flight bytes in the common case and pauses
source pulls slightly earlier than tight accounting would; this is
intentional slack.

Other ways to populate `object_bytes`, considered and rejected for
v1:

- **HEAD before GET**: an extra network round trip on every fetch.
  Adds ~1 RTT to fetch latency for a metric-tightening benefit.
  Rejected; the runtime's pessimistic reservation is cheaper.
- **Read object metadata from the GET response and attach it to a
  later descriptor**: requires the consumer to remember per-sequence
  sizes across calls, which is exactly the kind of state we are
  trying to keep out of `ConsumerFetchHandle`. Rejected for v1.
- **Use `Metadata.payload` to carry a producer-supplied size hint**:
  the payload is opaque and producer-defined (RFC 0001); requiring
  producers to put a size there would couple the buffer crate to
  its consumers in a way RFC 0001 was careful to avoid. Rejected.

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

`ConsumerFetchHandle::fetch` performs the same work
`Consumer::fetch_batch` does today:

1. `object_store.get(&Path::from(descriptor.location.as_str())).await`
2. `data.bytes().await`
3. `decode_batch(data)` to produce `Vec<Bytes>` entries
4. Construct `ConsumedBatch { entries, sequence, location, metadata }`

The metrics split between two sites:

- **Per-fetch counters and the per-fetch latency histogram**
  (`BATCHES_COLLECTED`, `ENTRIES_COLLECTED`, `BYTES_COLLECTED`,
  `FETCH_DURATION_SECONDS`) move to `ConsumerFetchHandle::fetch`.
  These fire exactly once per fetched batch under both old and new
  APIs.
- **The serial lag gauge** (`CONSUMER_LAG_SECONDS`) stays on the
  `&mut self` `fetch_descriptor` wrapper. Parallel-fetch callers do
  not advance the consumer's serial cursor and do not update this
  gauge; they observe lag through their own runtime metrics
  (`runtime_stage_latency_seconds{stage=fetch}` in RFC 0002).

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
            // Reserved field; the current manifest format does not
            // carry per-entry object size. See "BatchDescriptor and
            // Object Size" above.
            object_bytes: None,
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

The dequeue is the durable boundary. In-memory state is updated **only
after** dequeue succeeds, so a fence or storage error leaves
`last_acked_sequence` and the metrics counter unchanged:

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

    // Durable dequeue first. Bubbles up Fenced and Storage errors
    // without touching local state.
    self.consumer.dequeue(sequence).await?;

    // Only mutate in-memory state after dequeue succeeds.
    self.last_acked_sequence = Some(sequence);
    self.ack_count = self.ack_count.wrapping_add(count_advanced);
    metrics::counter!(m::ACKS).increment(count_advanced);
    Ok(())
}
```

The dequeue is unconditional. `ack` keeps its amortized-every-100-acks
behavior; the high-throughput runtime opts into immediate dequeue by
calling `ack_through`.

**Important caller contract**: when `ack_through` returns `Err(_)`, the
caller must treat the requested range as **not** durably acked and
must not advance any of its own bookkeeping (commit-group high
watermark, route completion record, etc.) past the previous
successful ack. A retry of `ack_through(sequence)` against the same
sequence is safe; if the underlying dequeue is idempotent (which it
is for `QueueConsumer::dequeue`) the second call either succeeds or
returns the same error class.

`Consumer::flush` keeps its meaning: if the caller used `ack` and is
between amortization boundaries, `flush` forces the dequeue. After
`ack_through`, `flush` is a no-op (the dequeue already ran), which
matches what callers want.

### Optional: Raw Decode Hook (Tier 2)

The high-throughput runtime wants to put `decode_batch` on a blocking
CPU pool. Two ways to expose that:

**Option A (chosen):** make `decode_batch` and `CompressionType` public,
and add a sibling `ConsumerFetchHandle::fetch_raw` that returns the
raw object bytes plus the descriptor without decoding. The raw method
lives on the handle (not on `Consumer`) because parallel-fetch callers
are the use case; serial callers continue to use `fetch_descriptor`
which already calls `decode_batch` inline.

```rust
pub use crate::model::{decode_batch, CompressionType};

impl ConsumerFetchHandle {
    /// Like `fetch`, but returns the still-encoded object bytes.
    /// Callers invoke `decode_batch` themselves (typically inside
    /// `tokio::task::spawn_blocking`) to keep the CPU-bound decode
    /// off the I/O task.
    pub async fn fetch_raw(&self, descriptor: BatchDescriptor)
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
let raw = fetch_handle.fetch_raw(d).await?;
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
`ConsumerFetchHandle::fetch_blocking_decode` that internally spawns
blocking. Rejected because it bakes a Tokio scheduling decision into
the buffer crate — callers running on a different scheduler (e.g.
async-std, smol, or Tokio with a specific blocking-pool size) lose
control.

Option A is small, additive, and gives the runtime exactly the seam it
wants without committing the buffer crate to a scheduling model.

The Tier 2 surface ships behind a feature flag in v0.x or as a separate
release; v1 of this RFC requires only the core read-ahead +
`fetch_handle` + `ack_through` methods.

### Metrics

Carry over the existing buffer metrics, splitting them between the
handle's fetch path (which any caller can drive concurrently) and the
serial `Consumer::fetch_descriptor` wrapper (which additionally
maintains the consumer's lag cursor):

- `buffer.batches_collected` (counter): increments inside
  `ConsumerFetchHandle::fetch`. Each successful fetch increments by 1
  regardless of which call path drove it (handle directly, or via the
  `&mut self` `Consumer::fetch_descriptor` wrapper).
- `buffer.bytes_collected`, `buffer.entries_collected`,
  `buffer.fetch_duration_seconds`: all increment inside
  `ConsumerFetchHandle::fetch`.
- `buffer.consumer_lag_seconds` (gauge): increments inside the serial
  `Consumer::fetch_descriptor` wrapper only. Parallel callers that
  only use `ConsumerFetchHandle::fetch` do not advance this gauge —
  the consumer's serial cursor is not the right observation point
  under N-way concurrency, and the runtime owns its own
  `runtime_stage_latency_seconds{stage=fetch}` histogram (RFC 0002).
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
- New types: `BatchDescriptor`, `ConsumerFetchHandle`, and
  `RawConsumedBatch` (Tier 2 only).
- New methods: `Consumer::next_descriptors`, `Consumer::fetch_handle`,
  `Consumer::fetch_descriptor` (which now takes `&mut self` and is a
  wrapper over the handle), `Consumer::ack_through`, and (Tier 2)
  `ConsumerFetchHandle::fetch_raw`.
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
- **Object fetch fails (`ConsumerFetchHandle::fetch`, or its
  serial-wrapper caller `Consumer::fetch_descriptor`)**: returns
  `Err(Error::Storage(_))`. The handle is stateless, so no cursor
  advances; the wrapper does not advance `last_fetched_sequence`
  either, since it only updates the cursor after a successful decode.
  Callers may retry the same descriptor with the same handle clone.
- **Object fetch returns 404 (GC race)**: surfaces as `Error::Storage`
  with a "404"-shaped message. Documented in
  `ConsumerFetchHandle::fetch`'s rustdoc (and inherited by the
  `Consumer::fetch_descriptor` wrapper); the runbook tells operators
  to extend `gc_grace_period` or to investigate over-aggressive ack
  flushing if this fires in production.
- **Caller calls `ack_through(seq)` with `seq <= last_acked_sequence`**:
  returns `Err(Error::Storage("non-monotonic ack_through: ..."))`. The
  caller's cursor does not advance.
- **Caller calls `fetch` with a sequence that is no longer in the
  manifest** (descriptor predates a recent dequeue or restart): the
  GET still succeeds against the still-present data object until GC
  deletes it; identical to the "descriptor outliving manifest entry"
  case. After GC, falls back to the 404 path above. Applies equally
  to handle and wrapper.
- **Fence between `next_descriptors` and a later fetch**: the fetch
  is unaffected (the handle does not consult the manifest, and the
  wrapper delegates to the handle). The next `next_descriptors` or
  `ack_through` returns `Error::Fenced`. The fetched batch is
  correct but should not be acked by a fenced consumer. The caller's
  runtime layer is responsible for not advancing ack state after a
  fence; today's `BufferConsumerRuntime` already treats `Fenced` as
  fatal.

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
  manifest is shorter; concurrent `ConsumerFetchHandle::fetch` calls
  from two tasks against clones of one handle (the manifest owner
  task continues `next_descriptors` and `ack_through` while fetches
  run); `ack_through` skipping over a contiguous range; `ack_through`
  rejecting non-monotonic input; `ack_through` leaving in-memory state
  untouched on `Fenced` and storage errors (dequeue-first invariant);
  fence detection on `next_descriptors`; `next_batch` parity with the
  wrapper.

## Migration Plan

1. **Phase 2.1–2.5**: implement `BatchDescriptor` (with
   `object_bytes: Option<u64>` reserved as `None`), `ConsumerFetchHandle`,
   `next_descriptors`, `fetch_handle`, `fetch_descriptor` (now `&mut
   self`, wrapping the handle plus serial-cursor update),
   `ack_through` (with the documented dequeue-first ordering), and
   rewrite `next_batch` as a wrapper. Tests in 2.6.
2. **opendata-contrib RFC 0002 / Phase 4**: the runtime extraction
   uses `next_descriptors` from day one and routes object fetches
   through one clone of `Consumer::fetch_handle()` (with `max = 1`
   and a single fetch task until Phase 6 turns N-way concurrency on).
   `ack_through` is used wherever the runtime would otherwise loop
   `ack`. Other consumers (timeseries, vector) keep using `next_batch`
   and `ack` unchanged.
3. **opendata-contrib Phase 6**: the runtime increases `max`, clones
   the fetch handle into N worker tasks, and runs
   `ConsumerFetchHandle::fetch` in parallel. No further buffer
   changes.
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
- `Consumer::fetch_descriptor(d)` (the `&mut self` wrapper) and
  `ConsumerFetchHandle::fetch(d)` against a `d` from
  `next_descriptors` both return a `ConsumedBatch` with
  `sequence == d.sequence`, `location == d.location`, and
  `metadata == d.metadata`.
- Two concurrent `ConsumerFetchHandle::fetch` calls (against clones
  of one handle) targeting distinct descriptors complete with no
  manifest contention; the owner task can run `next_descriptors` /
  `ack_through` in parallel without borrow-checker conflict.
- `ack_through(seq)` advances `last_acked_sequence` to `seq` and
  removes manifest entries `<= seq`. `flush()` after `ack_through` is
  a no-op.
- `ack_through(seq)` errors when `seq <= last_acked_sequence`.

### Fencing and Failure Atomicity

- A fenced consumer's `next_descriptors` returns `Error::Fenced` on the
  next call after the fence happens.
- A fenced consumer's `ack_through(seq)` returns `Error::Fenced`
  *and* leaves `last_acked_sequence` and `buffer.acks` unchanged. A
  subsequent retry against an unfenced consumer succeeds.
- A storage-error `ack_through` (object store unreachable) leaves
  `last_acked_sequence` and `buffer.acks` unchanged. Recovery is to
  retry once storage is reachable.
- `ConsumerFetchHandle::fetch` against a descriptor obtained before
  fencing succeeds (the data object is still readable until GC). The
  handle does not detect fence on its own.

### Concurrency (handle path)

- `Consumer::fetch_handle()` returns a value that is `Clone + Send +
  Sync + 'static`.
- Two clones of a fetch handle, called concurrently from two
  `tokio::spawn` tasks against distinct descriptors, complete with no
  manifest contention.
- A fetch handle clone outlives the spawned task that produced it
  (drop-after-spawn does not invalidate the handle).
- `ConsumerFetchHandle::fetch` does **not** mutate any cursor on the
  parent `Consumer`. After a fetch handle call, `next_batch` /
  `fetch_descriptor` from the owner task observe unchanged
  `last_fetched_sequence`.

### Descriptor Handout Contract

- After `next_descriptors` returns a descriptor with sequence `S`, a
  subsequent `next_descriptors` call without an intervening
  `ack_through` returns descriptors with sequence > `S` only.
- Process restart (`Consumer::new(_, Some(durable_frontier))`)
  re-emits descriptors strictly after `durable_frontier`, including
  any sequences a previous process handed out but did not ack.

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

- `ConsumerFetchHandle::fetch_raw(d)` returns a `RawConsumedBatch`
  whose `decode_batch(raw_bytes)` produces an `entries` vector
  identical to the one returned by `Consumer::fetch_descriptor(d)`.
- `decode_batch` is callable from `tokio::task::spawn_blocking` and
  produces the same output as the in-line decode.
- `fetch_raw` lives only on the handle (not on `Consumer`); serial
  callers continue to use `Consumer::fetch_descriptor`.

## Revision History

| Date | Description |
|---|---|
| 2026-05-07 | Initial draft. Adds `BatchDescriptor`, `Consumer::next_descriptors`, `Consumer::fetch_descriptor`, `Consumer::ack_through`, and an optional Tier 2 raw-decode hook behind a feature flag. Preserves epoch fencing and existing API/behavior. |
| 2026-05-07 (rev 2) | Phase 0 gate revision. (1) Added `ConsumerFetchHandle` (Clone + Send + Sync + 'static) so manifest-owner stays `&mut self` while N fetch workers run concurrently — Arc-of-Consumer alone could not do this because every other useful method takes `&mut`. (2) Made `ConsumerFetchHandle::fetch` stateless; dropped the `&self` cursor mutation; serial lag gauge stays on the `&mut self` `fetch_descriptor` wrapper. (3) Reordered `ack_through` so `dequeue` runs before any in-memory state advances; storage/fence errors leave `last_acked_sequence` and metrics untouched. (4) Added explicit "Descriptor Handout Contract" section: lost descriptors are not reissued within a process; recovery is restart from the durable ack frontier. (5) Added `BatchDescriptor.object_bytes: Option<u64>` reserved field with v1-always-None policy and pessimistic-reservation guidance for the runtime; documented why HEAD/payload-hint/in-consumer-memory alternatives were rejected for v1. Validation Criteria expanded to cover handle concurrency, fail-atomic ack_through, and the descriptor handout contract. |
| 2026-05-07 (rev 3) | Phase 0 gate reconciliation. (a) Goals section updated to name `ConsumerFetchHandle` and the dequeue-first ack ordering; the rev-1 phrasing about "concurrent-safe `fetch_descriptor(&self, ...)`" no longer matched the rev-2 design. (b) `next_descriptors` pseudocode now populates `BatchDescriptor.object_bytes: None` explicitly (was missing — the manifest entry has no size yet). (c) Tier 2 raw-decode method moved from `Consumer::fetch_descriptor_raw(&self, ...)` to `ConsumerFetchHandle::fetch_raw(&self, ...)`. The serial Consumer no longer has a raw entry point; serial callers use `Consumer::fetch_descriptor` (which decodes inline), parallel callers use the handle's `fetch` or `fetch_raw`. Validation Criteria and Compatibility list updated to match. |
| 2026-05-07 (rev 4) | Implementation Notes / Migration Plan / Validation Criteria swept for stale `fetch_descriptor` concurrency wording. Tests now name `ConsumerFetchHandle::fetch` from cloned handles (not `Arc<Consumer>` of a `&self`-fetch_descriptor); migration step 2 routes Phase 4 fetches through one handle clone and step 3 fans out to N handle clones; Validation Criteria asserts both `Consumer::fetch_descriptor` (the `&mut self` wrapper) and `ConsumerFetchHandle::fetch` return matching `ConsumedBatch` values, and that two handle clones can fetch in parallel while the manifest owner runs `next_descriptors` / `ack_through`. |
| 2026-05-07 (rev 5) | Metrics + Failure Modes split between `ConsumerFetchHandle::fetch` (per-fetch counters: `batches_collected`, `bytes_collected`, `entries_collected`, `fetch_duration_seconds`; object-fetch failures and 404s attributed here) and the `Consumer::fetch_descriptor` `&mut self` wrapper (`consumer_lag_seconds` only; the wrapper additionally maintains the serial lag cursor). Earlier wording attributed all per-fetch metrics and all object-fetch failures to `fetch_descriptor`, which conflicted with the rev-2 design that runs parallel fetches through the handle. |
