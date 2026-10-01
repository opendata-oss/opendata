# RFC 0003: Consumer Read-Ahead and `ack_through`

**Status**: Accepted

**Authors**:

- Apurva Mehta

## Summary

Add a fetch-handle type and three methods to `buffer::Consumer` so a consumer
can fetch and decode many data batches **concurrently** while keeping manifest
mutation **serialized and batched**:

```rust
impl Consumer {
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    /// Convenience wrapper for serial callers; constructs a local
    /// fetch handle and calls `fetch`.
    pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    /// Cheap, cloneable handle to the read path. Holds an `Arc` to
    /// the object store; carries no manifest state and no mutable
    /// cursor.
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
}
```

`next_descriptors` reads the manifest once and returns a contiguous run of
entries past the consumer's read-ahead cursor, performing no object-store
fetch. `ConsumerFetchHandle::fetch` performs the object fetch and batch decode
for a single descriptor; it is stateless and clone-shareable across worker
tasks, so many fetches can run in parallel. `ack_through(seq)` advances the
durable ack frontier through `seq` in a single manifest write, replacing the
per-sequence ack loop a batching consumer would otherwise run.

Together these turn the two manifest interactions — discovering work and
acknowledging it — into **batch** operations, and let the high-volume work —
fetching and decoding batch objects — run **in parallel**. That is the whole
point; see the Motivation for why this is the lever for throughput.

The existing `next_batch`, `ack`, `flush`, and `close` API is preserved
unchanged. `next_batch` becomes a thin wrapper over `next_descriptors(1)` +
`fetch_descriptor`, and — as today — can be called repeatedly to read
successive batches before any of them are acked (the read cursor is
independent of the durable ack frontier). Epoch fencing is unchanged.

opendata-contrib RFC 0002 (the generic ingest runtime) is **one example** of a
pipelined client built on these primitives. The API stands on its own and is
useful to any consumer that wants parallel fetch/decode and batched acks.

## Motivation

The shipped `Consumer` API (RFC 0001) is single-threaded and serial:

```rust
pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>>;
pub async fn ack(&mut self, sequence: u64) -> Result<()>;
pub async fn flush(&mut self) -> Result<()>;
```

The throughput theory is simple, and it motivates the whole RFC:

- **Batch objects are large, and fetch and decode are independent.** Fetching a
  batch from object storage is an I/O wait; decoding (length-decode +
  decompression) is CPU work. Running them serially on one task leaves both the
  object-store bandwidth and the machine's cores underused. The way to use both
  is to **fetch and decode many batches in parallel**.
- **Manifest operations are expensive and should be batched.** Reading the
  manifest and acking are CAS-style interactions with object storage, each with
  a high round-trip cost. Doing one per batch caps throughput at
  `1 / manifest_RTT`. The way to remove that cap is to **amortize the expensive
  manifest operations over many batches**: read N descriptors per manifest read,
  ack a whole contiguous run per manifest write.

So the design is: batch the expensive, low-volume operations (manifest read,
ack) and parallelize the cheap-per-call, high-volume operations (fetch, decode).
The shipped API blocks both:

- `next_batch` couples a manifest read to a single object fetch, and `&mut self`
  rules out running fetches concurrently. Per-sequence `read(seq)` is `O(N)`
  over the manifest entries, so pulling K consecutive sequences re-reads and
  re-scans the manifest K times.
- `ack(seq)` advances one sequence at a time. After processing a run of 500
  sequences a consumer calls `ack` 500 times; the consumer's real invariant is
  "advance the durable frontier through `high`," not "ack each sequence."
- Decode runs on the fetch task, so CPU-bound decompression blocks the I/O task.

The minimal change that unblocks this is to **split the read path into a
manifest step and a fetch step**, expose the split so fetches can run
concurrently, and add a bulk ack primitive. Existing serial consumers do not
have to change.

## Goals

- A read-ahead method that returns multiple contiguous descriptors per manifest
  read.
- A cloneable, concurrency-safe `ConsumerFetchHandle` whose `fetch(&self, …)`
  runs object-store GETs in parallel without sharing `&mut Consumer`.
- `ack_through(sequence)` that advances the durable ack frontier in one manifest
  write, with dequeue-first ordering so the in-memory cursor and the metrics
  counter only advance after the durable mutation succeeds.
- Preserve the existing `next_batch`, `ack`, `flush`, `close` API unchanged.
- Preserve epoch fencing exactly: read-ahead must surface `Fenced` the same way
  the serial path does.
- Keep manifest mutation (epoch increment, dequeue) serialized through one
  owner.
- Document the concurrency contract for `ConsumerFetchHandle::fetch` and the
  descriptor-handout contract on `next_descriptors` so callers can rely on them
  without reading the implementation.

## Non-Goals

- A Buffer-side worker pool, parallel-fetch helper, or decode scheduler. The new
  API hands the caller the primitives; orchestration belongs in client
  implementations — for example the new ingest runtime (opendata-contrib RFC
  0002).
- Multi-consumer fanout from one manifest. Buffer keeps "one active consumer per
  manifest," enforced by epoch fencing.
- A new manifest format, or any change to `Manifest` / `ManifestEntry` /
  `Metadata`.
- Changing the producer side (`QueueProducer` / `Producer`).
- Out-of-order acks. `ack_through(n)` advances the frontier monotonically; there
  is no API to ack a non-contiguous sequence and fill the gap later.
- Replay tooling. Consumers already accept `last_acked_sequence` on
  construction; the new API does not extend that.

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

Internals the read-ahead design depends on:

- `Consumer` holds `QueueConsumer`, an `Arc<dyn ObjectStore>`, the GC task, and
  three private cursors: `last_acked_sequence`, `last_fetched_sequence`,
  `ack_count` (used to amortize `dequeue` every `DEQUEUE_INTERVAL = 100` acks).
- `QueueConsumer::dequeue(through_sequence)` removes every manifest entry up to
  and including `through_sequence` in **one CAS write** (one manifest read, one
  write, retrying only on a conflicting concurrent write). It is the only
  mutating call on the read side. Because it operates on the whole range at
  once, advancing the frontier by one sequence and by ten thousand cost the same
  single manifest write.
- Epoch fencing: manifest reads return `Error::Fenced` if the manifest's epoch
  differs from the consumer's. The new API must do the same.
- `decode_batch` does length-decoding and (when configured) decompression. It
  runs synchronously inside the fetch path today.

Two properties the new API relies on:

1. **Reading the manifest does not mutate it**, so multiple readers can read it
   concurrently.
2. **`dequeue` is the only manifest mutation on the read side**, so as long as
   `dequeue` stays serialized through one owner, parallel fetch/decode is safe.

Note that `next_batch` already advances a read cursor independent of acking: a
consumer can call `next_batch` repeatedly to walk successive batches and then
ack them later. What it cannot do today is fetch those batches concurrently or
ack the run in one operation.

## Design

### Public API Surface

Add `BatchDescriptor`, `ConsumerFetchHandle`, and four methods to `Consumer`:

```rust
/// Lightweight pointer to one manifest entry. Carries everything a
/// caller needs to fetch the batch without re-reading the manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
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
    /// descriptors are fully independent; two concurrent calls against
    /// the same descriptor are safe but waste a GET.
    pub async fn fetch(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;
}

impl Consumer {
    /// Read the manifest once and return up to `max` contiguous
    /// descriptors past the consumer's read-ahead cursor. Performs no
    /// object-store GET and does not mutate the durable ack frontier.
    /// Advances an in-memory `last_handed_out` cursor by the number of
    /// descriptors returned. See "Descriptor Handout Contract".
    ///
    /// Returns an empty `Vec` if no new entries are available;
    /// returns `Err(Error::Fenced)` if the consumer's epoch no longer
    /// matches the manifest's.
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;

    /// Construct a cloneable fetch handle. Cheap; O(1) per call.
    pub fn fetch_handle(&self) -> ConsumerFetchHandle;

    /// Convenience wrapper for serial callers. Equivalent to
    /// `self.fetch_handle().fetch(descriptor).await`, but also updates
    /// the consumer's serial lag cursor (see "Cursor Semantics").
    pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence` with a single `dequeue` against the manifest, then
    /// update in-memory state on success. If `dequeue` fails (storage
    /// error, fence), `last_acked_sequence` does not advance and the
    /// metrics counter does not increment.
    ///
    /// Rejects `sequence <= last_acked_sequence` (the frontier is
    /// monotonic). It does **not** otherwise validate the range — see
    /// "Descriptor Handout Contract" for the caller's obligation.
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
}
```

The existing methods keep their current signatures.

#### Why a separate handle (not just `Arc<Consumer>`)

An obvious alternative is to put the consumer behind `Arc<Consumer>` and call a
`&self` fetch method from worker tasks. That does not work in practice: every
other useful method on `Consumer` (`next_descriptors`, `ack`, `ack_through`,
`flush`, `close`) takes `&mut self`. Holding any of those open while fetch tasks
run would require either an interior-mutability rewrite of every cursor, a
second `Consumer` (impossible — one active consumer per manifest), or a separate
handle whose only job is reading object storage.

The handle is the cheapest option. It owns nothing manifest-related, clones via
`Arc`, and the `&mut Consumer` on the owner side and the `&self` on the handle
do not interact at the borrow-checker level.

A pipelined client using these APIs would look like:

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
            // ...send to a decode stage...
        }
        Ok::<(), Error>(())
    });
}

// Owner task: poll a batch of descriptors, fan them out, ack the run.
loop {
    let descriptors = consumer.next_descriptors(K).await?;
    for d in descriptors { descriptor_tx.send(d).await?; }
    // ...wait for the contiguous-completed high watermark, then:
    consumer.ack_through(high).await?;
}
```

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

`fetch_descriptor` is the wrapper that drives the serial lag cursor; it calls
the handle's `fetch` and then advances `last_fetched_sequence` and updates the
`consumer_lag_seconds` gauge. Existing serial consumers — for example the
timeseries and vector buffer consumers, which call `next_batch` in a loop and
`ack` — get the same return type and per-call cost and do not change.

### Concurrency Model

The Consumer is split into two roles, each with its own type:

- **Manifest owner** (`Consumer`, `&mut self`): `next_descriptors`,
  `fetch_descriptor`, `ack`, `ack_through`, `flush`, `close`. **One holder at a
  time.** Sharing across tasks means picking one owner task and routing manifest
  operations through it; the RFC provides no cross-task coordination primitive.
- **Fetcher** (`ConsumerFetchHandle`, `&self`): `fetch`. Stateless, touches no
  manifest state. **Cloneable; safe to share across many tasks.**

Two patterns callers can rely on:

1. **Owner + N fetch workers** (a pipelined client): one task owns `&mut
   Consumer`, calls `next_descriptors(K)` to refill a bounded descriptor
   channel, and N fetch workers each hold a clone of `fetch_handle()` and pop
   descriptors off it. Workers send completed batches back; the owner observes
   completions and calls `ack_through(high)` periodically. Owner and workers run
   in parallel because their state is disjoint.
2. **Single-task serial** (legacy): one task calls `next_batch` (or
   `next_descriptors(1)` + `fetch_descriptor`) and `ack` in a loop. No handle
   needed.

Three non-obvious safety arguments:

- **Re-fetching a descriptor is safe.** Two `fetch` calls against the same
  descriptor both GET the object and return identical `ConsumedBatch` values.
  Wasteful, not incorrect.
- **A descriptor outliving its manifest entry is safe to fetch.** If the
  manifest is dequeued through `descriptor.sequence` between `next_descriptors`
  and `fetch`, the data object is still present until GC runs, so the GET
  succeeds. If GC has already deleted it (grace period elapsed), the GET returns
  404, surfaced as `Error::Storage`; the caller should retry the descriptor and,
  if it persists, treat it as a signal that acks are outrunning processing (see
  Failure Modes). Callers avoid this by acking *after* fetch + process, which is
  the normal pattern.
- **The handle never observes a fence on its own.** Fence is detected inside
  `next_descriptors` (the next manifest read after fencing returns `Fenced`). A
  worker holding a stale handle keeps fetching successfully against object
  storage; the manifest owner learns of the fence and propagates it (e.g. by
  closing the descriptor channel).

### Cursor Semantics

The Consumer maintains three monotonic cursors. **All three live on `Consumer`
and are mutated only through `&mut self` methods.** `ConsumerFetchHandle` is
stateless and never touches them.

| Cursor | Mutator | Meaning |
|---|---|---|
| `last_acked_sequence` | `ack` (success), `ack_through` (after dequeue success) | Highest sequence durably dequeued. |
| `last_fetched_sequence` | `fetch_descriptor` (the `&mut self` wrapper) | Highest sequence the **serial wrapper path** has read. Feeds `consumer_lag_seconds`. |
| `last_handed_out_sequence` (new) | `next_descriptors` | Highest sequence handed out as a descriptor. |

`last_handed_out_sequence` is what `next_descriptors` uses to find the next
contiguous run; it is distinct from `last_fetched_sequence` because a caller may
hand out descriptors before the corresponding fetches resolve. The initial value
of all three is the `last_acked_sequence` passed to `Consumer::new`.

**`ConsumerFetchHandle::fetch` advances no cursor.** A parallel-fetch caller
that wants a lag signal computes it itself: each `ConsumedBatch` carries
`metadata`, and `metadata.last().ingestion_time_ms` against the current wall
clock is the batch's age, which the caller can emit as its own gauge per fetched
batch. (A pipelined client typically tracks finer per-stage latencies than a
single max-sequence gauge would give.) The serial `fetch_descriptor` wrapper
keeps maintaining `consumer_lag_seconds` for legacy callers.

### `ack_through` — Batched, Single-Write Acking

`ack_through(seq)` performs **one `dequeue(seq)`**, which removes every manifest
entry `<= seq` in a single CAS write, then updates in-memory state only if the
dequeue succeeded:

```rust
pub async fn ack_through(&mut self, sequence: u64) -> Result<()> {
    if let Some(last) = self.last_acked_sequence
        && sequence <= last
    {
        return Err(Error::Storage(format!(
            "non-monotonic ack_through: last_acked={last}, requested={sequence}",
        )));
    }
    let count_advanced = match self.last_acked_sequence {
        None => sequence + 1,
        Some(last) => sequence - last,
    };

    // Durable dequeue first; bubbles up Fenced/Storage without touching state.
    self.consumer.dequeue(sequence).await?;

    self.last_acked_sequence = Some(sequence);
    self.ack_count = self.ack_count.wrapping_add(count_advanced);
    metrics::counter!(m::ACKS).increment(count_advanced);
    Ok(())
}
```

Because `dequeue` operates on the whole range in one write, `ack_through(seq)`
is a single manifest operation no matter how far it advances the frontier — that
is the batching win over the per-sequence `ack` loop (which still exists,
unchanged, with its amortize-every-100 behavior for serial callers).

When `ack_through` returns `Err(_)` the caller must treat the range as **not**
durably acked and not advance its own bookkeeping. Retrying `ack_through(seq)`
is safe: `QueueConsumer::dequeue` is idempotent, so the retry either succeeds or
returns the same error class.

`flush` keeps its meaning for `ack`-based callers (force the amortized dequeue).
After `ack_through`, `flush` is a no-op (the dequeue already ran).

### Descriptor Handout Contract

`next_descriptors` advances `last_handed_out_sequence` *before* the caller has
fetched the objects. That is intentional — read-ahead is the point — and it
places a contract on the caller. The key facts:

- **`ack_through(n)` does not verify that anything below `n` was processed.** It
  unconditionally advances the durable frontier to `n` with one dequeue, only
  rejecting `n <= last_acked_sequence`. The API does not — and at this layer
  cannot — detect "gaps." So a caller that does `next_descriptors(100)`,
  processes nothing, and calls `ack_through(100)` *will* get a successful ack
  and **silently lose** sequences 1..100. Nothing in the contract forbids it; it
  is the caller's responsibility not to do it.
- **The caller must track which sequences it has actually processed and only
  ever call `ack_through` with the highest fully-processed *contiguous*
  sequence.** Fetches and processing may complete out of order; the caller acks
  the contiguous-complete watermark, never past a still-pending sequence.
- **A "gap" is a caller-side notion, not a Buffer error.** If a descriptor in a
  run is permanently lost (a worker panics, a channel send is dropped), the
  caller must simply not advance `ack_through` past that sequence. The frontier
  stalls there until the caller recovers.
- **Recovery is process restart.** `Consumer::new(config, last_acked_sequence)`
  re-initializes `last_handed_out_sequence = last_acked_sequence`, so
  `next_descriptors` re-emits everything the durable frontier has not acked.
  This is the only supported way to reissue a lost descriptor; within a process,
  a handed-out descriptor is never reissued.

A client meeting this contract tracks processed sequences and issues contiguous
`ack_through` calls; treating a lost descriptor as fatal and restarting from the
durable frontier is the simplest correct policy. The opendata-contrib RFC 0002
runtime is one example: it bounds the descriptor channel and treats a dropped
descriptor as a runtime-fatal that forces a restart. The Buffer crate itself
does not detect lost descriptors.

### Byte-Budget Accounting and Object Size

A pipelined client typically wants to bound in-flight bytes (pause pulling when
too much fetched-but-undecoded data is outstanding). `BatchDescriptor`
deliberately does **not** carry an object size: the current manifest format does
not record per-entry object size, so Buffer cannot provide a real one. A client
bounds in-flight bytes using its own estimate (e.g. a configured
`estimated_max_batch_bytes`), accepting that it pauses slightly earlier than
tight accounting would.

The clean way to make this exact is a future Buffer change: record object size
in the manifest at append time and surface it on `BatchDescriptor`. That is a
manifest-format change (a declared non-goal here) and is the recommended
follow-up once the read-ahead path is stable; see Alternatives for the
size-discovery options that were rejected for this RFC.

### Parallel Decode

`ConsumerFetchHandle::fetch` does the GET and then `decode_batch` (length-decode
+ decompression) on the calling task. A client that wants to keep CPU-bound
decode off the I/O task needs a way to get the raw bytes and decode them
elsewhere (typically on a blocking pool).

The seam for that is a sibling method on the handle plus public decode helpers:
`fetch_raw` returns the still-encoded object bytes as a `RawConsumedBatch`, and
`decode_batch` / `CompressionType` are public so the caller decodes when and
where it chooses:

```rust
pub use crate::model::{decode_batch, CompressionType};

impl ConsumerFetchHandle {
    /// Like `fetch`, but returns the still-encoded object bytes.
    /// The caller invokes `decode_batch` itself (e.g. inside
    /// `tokio::task::spawn_blocking`) to keep decode off the I/O task.
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

```rust
let raw = fetch_handle.fetch_raw(d).await?;
let entries = tokio::task::spawn_blocking(move || decode_batch(raw.raw_bytes))
    .await
    .map_err(|e| Error::Storage(e.to_string()))??;
let batch = ConsumedBatch { entries, sequence: raw.sequence,
                            location: raw.location, metadata: raw.metadata };
```

This lives on the handle (not `Consumer`) because parallel-fetch callers are the
use case; serial callers keep using `fetch_descriptor`, which decodes inline.
Putting decode behind an in-crate `spawn_blocking` was rejected: it bakes a
Tokio scheduling decision into Buffer, which callers on a different scheduler or
blocking-pool size lose control over.

The core read-ahead methods (`next_descriptors`, `fetch_handle`,
`fetch_descriptor`, `ack_through`) are the required surface. The raw-decode
helpers (`fetch_raw` / `RawConsumedBatch` / the public `decode_batch`) are an
additive extension for clients that parallelize decode; they can ship behind a
feature flag so consumers that do not need them do not carry the extra public
types.

### Metrics

The existing buffer metrics split between the handle's fetch path (any caller
can drive it concurrently) and the serial `fetch_descriptor` wrapper:

- `buffer.batches_collected`, `buffer.bytes_collected`,
  `buffer.entries_collected`, `buffer.fetch_duration_seconds`: incremented
  inside `ConsumerFetchHandle::fetch`, once per fetched batch under either path.
- `buffer.consumer_lag_seconds` (gauge): updated only by the serial
  `fetch_descriptor` wrapper. Parallel callers that use the handle directly do
  not advance it and observe lag through their own metrics.
- `buffer.acks` (counter): also incremented by `ack_through`, by the number of
  sequences the call advances over, so the rate reads "sequences acked / sec"
  under both APIs.
- `buffer.queue_length` (gauge): updated inside `next_descriptors` as it is
  inside `next_batch` today.

One new metric:

- `buffer.descriptors_handed_out` (counter): incremented by `descriptors.len()`
  on each `next_descriptors` call, so operators can see read-ahead activity.

No metric renames; existing dashboards keep working.

### `next_descriptors` Implementation

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
            if out.len() >= max { break; }
        }
        Ok(out)
    }
}
```

```rust
pub async fn next_descriptors(&mut self, max: usize)
    -> Result<Vec<BatchDescriptor>>
{
    if max == 0 { return Ok(Vec::new()); }
    let entries = self.consumer
        .descriptors_after(self.last_handed_out_sequence, max).await?;
    metrics::gauge!(m::QUEUE_LENGTH).set(self.consumer.len() as f64);
    if let Some(last) = entries.last() {
        self.last_handed_out_sequence = Some(last.sequence);
    }
    Ok(entries.into_iter().map(|e| BatchDescriptor {
        sequence: e.sequence, location: e.location, metadata: e.metadata,
    }).collect())
}
```

This is `O(handed_out_offset_within_manifest + max)` per call, dominated by the
manifest read latency, and strictly better than K calls to `next_batch` for K
contiguous entries. A future optimization could teach `Manifest::iter` to skip
to a target sequence in `O(log N)`; out of scope here.

## Failure Modes

- **Manifest read fails (storage error):** `next_descriptors` returns
  `Err(Error::Storage(_))`, mirroring `next_batch`. The cursor does not advance;
  a retry succeeds against a recovered store.
- **Object fetch fails (`fetch`, or its `fetch_descriptor` wrapper):** returns
  `Err(Error::Storage(_))`. The handle is stateless, so no cursor advances; the
  wrapper does not advance `last_fetched_sequence`. Retry the same descriptor
  with the same handle clone.
- **Object fetch returns 404 (GC race):** surfaces as `Error::Storage`. Retry
  the descriptor; if it keeps 404-ing, the data object was GC'd because acks
  outran processing — extend `gc_grace_period` or fix the over-aggressive
  acking. Documented in `fetch`'s rustdoc.
- **`ack_through(seq)` with `seq <= last_acked_sequence`:** returns
  `Err(Error::Storage("non-monotonic ack_through: …"))`; the cursor does not
  advance. (There is no "gap" rejection — see the Descriptor Handout Contract.)
- **Fence between `next_descriptors` and a later fetch:** the fetch is
  unaffected (the handle does not consult the manifest). The next
  `next_descriptors` or `ack_through` returns `Error::Fenced`. The fetched batch
  is correct but must not be acked by a fenced consumer; the client's layer is
  responsible for not advancing ack state after a fence (today's
  `BufferConsumerRuntime` treats `Fenced` as fatal).

## Alternatives Considered

### One Bigger Refactor (Internal Worker Pool)

Fixing serial fetch, wasted manifest reads, and the ack loop with a single
larger change — e.g. an internal worker pool inside the buffer crate — would
couple Buffer to a specific runtime/concurrency model. Splitting the read path
into manifest + fetch steps and exposing the split is the minimal change that
unblocks parallel clients while leaving orchestration to them; existing
consumers do not change.

### Add Object Size via HEAD, GET Metadata, or a Payload Hint

To give `BatchDescriptor` a real object size without a manifest-format change: a
HEAD before each GET (an extra round trip per fetch, for a metric-tightening
benefit — rejected, estimation is cheaper); remembering sizes from prior GET
responses (re-introduces per-sequence state into the otherwise-stateless fetch
path — rejected); or a producer-supplied size in `Metadata.payload` (couples
Buffer to its producers, which RFC 0001 avoided — rejected). Recording size in
the manifest at append time is the right long-term fix and is left as a future
follow-up.

### Keep the Read Path Serial, Add Only `ack_through`

`ack_through` alone removes the ack loop and helps even serial consumers, but it
does not address the dominant cost (serial object fetch). Shipping both together
is cheaper to review and reason about.

### A Streamed `next_batch_stream(max)` Yielding Decoded Batches

A `Stream` of `ConsumedBatch` would hide the manifest-vs-fetch split, but it
makes decode placement implicit (defeating the blocking-decode seam), leaks the
manifest-read cadence into the iteration cadence, and hands Buffer the
concurrent-fetch scheduling responsibility this RFC deliberately leaves to
callers.

### A Separate `BufferReader` Type

Enforcing the owner-vs-fetcher split through two types rather than `&mut self`
vs `&self` would force every existing caller to choose a type at construction;
the handle is a smaller change with the same safety.

### `next_descriptors` Returning a Borrowed Iterator, or Taking `start_sequence`

A borrowed iterator over the loaded manifest would force callers to copy out
descriptors or hold the consumer mutably while iterating (killing parallel
fetch). An explicit `start_sequence` leaks the internal cursor and opens
non-monotonic read-ahead; the `last_acked_sequence` constructor argument already
covers "resume from a point."

### `ack_through(&self)` for Concurrent Acking

Manifest mutation must stay serialized; concurrent acks would race on `dequeue`.
A client that needs many ack sources serializes them at the owner (RFC 0002's
runtime has a per-source ack coordinator that does exactly this).

## Implementation Notes

- `descriptors_after` on `QueueConsumer` re-uses `read_manifest`; no CAS, no
  mutation.
- `last_handed_out_sequence` is a new private field initialized from
  `last_acked_sequence`.
- The metric move (`fetch_batch` → `fetch_descriptor`) is mechanical;
  `next_batch` becomes a wrapper over `next_descriptors(1)` + `fetch_descriptor`.
- `ack_through` performs one unconditional `dequeue`; it does not call `flush`.
- The raw-decode helpers (`fetch_raw` / `RawConsumedBatch` / the `decode_batch`
  re-export) can sit behind a feature flag so consumers that do not need them do
  not carry the extra public types.
- Tests must cover: `next_descriptors` returning `< max` on a short manifest;
  concurrent `fetch` from two clones of one handle while the owner runs
  `next_descriptors` / `ack_through`; `ack_through` advancing over a contiguous
  range in one dequeue; `ack_through` rejecting non-monotonic input;
  `ack_through` leaving state untouched on `Fenced`/storage errors
  (dequeue-first invariant); fence detection on `next_descriptors`; `next_batch`
  parity with the wrapper.

## Migration Plan

1. **Buffer crate**: implement `BatchDescriptor`, `ConsumerFetchHandle`,
   `next_descriptors`, `fetch_handle`, `fetch_descriptor` (`&mut self`, wrapping
   the handle plus the serial-cursor update), and `ack_through` (dequeue-first);
   rewrite `next_batch` as a wrapper.
2. **Pipelined-client adoption** (e.g. opendata-contrib RFC 0002): use
   `next_descriptors`, route fetches through clones of `fetch_handle()`, and
   call `ack_through` instead of looping `ack`. A client can start with `max =
   1` and one fetch task, then raise `max` and fan out to N fetch workers for
   parallel fetch with no further buffer changes. Serial consumers (timeseries,
   vector) keep `next_batch` + `ack` unchanged.
3. **Raw-decode helpers**: land when a client is ready to move `decode_batch`
   onto a blocking pool; until then the feature flag stays off.

The buffer crate version bumps to a minor (additive API). No SemVer breakage.

## Validation Criteria

### API and Behavior

- `next_descriptors(0)` returns `Ok(Vec::new())` without a manifest read.
- `next_descriptors(K)` on an empty queue returns `Ok(Vec::new())`.
- `next_descriptors(K)` on a manifest with `M < K` available entries returns `M`
  descriptors and advances `last_handed_out_sequence` through the last one.
- `next_batch` after a fresh `Consumer::new` returns the same
  `ConsumedBatch.sequence` and `entries` as before, for the same manifest state;
  repeated `next_batch` calls walk successive batches without an intervening ack.
- `fetch_descriptor(d)` and `ConsumerFetchHandle::fetch(d)` against a `d` from
  `next_descriptors` both return a `ConsumedBatch` with matching `sequence`,
  `location`, and `metadata`.
- Two concurrent `fetch` calls (clones of one handle) against distinct
  descriptors complete with no manifest contention while the owner runs
  `next_descriptors` / `ack_through`.
- `ack_through(seq)` advances `last_acked_sequence` to `seq` and removes manifest
  entries `<= seq` in one dequeue; `flush` afterward is a no-op.
- `ack_through(seq)` errors when `seq <= last_acked_sequence`.

### Fencing and Failure Atomicity

- A fenced consumer's `next_descriptors` returns `Error::Fenced` on the next
  call after the fence.
- A fenced or storage-failed `ack_through(seq)` leaves `last_acked_sequence` and
  `buffer.acks` unchanged; a retry against a healthy consumer succeeds.
- `fetch` against a descriptor obtained before fencing succeeds (the data object
  is readable until GC); the handle does not detect fence on its own.

### Concurrency (handle path)

- `fetch_handle()` returns a value that is `Clone + Send + Sync + 'static`.
- Two handle clones fetch concurrently from two tasks with no manifest
  contention; a clone outlives the task that produced it.
- `fetch` mutates no `Consumer` cursor: after a handle fetch, the owner's
  `last_fetched_sequence` is unchanged.

### Descriptor Handout Contract

- After `next_descriptors` returns sequence `S`, a subsequent call without an
  intervening `ack_through` returns sequences `> S` only.
- `Consumer::new(_, Some(frontier))` re-emits descriptors strictly after
  `frontier`, including sequences a previous process handed out but never acked.

### Metrics

- `buffer.batches_collected` increments once per `fetch_descriptor` call (and
  thus once per `next_batch`).
- `buffer.acks` increments by the number of sequences `ack_through` advances.
- `buffer.descriptors_handed_out` increments by `descriptors.len()` per
  `next_descriptors` call.

### Raw-decode helpers (when the feature is enabled)

- `fetch_raw(d)` returns a `RawConsumedBatch` whose `decode_batch(raw_bytes)`
  produces an `entries` vector identical to `fetch_descriptor(d)`'s.
- `decode_batch` is callable from `spawn_blocking` and produces the same output
  as the inline decode.
- `fetch_raw` lives only on the handle; serial callers use `fetch_descriptor`.
