# RFC 0004: Write Coordination

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a reusable write coordination component for opendata systems. The coordinator
enables systems to implement their own in-memory representation of state that is accumulated
across multiple writes before flushing to a persistent store (similar to the SlateDB memtable).
It handles batching, serializability, and coordination between writers and readers.

## Motivation

Existing opendata systems like `timeseries` and `vector` both implement their own in-memory buffer
before flushing data to SlateDB to reduce the serialization overhead of frequent merge operations.
This pattern is challenging to implement correctly and has caused various issues such as:

- [#82](https://github.com/opendata-oss/opendata/issues/82)
- [#95](https://github.com/opendata-oss/opendata/issues/95)

In both of those tickets, data was not properly synchronized between writers causing potential
corruption and correctness issues. In addition to fixing issues, this RFC sets the stage for
reusable components such as a backpressure mechanism and durability semantics that can be 
leveraged by various systems.

## Goals

- Implement correct, serializable write coordination semantics
- Composable design that can be leveraged by any opendata system
- Backpressure mechanism to prevent system degradation
- Clear epoch-based durability guarantees

## Non-Goals

- Reader/Writer coordination (this RFC sets the stage for this)
- Distributed write coordination

## Design

### Components & Terminology

- `Write` is an insertion of data into the system; each write is assigned a monotonically increasing
  `epoch` when dequeued by the coordinator (for ordering and durability tracking)
- `WriteQueue` is a queue that accepts `Write` as input and applies an epoch-based ordering
- `Snapshot` is a point-in-time reference to the storage state, broadcast to readers after each flush;
  represented in the API as `StorageRead` (a SlateDB snapshot)
- `Image` is the in-memory state needed to initialize a delta (e.g., series dictionary, ID counters).
  The initial image is derived from storage (e.g., loading the fingerprint→ID map); subsequent images
  are extracted from deltas via `fork_image()` to enable non-blocking flushes
- `Delta` is the result of applying writes to an `Image`; it accumulates changes until flushed

### Architecture

```
                ┌─────────┐ ┌─────────┐ ┌─────────┐
                │  Write  │ │  Write  │ │  Write  │
                └─────────┘ └─────────┘ └─────────┘
                     │           │           │
                     └───────────┼───────────┘
                                 ▼
                     ┌───────────────────────┐
                     │      Write Queue      │
                     └───────────────────────┘
                                 │
                                 ▼
┌Write Coordinator────────────────────────────────────────────────┐
│ ┌─────────────────────────────┐ ┌──────────────┐┌──────────────┐│             ┌───────────┐
│ │        select! loop         │ │ EpochTracker ││   Flusher    │├FlushEvent──▶│  Reader   │
│ ├─────────────────────────────┤ └──────────────┘└───────▲──────┘│             └───────────┘
│ │                             │         ┌───────────────┤       │
│ │                             │ ┌Delta──┴──────┐┌Image──┴──────┐│
│ │1 ─► handle flush complete   │ │ ┌──────────┐ ││              ││
│ │2 ─► flush (on command)      │ │ │  Writes  │ ││ Materialized ││
│ │3 ─► apply queued writes     │ │ └──────────┘ ││    State     ││
│ │4 ─► flush (on timer)        │ │ ┌──────────┐ ││              ││
│ │                             │ │ │  Writes  │ ││              ││
│ │                             │ │ └──────────┘ ││              ││
│ └─────────────────────────────┘ └──────────────┘└──────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### APIs

The main ingestion loop has the following sequence:

1. `Order`: Writes are enqueued on to the `WriteQueue`
2. `Buffer`: Writes are dequeued from the queue (possibly batched for efficiency)
3. `Apply`: Writes are applied to the single pending `Delta`
4. `Flush`: The pending delta is taken and passed to `Flusher::flush()`

These actions are pluggable, so different systems can compose them however they see fit. Here are
the APIs provided by this RFC:

```rust
/// Event broadcast to subscribers after each flush.
pub struct FlushEvent<D: Delta> {
    /// The new snapshot reflecting the flushed state.
    pub snapshot: Arc<StorageRead>,
    /// Clone of the delta that was flushed (pre-flush state).
    pub delta: D,
    /// Epoch range covered by this flush: (previous_max, new_max].
    /// A subscriber whose cache is at `epoch_range.0` can apply `delta`
    /// to update their state to `epoch_range.1`.
    pub epoch_range: (u64, u64),
}

pub trait Delta: Send + Clone + 'static {
    type Image: Send + Sync + 'static;
    type Write: Send + 'static;

    /// Initialize the delta with state from the image.
    fn init(&mut self, image: &Self::Image);
    /// Apply writes to this delta.
    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()>;
    /// Estimate the memory size of this delta for backpressure.
    fn estimate_size(&self) -> usize;
    /// Extract state needed for the next delta's initialization.
    /// Called before flush to enable non-blocking writes.
    fn fork_image(&self) -> Self::Image;
}

pub trait Flusher: Send + Sync + 'static {
    type Delta: Delta;

    /// Flush a delta to storage and return the new snapshot.
    async fn flush(&self, delta: Self::Delta) -> Result<StorageRead>;
}
```

The `WriteCoordinator` itself has the following APIs:

```rust
/// Durability levels for write acknowledgment.
pub enum Durability {
    /// Write has been applied to an in-memory delta.
    Applied,
    /// Write has been flushed to SlateDB memtable.
    Flushed,
    /// Write has been persisted to object storage.
    Durable,
}

/// Watchers for durability watermarks. Created per-handle via `sender.subscribe()` to ensure
/// each handle has independent cursor state.
struct FlushWatchers {
    applied: watch::Receiver<u64>,
    flushed: watch::Receiver<u64>,
    durable: watch::Receiver<u64>,
}

/// Handle returned from a write operation for tracking durability.
pub struct WriteHandle {
    epoch: Shared<oneshot::Receiver<u64>>,
    watchers: FlushWatchers,
}

impl WriteHandle {
    /// Returns the epoch assigned to this write. Epochs are assigned when the
    /// coordinator dequeues the write, so this method blocks until sequencing.
    /// Epochs are monotonically increasing and reflect the actual write order.
    pub async fn epoch(&self) -> Result<u64> {
        self.epoch.clone().await.map_err(|_| /* coordinator dropped */)
    }
    /// Wait until the write reaches the specified durability level.
    pub async fn wait(&self, durability: Durability) -> Result<()> {
        let epoch = self.epoch().await?;
        self.watchers.wait(epoch, durability).await
    }
}

/// Handle for interacting with the write coordinator.
pub struct WriteCoordinatorHandle<D: Delta> { /* ... */ }

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write and receive a handle to track its durability.
    pub async fn write(&self, event: D::Write) -> Result<WriteHandle> { /* ... */ }
    /// Request a flush up to the specified epoch (or all pending if None).
    pub async fn flush(&self, epoch: Option<u64>) -> Result<()> { /* ... */ }
    /// Subscribe to flush events.
    ///
    /// Each event contains the new snapshot, a clone of the flushed delta, and the
    /// epoch range covered. Subscribers can use this for incremental cache updates
    /// if their local state is at `epoch_range.0`.
    ///
    /// **Note**: Uses a `watch` channel — if flushes occur faster than the subscriber
    /// processes them, intermediate events are dropped. Subscribers that miss events
    /// must rebuild their cache from the snapshot.
    pub fn subscribe(&self) -> watch::Receiver<FlushEvent<D>> { /* ... */ }
}
```

The `WriteHandle` is used to allow readers to wait for the following durability guarantees:

| Watermark | Meaning                                                                 |
|-----------|-------------------------------------------------------------------------|
| `applied` | Highest epoch that has been applied to the pending delta                |
| `flushed` | Highest epoch reflected in the current image (delta applied to SlateDB) |
| `durable` | Highest epoch persisted to object storage (SlateDB WAL flush complete)  |

### Implementation

The coordinator runs as a single-threaded async loop that processes commands and manages flushes.

1. **Ordering**: all writes are serialized through a single `mpsc` channel (`WriteQueue`). Each
   enqueued write includes a `oneshot::Sender` for epoch delivery. Epochs are assigned when the
   coordinator dequeues and processes the write. This means `epoch()` must be awaited, but ordering
   is still guaranteed within a single async task: if `write(A).await` completes before
   `write(B).await` is called (sequential awaits), then `epoch(A) < epoch(B)` because both writes
   enter the channel in that order. This matches SlateDB's write semantics.
2. **Non-Blocking Flush**: when a flush is triggered:
   1. `fork_image()` is called on the current delta to extract state for the next delta
   2. The current delta is cloned for subscribers
   3. A new delta is created and initialized from the forked image
   4. The old delta is passed to the flusher (ownership transferred)
   5. Writes continue to the new delta while the flush runs in the background
   6. When flush completes, a `FlushEvent` is broadcast with the snapshot, cloned delta, and epoch range
3. **Flush Triggers**: flushes can be triggered manually via `flush()` or based on conditions
   evaluated on a timer. Flush requests are queued if a flush is in progress. A call to
   `flush(epoch)` is a no-op if `epoch` ≤ the already-flushed epoch.
4. **Epoch Tracking**: once a write is dequeued and assigned an epoch, the coordinator sends the
   epoch through the write's oneshot channel, unblocking any pending `epoch()` calls. The handle is
   automatically cleaned up when dropped (the oneshot is dropped with it). The coordinator holds
   `watch::Sender<u64>` for each durability level (applied/flushed/durable) and updates them as
   watermarks advance. Each `WriteHandle` gets its own receivers via `sender.subscribe()`, ensuring
   independent cursor state. `WriteHandle::wait()` awaits its epoch, then waits for the appropriate
   watermark to pass that epoch.
5. **Failure Propagation**: there are two modes of failure:
    1. failures that occur when applying a `Write` to the delta will be propagated to the caller via
       `WriteHandle::wait()`
    2. failures that happen during flushing are considered fatal and will cause coordinator to panic
6. **Backpressure**: backpressure is applied by both the channel capacity and the size of pending
   deltas. Flushes will be attempted on the ticker interval until backpressure conditions are
   released, during which time writes will fail with `Err(WriteError::Backpressure)`.

Readers that maintain in-memory caches can use `FlushEvent` to update incrementally. On each event,
the reader compares its local epoch against `epoch_range.0`. If they match, the reader applies the
delta to advance its state to `epoch_range.1`. If they don't match (the reader missed one or more
events), the cache is stale and must be rebuilt from the snapshot. 

### Subsystem Implementations

#### TimeSeries

TimeSeries identifies each series by a fingerprint (hash of sorted labels). During `init()`, the
delta clones the series dictionary and ID counter from the image. During `apply()`, it looks up or
creates a series ID for each fingerprint and appends samples — multiple writes to the same series
accumulate. Before flush, `fork_image()` extracts the current state so a new delta can continue
accepting writes.

```rust
impl Delta for TsdbDelta {
    type Image = TsdbImage;
    type Write = Series;

    fn init(&mut self, image: &Self::Image) {
        self.series_dict = image.series_dict.clone();  // HashMap<Fingerprint, SeriesId>
        self.next_series_id = image.next_series_id;    // u32
    }

    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()> {
        for series in writes {
            let fp = series.labels.fingerprint();
            let id = *self.series_dict.entry(fp).or_insert_with(|| {
                let id = self.next_series_id;
                self.next_series_id += 1;
                id
            });
            self.samples.entry(id).or_default().extend(series.samples);
        }
        Ok(())
    }

    fn fork_image(&self) -> Self::Image {
        TsdbImage {
            series_dict: self.series_dict.clone(),
            next_series_id: self.next_series_id,
        }
    }
}
```

The flusher converts the delta into storage records: forward index (series metadata), inverted
index (label to series ID mappings), and sample data. Since IDs were allocated during `apply()`,
the flusher just needs to persist everything atomically and return the new snapshot.

```rust
impl Flusher for TsdbFlusher {
    type Delta = TsdbDelta;

    async fn flush(&self, delta: TsdbDelta) -> Result<StorageRead> {
        let forward_index = build_forward_index(&delta);
        let inverted_index = build_inverted_index(&delta);

        let mut batch = WriteBatch::new();
        for (id, samples) in delta.samples() {
            batch.merge(samples_key(id), encode_samples(&samples));
        }
        for (id, spec) in forward_index {
            batch.put(forward_key(id), encode_spec(&spec));
        }
        for (label, ids) in inverted_index {
            batch.merge(inverted_key(&label), encode_bitmap(&ids));
        }

        self.db.write(batch).await?;
        Ok(self.db.snapshot())
    }
}
```

#### Vector

Vector identifies records by an external string ID provided by the user. During `init()`, the delta
gets the collection config for validation. During `apply()`, vectors are keyed by external ID with
last-write-wins semantics — if the same ID is written twice, the later write replaces the earlier
one. Internal ID allocation and upsert handling (marking old vectors as deleted) happen at flush
time in the `Flusher`.

```rust
impl Delta for VectorDelta {
    type Image = VectorImage;
    type Write = Vector;

    fn init(&mut self, image: &Self::Image) {
        self.config = image.config.clone();
    }

    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()> {
        for vector in writes {
            self.vectors.insert(vector.id.clone(), vector);  // last write wins
        }
        Ok(())
    }

    fn fork_image(&self) -> Self::Image {
        VectorImage {
            config: self.config.clone(),
        }
    }
}
```

The flusher handles internal ID allocation and upsert logic. For each vector, it checks if the
external ID already exists (upsert case), allocates a new internal ID, and marks the old vector
as deleted if necessary.

```rust
impl Flusher for VectorFlusher {
    type Delta = VectorDelta;

    async fn flush(&self, delta: VectorDelta) -> Result<StorageRead> {
        let mut batch = WriteBatch::new();
        for (external_id, vector) in delta.vectors {
            let old_id = self.db.get(id_mapping_key(&external_id)).await?;
            let new_id = self.id_allocator.next();

            batch.put(id_mapping_key(&external_id), new_id);
            batch.put(vector_key(new_id), encode_vector(&vector));

            if let Some(old_id) = old_id {
                batch.merge(deleted_bitmap_key(), encode_id(old_id));
                batch.delete(vector_key(old_id));
            }
        }
        self.db.write(batch).await?;
        Ok(self.db.snapshot())
    }
}
```

## Alternatives

### Simple Buffer Without Intermediate Processing

An earlier version of this RFC proposed a simpler model where the delta is just a buffer of writes
with no processing during `apply()`. All logic (ID allocation, deduplication, index building) would
happen at flush time. The delta would be keyed by natural identifiers (fingerprint for timeseries,
external_id for vector) and required a `merge()` method to combine writes to the same key.

This was rejected because:
- Processing during `apply` allows validation errors to be returned as non-fatal
- Subsystems may need to maintain invariants (e.g., series dictionary) across writes

### Mutex-Protected Delta

Instead of a queue, writes could directly acquire a mutex to apply to the delta:

```rust
impl WriteCoordinator {
    async fn write(&self, w: Write) -> Result<WriteHandle> {
        let mut delta = self.delta.lock().await;
        delta.apply(w)?;
        Ok(WriteHandle::new(self.epoch.fetch_add(1)))
    }
}
```

This was rejected because:
- Mutex contention under high write load
- Harder to implement batching (writes are applied one at a time)
- Backpressure is less natural (blocking on mutex vs. failing fast on full queue)

### Concurrent Writes (Current Code)

The current `timeseries` and `vector` implementations allow concurrent writes using lock-free
structures (`DashMap`, `AtomicU32`) with a separate flush mutex:

```rust
// Current pattern (simplified)
struct Db {
    delta: DashMap<Key, Value>,
    flush_mutex: Mutex<()>,
}

async fn write(&self, k: Key, v: Value) {
    self.delta.insert(k, v);  // concurrent writes OK
}

async fn flush(&self) {
    let _guard = self.flush_mutex.lock().await;  // serialize flushes
    // ... flush delta to storage
}
```

This was rejected because:
- Race conditions between concurrent writers (issues #82, #95)
- Ambiguous flush semantics — a `write()` followed by `flush()` may not include that write
- No clear epoch-based durability guarantees
- Harder to reason about correctness

## Future Considerations

### Sequencing Maintenance Operations

Some subsystems (e.g., vector index maintenance) may need to sequence internal operations alongside
user writes. For example, splitting a centroid requires: (1) marking the old centroid as draining,
(2) waiting until the change is readable, (3) reading vectors to compute reassignments.

The split between `id` (assigned at enqueue) and `epoch` (assigned at dequeue) enables this pattern.
Maintenance operations can be submitted through the same write channel as user writes:

```rust
// Submit the split command
let handle = coordinator.write(SplitCentroid { c, into: [c_0, c_1] }).await?;
// Wait until the split is readable
handle.wait(Durability::Flushed).await?;
// Now safe to read vectors from c for reassignment
let vectors = read_centroid_vectors(c).await?;
```

Because epochs are assigned at dequeue time, the split command receives an epoch that correctly
sequences it relative to concurrent user writes. The `wait(Flushed)` call blocks until the split
is visible to readers, ensuring the subsequent read sees the draining state.

Backpressure is handled client-side: if writes arrive faster than the index can be maintained, the
client is responsible for pausing ingest while maintenance catches up. This keeps the coordinator
simple and gives subsystems full control over their own flow control policies.

### Reading from the Pending Delta

This RFC scopes reads to flushed snapshots only — queries do not see unflushed data in the pending
delta. A future enhancement could expose the delta to readers, enabling read-your-writes semantics
and lower-latency queries.

The key challenge is synchronization: readers need a consistent view of both the snapshot and the
delta. Since the coordinator loop is single-threaded, we can leverage epochs to provide consistency:

1. Attach an epoch watermark to both the snapshot and the pending delta
2. When a reader subscribes, they receive `(snapshot, delta_ref, read_epoch)`
3. The reader merges results from both, filtering the delta to only include writes ≤ `read_epoch`
4. When a flush completes, the snapshot advances and the delta is cleared atomically (from the
   reader's perspective)

This approach allows read-your-writes without complex locking, but is deferred until there's a
concrete use case requiring sub-flush-interval read latency.

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-01-27 | Initial draft |
