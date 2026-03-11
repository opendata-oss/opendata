# RFC 0001: Stateless Ingest

**Status**: Draft

**Authors**:

- [Bruno Cadonna](https://github.com/cadonna)

## Summary

This RFC proposes a shared, stateless ingest module for OpenData.
The module provides the core write-path infrastructure that all OpenData databases
(Timeseries, Log, Vector, and future databases) can reuse.
Stateless ingest consists of multiple components -- called ingestors -- that accept write entries from an API layer,
accumulate them in memory, and periodically flush batched data files to object storage.
On the read side of the ingestion, a collector reads the batches and makes them available to a writer of a database.
Each ingestor accepts any data, i.e., no mapping from specific ingest data partitions to specific ingestor exist.
If an ingestor fails, any other still running ingestor can take over the work without running any rebalancing protocol.
A manifest-backed queue coordinates producers (ingestors) and consumers (collectors) in a stateless, crash-safe way.
Stateless ingest enables simple high availability ingest and ingest scaling in OpenData systems.


## Motivation

Data ingestion to an OpenData system should never lose data or block upstream systems.
If the actual database fails or cannot keep up with the load, data should not be silently dropped or
cause backpressure for upstream systems.
For example, if OpenData-timeseries fails, metrics from upstream systems should not be dropped.

To avoid data loss and blocking, data ingestion needs to be decoupled from the actual write into the OpenData system.
The ingestion needs to be fault-tolerant and it needs to be highly-available, that means it needs to scale
independently from the actual write into the OpenData system.
A stateless ingestion allows simple fail-over and scaling.

Another big motivation for stateless ingest through object storage is cost savings.
The ingested data does not need to be sent to the collector that might run in a different availability zones.
In other words, data does not need to cross zones which saves costs because reads from object storage in
different zones do not incur cross-zonal transfer fees.


## Goals

- Design ingest that is stateless, fault-tolerant, highly-available
- Specify how the ingest makes data available to the collector that inserts the data to the OpenData system.
- Specify how the collector can read the ingested data in the correct ingest order including the fail-over case.


## Non-Goals

- Specify how unacknowledged entries can be re-ingested in case of a re-start.
  Progress handling is the responsibility of the system that uses the ingestors.
- Specify fail-over for the external system that uses the ingestors.
- Specify a service that accepts data to ingest.


## Design

```ascii
            ┌────────────┐    ┌────────────┐    ┌────────────┐
            │ Ingestor 1 │    │ Ingestor 2 │    │ Ingestor N │
            │ q-producer │    │ q-producer │    │ q-producer │
            └──┬──────┬──┘    └──┬──────┬──┘    └────┬────┬──┘
               │    ┌─┼──────────┘      │            │    │
               │    │ └────────────┐    │            │    │
               │    │    ┌─────────┼────┼────────────┘    │
┌──────────────┼────┼────┼─────────┼────┼─────────────────┼────────┐
│ Object Store │    │    │         │    └────┐            │        │
│ Bucket       │    │    │         └──────┐  │  ┌─────────┘        │
│  ┌───────────▼────▼────▼──┐  ┌──────────┼──┼──┼───────────────┐  │
│  │          data          │  │  queue   │  │  │               │  │
│  │                        │  │        ┌─▼──▼──▼────┐          │  │
│  │  01J5T4R3KXBMZ7...     │  │        │ q-manifest │          │  │
│  │  01J5T4R7NP39QW...     │  │        └────▲───────┘          │  │
│  │  01J5T4RBYW52MK...     │  │             │                  │  │
│  └───────────▲────────────┘  └─────────────┼──────────────────┘  │
└──────────────┼─────────────────────────────┼─────────────────────┘
               │                             │
               │                             │
               │        ┌────────────────────┼─────┐
               │        │ Writer             │     │
               │        │        ┌───────────▼──┐  │     ┌────────────┐
               └────────┼────────▶  Collector   │  ├────▶│  Database  │
                        │        │  q-consumer  │  │     └────────────┘
                        │        └──────────────┘  │
                        └──────────────────────────┘
```

### Queue

The queue coordinates ingestors and collectors via a single queue manifest in object storage
(`q-manifest` in the diagram).

The queue producers (used internally by the ingestors) write the locations of the batches of ingested data
to the queue manifest.
More specifically, a queue producer reads the manifest and loads the list of locations into memory.
It appends the location of the flushed batch to the end of the list and writes the manifest back to object storage.
Each write of the queue manifest is a CAS-write.
That means, a write only succeeds if the queue manifest has not been modified since it was read by the queue producer.
This ensures that locations appended to the queue manifest are not overwritten by other queue producers.
However, that also means that queue producers contend for appending their locations.

The queue consumer (used internally by the collector) reads the queue manifest to know the locations of the
data it needs to read next.
On startup, the consumer increments an `epoch` counter stored in the queue manifest footer.
Only a consumer whose epoch matches the manifest's current epoch may perform queue operations.
If a new consumer starts and increments the epoch, any previous consumer is fenced —
its subsequent queue operations fail with a `Fenced` error.
This epoch-based fencing replaces heartbeat-based claim tracking and ensures
only a single active consumer processes entries at any time.

When the consumer has processed entries, it calls `dequeue(through_sequence)` which removes
all entries with sequence numbers up to and including `through_sequence` from the queue manifest.

#### Manifest Format

The queue manifest uses a compact binary format that supports appending new entries
without deserializing existing entries. Each entry records the object storage location
and a list of metadata items that describe ranges of records in the data batch:

```ascii
┌──────────────────────────────────────────────────────────────┐
│  entry 0: [entry_len: u32 LE][sequence: u64 LE]              │
│           [location_len: u16 LE][location: bytes]            │
│           [metadata_count: u32 LE]                           │
│           [metadata 0: [start_index: u32 LE]                 │
│                        [ingestion_time_ms: i64 LE]           │
│                        [payload_len: u32 LE]                 │
│                        [payload: bytes]]                     │
│           [metadata 1: ...]                                  │
│  entry 1: ...                                                │
│  ...                                                         │
│  entry N: ...                                                │
├──────────────────────────────────────────────────────────────┤
│  footer (22 bytes):                                          │
│    entry_count    : u32 LE                                   │
│    next_sequence  : u64 LE                                   │
│    epoch          : u64 LE                                   │
│    version        : u16 LE  (= 1)                            │
└──────────────────────────────────────────────────────────────┘
```

- `entry_len` is the total number of bytes after this field:
  `8 + 2 + location_len + 4 + Σ(4 + 8 + 4 + payload_i_len)`.
- `sequence` is a monotonically increasing u64, auto-assigned by the manifest on append.
  Sequences are contiguous but can start at any value (e.g., after dequeue, entries 5,6,7 are valid).
- `location` is the UTF-8 encoded object storage path of the data batch.
- `metadata_count` is the number of metadata items in this entry. Each metadata item describes a range
  of records in the data batch that share the same metadata and ingestion time.
- Each metadata item contains:
  - `start_index` (u32 LE): the index of the first record in the data batch to which this metadata applies.
    The range extends to the next metadata item's `start_index` or to the end of the batch.
  - `ingestion_time_ms` (i64 LE): wall-clock time in milliseconds since the Unix epoch when
    the records in this range were ingested.
  - `payload_len` (u32 LE) and `payload` (bytes): an opaque byte payload of application-defined metadata.
- The footer is always the last 22 bytes. `next_sequence` stores the sequence number that will be assigned
  to the next appended entry. `epoch` is a monotonically increasing counter used for consumer fencing:
  a new consumer increments the epoch on initialization, and only a consumer whose epoch matches the
  manifest's epoch may perform queue operations. It allows readers to verify the entry count and detect format changes.

To append a new entry a producer reads the raw bytes, strips the 22-byte footer, appends the encoded entry
(with the sequence number from `next_sequence`), and writes a new footer with the incremented entry count
and incremented `next_sequence`.
Existing entries are never deserialized during append, which keeps the write path O(1) in the number of entries.
Entries are listed in ingestion order. A `dequeue(n)` operation removes all entries with `sequence <= n`
and returns them.

The queue guarantees that entries are processed in ingestion order within a single active consumer.
If a consumer fails, the new consumer increments the epoch, fencing the old one,
and resumes processing from the earliest unprocessed entry in the queue manifest.


### Ingestor

The ingestor provides an API to ingest a vector of opaque byte entries with associated metadata.
The entries are buffered in a batch in ingestion order.
The ingestor flushes the batches of ingested entries to object storage and appends the locations of the
flushed objects to the queue with the internally used queue producer (`q-producer` in the diagram).
Flushes are triggered after a given time interval elapsed or if a batch of the ingested data exceeds a given size.

The API of the ingestor is the following:
```rust
impl Ingestor {
  pub fn new(config: IngestorConfig, clock: Arc<dyn Clock>) -> Result<Self> { ... }

  pub async fn ingest(&self, entries: Vec<Bytes>, metadata: Bytes) -> Result<WriteHandle> { ... }

  pub async fn close(self) -> Result<()> { ... }
}
```

An ingestor is constructed by calling the method `new()` passing to it the configuration and a clock.
The configurations for the ingestor are:

```rust
pub struct IngestorConfig {
  /// Determines where and how ingest data is persisted. See [`StorageConfig`].
  pub storage: StorageConfig,

  /// Path prefix for data batch objects in object storage.
  ///
  /// Defaults to `"ingest"`.
  pub data_path_prefix: String,

  /// Path to the queue manifest in object storage.
  ///
  /// Defaults to `"ingest/manifest"`.
  pub manifest_path: String,

  /// Time interval that triggers the flush of the current batch to object storage when elapsed.
  ///
  /// Defaults to 100 ms.
  pub flush_interval: Duration,

  /// Batch size in bytes that triggers a flush when exceeded.
  ///
  /// Defaults to 64 MiB.
  pub flush_size_bytes: usize,

  /// Maximum number of input entries vectors that can be buffered before backpressure is applied.
  ///
  /// Defaults to 1000.
  pub max_buffered_inputs: usize,
}
```
The queue manifest takes the name specified in `manifest_path`.
The config `flush_size_bytes` is a loose limit.
The batch needs to exceed that size to trigger a flush to object storage.
The config `max_buffered_inputs` limits the number of `ingest()` calls that can be buffered. 
When the buffer is full, `ingest()` blocks until the background task consumes a message.
If this backpressure becomes an issue, new ingestors can be created to better distribute the load.

A call to `ingest()` takes a vector of opaque byte entries and a metadata payload, and returns a `WriteHandle`
with which the caller can await the completion of the flush to object storage of the data entries.
Because multiple `ingest()` calls may be batched into a single flush, each call's metadata is stored
as a separate metadata item in the queue manifest entry with a `start_index` pointing to the first record
in the data batch that the metadata applies to. The ingestion time is also recorded per metadata item.
The collector can use the metadata items to interpret ranges of records in the batch without reading it.

The `WriteHandle` contains a `DurabilityWatcher` that allows the caller to check or await durability:
```rust
pub struct WriteHandle {
    pub watcher: DurabilityWatcher,
}

impl DurabilityWatcher {
    pub fn result(&self) -> Option<Result<()>>

    pub async fn await_durable(&mut self) -> Result<()>
}
```
As soon as the call to `watcher.await_durable().await` returns or the call to `watcher.result()` is not `None`,
the vector of entries is stored in object storage and the location of the object that contains the vector of entries
is appended to the queue.
More specifically, the location is appended to the end of the list of pending locations in the queue manifest
(`q-manifest` in the diagram).

Method `close()` flushes unflushed entries and terminates the ingestor.

### Data Batch Format

A data batch is the unit of data that an ingestor flushes to object storage.
Each batch is a compact binary file that contains a sequence of length-prefixed records followed by a fixed-size footer:

```ascii
┌──────────────────────────────────┐
│  record 0: [len: u32 LE][data]   │
│  record 1: [len: u32 LE][data]   │
│  ...                             │
│  record N: [len: u32 LE][data]   │
├──────────────────────────────────┤
│  footer (6 bytes, fixed):        │
│    record_count : u32 LE         │
│    version      : u16 LE         │
└──────────────────────────────────┘
```

Each record stores one opaque byte entry preceded by its length as a little-endian `u32`.
Records are written in ingestion order.
The footer is always the last 6 bytes of the file: a little-endian `u32` record count followed by a little-endian `u16` version.
The current version is `1`.
The footer allows a reader to verify the record count and detect format changes.

The semantics of the entries are defined by the database that consumes the data.
The ingest module does not interpret the entries; it preserves them as-is.

Each batch is stored under the configured `data_path_prefix` with a ULID filename
(e.g., `data/01J5T4R3KXBMZ7QV9N2WG8YDHP.batch`).
The location (object storage path) of the batch is then enqueued in the queue manifest
so the collector can discover and read it.


### Collector

The collector reads the locations of the ingested entries in ingestion order from the queue with the
internal queue consumer (`q-consumer` in the diagram) and returns the batches of entries.

The API of the collector is the following:
```rust
impl Collector {
  pub fn new(config: CollectorConfig, clock: Arc<dyn Clock>) -> Result<Self> { ... }

  pub async fn next_batch(&self) -> Result<Option<CollectedBatch>> { ... }

  pub async fn ack(&self, batch: &CollectedBatch) -> Result<()> { ... }
}
```

A collector is constructed by calling `new()` passing to it the configuration and a clock.
The configurations for the collector are:
```rust
pub struct CollectorConfig {
    pub object_store_config: ObjectStoreConfig,  // configuration of the object store from opendata/common
    pub manifest_path: String,                   // path to the queue manifest, default: "ingest/manifest"
}
```
The queue manifest takes the name specified in `manifest_path`.

The collector internally creates a queue consumer and an object store client from the configuration.
On startup, the queue consumer initializes by incrementing the epoch in the queue manifest,
fencing any previous consumer instance.

A `CollectedBatch` contains the deserialized entries and the location of the batch:
```rust
pub struct CollectedBatch {
    pub entries: Vec<Bytes>,
    location: String,
}
```

By calling `next_batch()` the collector iterates over the entries in the queue manifest via the
queue consumer, reads the next data batch from object storage, deserializes the entries, and returns
them as a `CollectedBatch`.
If no entries are available in the queue, `None` is returned.

By calling `ack()` the caller confirms that the batch has been fully processed.
The method calls `dequeue()` on the queue consumer to remove the acknowledged entries from the
queue manifest.

If the collector fails, a new collector can be started. It will increment the epoch,
fencing the old consumer, and resume from the earliest unprocessed entry.

### Delivery guarantees

Once entries are confirmed to be durable they are guaranteed to be delivered to the caller of the collector 
at least once.
If the collector fails after it claimed a location and processed the corresponding batch, but before it was able to 
acknowledge the batch, the new collector will re-read the batch and re-process it. 

The delivery guarantees of the entries that were not confirmed to be durable depends on the progress tracking of the
caller of the stateless ingest.
If they track the progress and re-ingest unacknowledged entries they can achieve at-least-once guarantee.

### Observability

TBD

Some ideas:
- conflict rate for queue manifest
- queue length
- size of all data batches

## Alternatives

### Stateless broker for the queue

To avoid contention on the queue manifest files, a stateless broker can be implemented that is responsible to write
the queue manifest files.
This idea comes from the following turbopuffer blog post: https://turbopuffer.com/blog/object-storage-queue.
Since we assume one ingestor per availability zone and a single collector which limits the contention, we decided
against the broker.
One reason for deciding against the broker is that the broker would be an additional component that a user needs to
deploy and operate.
We want to keep the ingest as simple as possible.
In the future, it might be necessary to revise this decision and implement the stateless broker approach if
contention gets worse.


### Using two manifests for the queue (previous approach)

The previous design used a separate consumer manifest (JSON) to track claimed and done locations
with heartbeat-based timeouts. This was replaced by epoch-based fencing in the single queue manifest,
which simplifies the design for the single-consumer model and eliminates the need for heartbeats,
stale reclaim logic, and a separate consumer manifest file.


### Using a counter for the batch names

We could use a counter for the batch names to impose an order.
During the flushing of batches, the ingestor:
1. lists the prefix to which the batches are written, 
2. finds the name with the largest number, 
3. increases it, and 
4. conditionally tries to write the batch named with the increased number.

If the write fails because a batch with that name already exists, the ingestor increases the number again, 
tries to write the batch again, and so forth.

The collector:
1. lists the prefix to which the batches are written,
2. finds the batch with the lowest number,
3. makes that batch available.

Once the batch is acknowledged, the batch is removed from the prefix.

It is not clear if the conflict resolution would have a higher latency 
since the conflict would be on the data batches and the conditional write is checked at the end of the write.
That means, that maybe all the data is first send to object storage to then discover that there is a different
object with the same name. This aspect would require some experiments.


## Open Questions

None at this time.


## Updates

| Date       | Description                                |
|------------|--------------------------------------------|
| 2026-02-26 | Initial draft                              |
| 2026-03-05 | Added binary formats for queue and batches |
| 2026-03-10 | Changed queue entry metadata to a vector of length-prefixed items |
| 2026-03-10 | Moved durability API into DurabilityWatcher inside WriteHandle |
| 2026-03-10 | Changed ingest() to take Vec\<IngestEntry\> with per-entry data and metadata |
| 2026-03-11 | Replaced max_unflushed_bytes with max_buffered_inputs using a bounded channel |
| 2026-03-11 | Changed queue metadata to per-range format with start_index and ingestion_time per metadata item |
| 2026-03-11 | Replaced IngestEntry with ingest(entries: Vec\<Bytes\>, metadata: Bytes) |



