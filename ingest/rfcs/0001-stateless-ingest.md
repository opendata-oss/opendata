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
│ Object Store │    │    │         │  ┌─┘                 │        │
│ Bucket       │    │    │         │  │  ┌────────────────┘        │
│  ┌───────────▼────▼────▼──┐  ┌───┼──┼──┼──────────────────────┐  │
│  │          data          │  │   │  │  │  queue               │  │
│  │                        │  │ ┌─▼──▼──▼────┐  ┌────────────┐ │  │
│  │  f47ac10b-58cc-...     │  │ │ producer-q │  │ consumer-q │ │  │
│  │  9b2e1c4d-83f5-...     │  │ └────▲───────┘  └────▲───────┘ │  │
│  │  3d6f4a9e-c2b7-...     │  │      │     ┌─────────┘         │  │
│  └───────────▲────────────┘  └──────┼─────┼───────────────────┘  │
└──────────────┼──────────────────────┼─────┼──────────────────────┘
               │                      │     │
               │                      │     │
               │        ┌─────────────┼─────┼──────┐
               │        │ Writer      │     │      │
               │        │        ┌────▼─────▼───┐  │     ┌────────────┐
               └────────┼────────▶  Collector   │  ├────▶│  Database  │
                        │        │  q-consumer  │  │     └────────────┘
                        │        └──────────────┘  │
                        └──────────────────────────┘
```

### Queue

The queue coordinates ingestors and collectors via two manifests in object storage,
the producer manifest (`producer-q` in the diagram) and the consumer manifest (`consumer-q` in the diagram).

The queue producers (used internally by the ingestors) write the locations of the batches of ingested data
to the producer manifest.
More specifically, a queue producer reads the producer manifest and loads the list of locations into memory.
It appends the location of the flushed batch to the end of the list and writes the manifest back to object storage.
Each write of the producer manifest is a CAS-write.
That means, a write only succeeds if the producer manifest has not been modified since it was read by the queue producer.
This ensures that locations appended to the producer manifest are not overwritten by other queue producers.
However, that also means that queue producers contend for appending their locations.

The queue consumer (used internally by the collector) reads the producer manifest to know the locations of the
data it needs to read next.
The queue consumer claims locations of the data batches that will be made available next.
Once the data is read and processed, the consumer can mark the location as done.
To avoid contention on the producer manifest, the queue consumer keeps claimed and done locations in the consumer manifest.
However, once in a while, the queue consumer needs to remove done locations from the producer manifest and
from the consumer manifest to avoid unbounded growth of those manifests.
After a configured number of done locations, the queue consumer cleans up both manifests.
In this way the queue consumer avoids contending too much with the queue producers for the producer manifest.

#### Manifest Format

Both manifests are stored as JSON files in object storage.

The producer manifest contains a list of pending locations:
```json
{
  "pending": [
    "data/f47ac10b-58cc-4372-a567-0e02b2c3d479.batch",
    "data/9b2e1c4d-83f5-4a1e-b9d7-6f8e2a3c5b10.batch",
    "data/3d6f4a9e-c2b7-41d8-9e5f-7a1b3c8d2e6f.batch"
  ]
}
```
The `pending` list preserves ingestion order. New locations are always appended to the end.
Locations are removed from the list during cleanup after they have been processed by the consumer.

The consumer manifest contains a map of claimed locations with their heartbeat timestamps
and a list of done locations:
```json
{
  "claimed": {
    "data/f47ac10b-58cc-4372-a567-0e02b2c3d479.batch": 1719400000000,
    "data/9b2e1c4d-83f5-4a1e-b9d7-6f8e2a3c5b10.batch": 1719400005000
  },
  "done": [
    "data/3d6f4a9e-c2b7-41d8-9e5f-7a1b3c8d2e6f.batch"
  ]
}
```
The `claimed` map associates each claimed location with a timestamp in milliseconds since the Unix epoch.
This timestamp is set when the location is claimed or refreshed by `heartbeat()`.
If the timestamp is older than `heartbeat_timeout_ms`, the location is considered stale and may be re-claimed
by another consumer.
The `done` list contains locations that have been fully processed. These locations remain in the consumer manifest
until the `done_cleanup_threshold` is reached, at which point they are removed from both the producer manifest's
`pending` list and the consumer manifest's `done` list.

### Ingestor

The ingestor provides an API to ingest a vector of key-value pairs.
The key-value pairs are buffered in a batch in ingestion order.
The ingestor flushes the batches of ingested key-value pairs to object storage and appends the locations of the
flushed objects to the queue with the internally used queue producer (`q-producer` in the diagram).
Flushes are triggered after a given time interval elapsed or if a batch of the ingested data exceeds a given size.

The API of the ingestor is the following:
```rust
impl Ingestor {
  pub fn new(config: IngestorConfig, clock: Arc<dyn Clock>) -> Result<Self> { ... }

  pub async fn ingest(&self, entries: Vec<KeyValueEntry>) -> Result<WriteWatcher> { ... }

  pub async fn close(self) -> Result<()> { ... }
}
```

An ingestor is constructed by calling the method `new()` passing to it the configuration and a clock.
The configurations for the ingestor are:

```rust
pub struct IngestorConfig {
  pub object_store_config: ObjectStoreConfig,  // configuration of the object store from opendata/common
  pub data_path_prefix: String,                // path prefix where to store the data objects, default: "ingest"
  pub manifest_path: String,                   // path to the queue manifest, default: "ingest/manifest.json"
  pub flush_interval: Duration,                // time interval that once elapsed triggers a flush of the
                                               // current batch to object storage, default: 100ms
  pub flush_size_bytes: usize,                 // size in bytes that triggers a flush if the current batch exceeds it,
                                               // default: 64 MiB
  pub max_unflushed_bytes: usize,              // limit in bytes that triggers backpressure, default: usize:MAX
}
```
The queue producer manifest takes the name specified in `manifest_path`.
The config `flush_size_bytes` is a loose limit.
The batch needs to exceed that size to trigger a flush to object storage.
The config `max_unflushed_bytes` is also a loose limit.
Each time the call to `ingest()` sees a size of unflushed key-value pairs in the ingestor that is larger
than `max_unflushed_bytes`, the call blocks and flushes will be triggered until the size of the unflushed key-value
pairs is less than `max_unflushed_bytes`.
If this backpressure blocking the ingestion becomes an issue, new ingestors can be created to better distribute the
load.

A call to `ingest()` takes a vector of key-value pairs and returns a `WriteWatcher` with which the caller can await
the completion of the flush to object storage of the vector of key-value pairs.

The `WriteWatcher` has the following API:
```rust
    pub fn result(&self) -> Option<Result<()>>

    pub async fn await_durable(&mut self) -> Result<()>
```
As soon as the call to `await_durable().await` returns or the call to `result()` is not `None`, the vector of key-value pairs
is stored in object storage and the location of the object that contains the vector of key-value pairs is appended to the
queue.
More specifically, the location is appended to the end of the list of pending locations in the producer queue manifest
(`producer-q` in the diagram).

Method `close()` flushes unflushed key-value pairs and terminates the ingestor.

### Data Batch Format

A data batch is the unit of data that an ingestor flushes to object storage.
Each batch is a JSON array of key-value entries serialized in ingestion order:
```json
[
  { "key": "Y3B1Lmhvc3QtMQ==", "value": "PHRpbWVzdGFtcD4..." },
  { "key": "Y3B1Lmhvc3QtMg==", "value": "dmFsdWUtYmluYXJ5..." }
]
```

Each entry is a `KeyValueEntry`:
```rust
pub struct KeyValueEntry {
    pub key: Bytes,    // serialized as base64
    pub value: Bytes,  // serialized as base64
}
```
Both `key` and `value` are opaque byte blobs encoded as base64 in the JSON representation.
Their semantics are defined by the database that consumes the data
(e.g., the key could be a metric name for Timeseries or a stream name for Log).
The ingest module does not interpret keys or values; it preserves them as-is.

Each batch is stored under the configured `data_path_prefix` with a UUID v4 filename
(e.g., `data/f47ac10b-58cc-4372-a567-0e02b2c3d479.json`).
The location (object storage path) of the batch is then enqueued in the producer manifest
so the collector can discover and read it.


### Collector

The collector reads the locations of the ingested key-value pairs in ingestion order from the queue with the
internal queue consumer (`q-consumer` in the diagram) and returns the batches of key-value pairs.

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
    pub manifest_path: String,                   // path to the queue manifest, default: "ingest/manifest.json"
    pub heartbeat_timeout_ms: Duration,          // duration after which a claimed location is considered failed,
                                                 // default: 30s
    pub done_cleanup_threshold: usize,           // number of done locations at which a cleanup of the locations
                                                 // and corresponding data batches is triggered, default: 100
}
```
The queue producer manifest takes the name specified in `manifest_path`.
The queue consumer manifest is derived from `manifest_path` by adding `consumer` in front of the file extension
or to the end of the `manifest_path` if the path does not have an extension.

The collector internally creates a queue consumer and an object store client from the configuration.
It also spawns a background heartbeat task that periodically refreshes the timestamps of all
in-flight batches in the consumer manifest. The heartbeat interval is derived from the configuration
as `heartbeat_timeout_ms / 3`, ensuring claims stay fresh well before they expire.

A `CollectedBatch` contains the deserialized key-value entries and the location of the batch:
```rust
pub struct CollectedBatch {
    pub entries: Vec<KeyValueEntry>,
    location: String,
}
```

By calling `next_batch()` the collector claims the next available location from the queue, reads the
data batch from object storage, deserializes the key-value entries, and returns them as a `CollectedBatch`.
The batch is tracked as in-flight so the background heartbeat keeps it alive.
If no location is available in the queue, `None` is returned.

By calling `ack()` the caller confirms that the batch has been fully processed.
The method removes the batch from the in-flight set so the background heartbeat stops refreshing it,
and calls `dequeue()` on the queue consumer to mark the location as done.
This may trigger cleanup of the manifests and the data batch in object storage if the
`done_cleanup_threshold` is reached.

When the collector is dropped, the background heartbeat task is cancelled.
In-flight batches that are not acknowledged will eventually become stale and can be re-claimed
by another collector.

### Observability

TBD

Some ideas:
- conflict rate for producer and consumer manifest
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


### Using one single manifest for the queue

With a very fast collector, the contention on the manifest would be quite high.
This contention would also affect the queue producers.
With a queue producer manifest and a queue consumer manifest, we can better decouple producer contention from
consumer contention.

## Open Questions

- What format should we use for the data batches?
- Should the clean-up of the data batches happen independently of the clean-up of the queue?
- Should we also track the age of claimed/done batches to see if batches fall too much behind?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-02-26 | Initial draft |



