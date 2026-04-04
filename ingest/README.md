# opendata-ingest

A shared, stateless ingestion library for [OpenData](https://github.com/opendata-oss/opendata) databases.

Provides write-path infrastructure that all OpenData databases (Timeseries, Log, Vector) can reuse. Ingestors accept opaque byte entries, buffer them in memory, and periodically flush batched data files to object storage. A manifest-backed queue coordinates producers (ingestors) and consumers (collectors) in a stateless, crash-safe way.

## Why stateless ingest?

- **Fault tolerance** — ingestors are stateless. If one fails, any other running ingestor can take over without a rebalancing protocol.
- **Decoupled from writes** — if the downstream database is slow or unavailable, ingested data is safely persisted in object storage rather than dropped or back-pressured.
- **Cost savings** — data flows through object storage rather than across availability zones, avoiding cross-zonal transfer fees.

## Architecture

```text
╔═Ingestors══════════════════════════════════════════════════╗
║                                                            ║░
║  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  ║░
║  │  Ingestor 1  │    │  Ingestor 2  │    │  Ingestor N  │  ║░
║  │  q-producer  │    │  q-producer  │    │  q-producer  │  ║░
║  └───────┬──────┘    └──────────────┘    └───────┬──────┘  ║░
║          │                                       │         ║░
╚══════════╪═══════════════════════════════════════╪═════════╝░
 ░░░░░░░░░░│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│░░░░░░░░░░░
           └flush                            enqueue
                │                             │
╔═Object Store Bucket═════════════════════════╪══════════════╗
║               │                             │              ║░
║               │                             │              ║░
║  ┏━data━━━━━━━▼━━━━━━━━━━━┓     ┏━queue━━━━━▼━━━━━━━━━━┓   ║░
║  ┃                        ┃     ┃                      ┃   ║░
║  ┃                        ┃     ┃  ╔════════════════╗  ┃   ║░
║  ┃ 01J5T4R3.batch         ┃     ┃  ║                ║  ┃   ║░
║  ┃ 01J5T4R7.batch         ┃     ┃  ║   q-manifest   ║  ┃   ║░
║  ┃ 01J5T4RB.batch         ┃     ┃  ║                ║  ┃   ║░
║  ┃                        ┃     ┃  ╚════════════════╝  ┃   ║░
║  ┃                        ┃     ┃                      ┃   ║░
║  ┃                        ┃     ┃                      ┃   ║░
║  ┗━━━━━━━━━━━━┬━━━━━━━━━━━┛     ┗━━━━━━━━━━━▲━━━━━━━━━━┛   ║░
║               │                             │              ║░
║               │                             │              ║░
╚═══════════════╪═════════════════════════════╪══════════════╝░
 ░░░░░░░░░░░░░░░│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│░░░░░░░░░░░░░░░░
                │        ┌───────poll─────────┘
              read       │
                │        │
       ╔═Writer═╪════════╪═════════╗
       ║        │        │         ║░
       ║        │┌───────┴──────┐  ║░     ╔════════════════╗
       ║        ││  Collector   │  ║░     ║                ║
       ║        └▶  q-consumer  ├──write──▶    Database    ║
       ║         └──────────────┘  ║░     ║                ║
       ║                           ║░     ╚════════════════╝
       ╚═══════════════════════════╝░
        ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

## Usage

### Ingestor

The ingestor buffers entries and flushes them as compressed batches to object storage, appending their locations to the queue manifest.

```rust
use ingest::{Ingestor, IngestorConfig};
use bytes::Bytes;
use std::sync::Arc;

let ingestor = Ingestor::new(IngestorConfig::default(), clock)?;

// Ingest entries with metadata — returns a handle to await durability
let handle = ingestor.ingest(
    vec![Bytes::from("entry-1"), Bytes::from("entry-2")],
    Bytes::from("my-metadata"),
).await?;

// Block until the batch is flushed to object storage and enqueued
handle.watcher.await_durable().await?;

// Flush remaining entries and shut down
ingestor.close().await?;
```

#### Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `flush_interval` | 100 ms | Time interval that triggers a flush |
| `flush_size_bytes` | 64 MiB | Batch size threshold that triggers a flush |
| `max_buffered_inputs` | 1000 | Max buffered `ingest()` calls before backpressure |
| `batch_compression` | `None` | Compression algorithm (`None` or `Zstd`) |
| `data_path_prefix` | `"ingest"` | Object storage prefix for data batches |
| `manifest_path` | `"ingest/manifest"` | Path to the queue manifest |

### Collector

The collector reads batches from the queue in ingestion order and makes them available to a database writer.

```rust
use ingest::{Collector, CollectorConfig};

let collector = Collector::new(CollectorConfig::default(), clock)?;

// Initialize the consumer — fences any previous collector via epoch bump
collector.initialize(None).await?;

// Read batches in order
while let Some(batch) = collector.next_batch().await? {
    // batch.entries: Vec<Bytes>
    // batch.sequence: u64
    // batch.metadata: Vec<Metadata>
    process(&batch);
    collector.ack(batch.sequence).await?;
}

// Force-flush acked entries from the manifest
collector.flush().await?;
```

## Delivery guarantees

Exactly-once delivery is achievable when the caller atomically writes both the batch and its sequence number to the downstream database. After a failure, the collector resumes from the last committed sequence — no data is processed twice.

On the ingestor side, callers that track progress and re-ingest unacknowledged entries achieve at-least-once delivery.

## CLI

A companion CLI is included for inspecting ingest state. Install with:

```bash
cargo install opendata-ingest --features cli
```

### `manifest dump`

Deserializes a manifest file to JSON:

```bash
opendata-ingest manifest dump /path/to/manifest
```

```json
{
  "version": 1,
  "epoch": 0,
  "next_sequence": 3,
  "entries": [
    {
      "sequence": 0,
      "location": "ingest/01J5T4R3.batch",
      "metadata": [
        {
          "start_index": 0,
          "ingestion_time_ms": 1712345678000,
          "payload_base64": "bXktbWV0YWRhdGE="
        }
      ]
    }
  ]
}
```

Pipe through `jq` for filtering:

```bash
opendata-ingest manifest dump /path/to/manifest | jq '.entries | length'
```

## Data batch format

Each batch is a compact binary file with an optionally compressed block of length-prefixed records followed by a fixed-size footer:

```text
+------------------------------------------+
|  compressed record block (variable):     |
|    record 0: [len: u32 LE][data]         |
|    record 1: [len: u32 LE][data]         |
|    ...                                   |
+------------------------------------------+
|  footer (7 bytes):                       |
|    compression_type : u8                 |
|    record_count     : u32 LE             |
|    version          : u16 LE  (= 1)     |
+------------------------------------------+
```
