# opendata-buffer

A shared, stateless buffer library for [OpenData](https://github.com/opendata-oss/opendata) databases.

Provides write-path infrastructure that all OpenData databases (Timeseries, Log, Vector) can reuse. Producers accept opaque byte entries, accumulate them in memory, and periodically flush batched data files to object storage. A manifest-backed queue coordinates producers and consumers in a stateless, crash-safe way.

## Why a stateless buffer?

- **Fault tolerance** — producers are stateless. If one fails, any other running producer can take over without a rebalancing protocol.
- **Decoupled from writes** — if the downstream database is slow or unavailable, buffered data is safely persisted in object storage rather than dropped or back-pressured.
- **Cost savings** — data flows through object storage rather than across availability zones, avoiding cross-zonal transfer fees.

## Architecture

```text
╔═Producers══════════════════════════════════════════════════╗
║                                                            ║░
║  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  ║░
║  │  Producer 1  │    │  Producer 2  │    │  Producer N  │  ║░
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
       ╔═Consumer╪═══════╪═════════╗
       ║        │        │         ║░
       ║        │┌───────┴──────┐  ║░     ╔════════════════╗
       ║        ││   Consumer   │  ║░     ║                ║
       ║        └▶  q-consumer  ├──write──▶    Database    ║
       ║         └──────────────┘  ║░     ║                ║
       ║                           ║░     ╚════════════════╝
       ╚═══════════════════════════╝░
        ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

## Usage

### Producer

The producer accumulates entries and flushes them as compressed batches to object storage, appending their locations to the queue manifest.

```rust
use buffer::{Producer, ProducerConfig};
use bytes::Bytes;
use std::sync::Arc;

let producer = Producer::new(ProducerConfig::default(), clock)?;

// Submit entries with metadata — returns a handle to await durability
let handle = producer.produce(
    vec![Bytes::from("entry-1"), Bytes::from("entry-2")],
    Bytes::from("my-metadata"),
).await?;

// Block until the batch is flushed to object storage and enqueued
handle.watcher.await_durable().await?;

// Flush remaining entries and shut down
producer.close().await?;
```

#### Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `flush_interval` | 100 ms | Time interval that triggers a flush |
| `flush_size_bytes` | 64 MiB | Batch size threshold that triggers a flush |
| `max_buffered_inputs` | 1000 | Max buffered `produce()` calls before backpressure |
| `batch_compression` | `None` | Compression algorithm (`None` or `Zstd`) |
| `data_path_prefix` | `"ingest"` | Object storage prefix for data batches |
| `manifest_path` | `"ingest/manifest"` | Path to the queue manifest |

### Consumer

The consumer reads batches from the queue in ingestion order and makes them available to a database writer.

```rust
use buffer::{Consumer, ConsumerConfig};

let consumer = Consumer::new(ConsumerConfig::default(), clock)?;

// Initialize the consumer — fences any previous consumer via epoch bump
consumer.initialize(None).await?;

// Read batches in order
while let Some(batch) = consumer.next_batch().await? {
    // batch.entries: Vec<Bytes>
    // batch.sequence: u64
    // batch.metadata: Vec<Metadata>
    process(&batch);
    consumer.ack(batch.sequence).await?;
}

// Force-flush acked entries from the manifest
consumer.flush().await?;
```

## Delivery guarantees

Exactly-once delivery is achievable when the caller atomically writes both the batch and its sequence number to the downstream database. After a failure, the consumer resumes from the last committed sequence — no data is processed twice.

On the producer side, callers that track progress and re-submit unacknowledged entries achieve at-least-once delivery.

## CLI

A companion CLI is included for inspecting buffer state. Install with:

```bash
cargo install opendata-buffer --features cli
```

### `manifest dump`

Deserializes a manifest file to JSON:

```bash
opendata-buffer manifest dump /path/to/manifest
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
opendata-buffer manifest dump /path/to/manifest | jq '.entries | length'
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
