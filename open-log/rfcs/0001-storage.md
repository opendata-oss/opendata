# RFC 0001: Open-Log Storage

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the storage model and core API for open-log. Keys are stored directly in the LSM with a sequence number suffix, enabling per-key log streams with global ordering. The API provides append and scan operations mirroring SlateDB's interface.

## Motivation

Open-Log is a key-oriented log system inspired by Kafka, but with a simpler model: keys are user-defined and every key is logically its own independent log. There is no concept of partitions or repartitioning—users simply write to new keys when their access patterns change.

Logs are stored in SlateDB's LSM tree. Writes are appended to the WAL and memtable, then flushed to sorted string tables (SSTs). LSM compaction naturally organizes data for log locality, grouping entries by key prefix over time. This provides efficient sequential reads for individual logs without requiring explicit partitioning infrastructure.

## Goals

- Define a write API for appending entries to a key's log
- Define a scan API for reading entries from a key's log

## Non-Goals (left for future RFCs)

- Active polling or push-based consumption
- Retention policies
- Checkpoints and cloning
- Key-range scans

## Design

### Key Encoding

SlateDB keys are a composite of the user key and a `u64` sequence number. A leading byte discriminator distinguishes entry types.

```
Log Entry:
  SlateDB Key:   | 0x01 | key (bytes) | sequence (u64) |
  SlateDB Value: | record value (bytes) |
```

The `0x01` discriminator is reserved for log entries. Additional record types (e.g., metadata, indexes) may be introduced in future RFCs using different discriminators.

This encoding preserves lexicographic key ordering, enabling key-range scans. Entries for the same key are ordered by sequence number.

#### Variable-Length Key Considerations

With variable-length keys, the boundary between key and sequence number is ambiguous during comparison. Two approaches to handle this:

**Custom comparator** — SlateDB could support a custom comparator that treats the key and sequence number as separate components. This would enable correct ordering without encoding changes.

**Scan filtering** — Without a custom comparator, scans may return entries from adjacent keys when key bytes happen to interleave with sequence number bytes. Results can be filtered by exact key match.

In practice, users are likely to use fixed-length keys, which avoids this issue entirely.

### Sequence Numbers

Sequence numbers are assigned from a global counter that increments on every append. Each key's log is monotonically ordered by sequence number, but the sequence numbers are not contiguous—other keys' appends are interleaved in the global sequence.

This approach simplifies ingestion by avoiding per-key sequence tracking. The trade-off is that sequence numbers do not reflect the count of entries within a key's log.

### Write API

The write API mirrors SlateDB's `write` API. The only supported operation is `append`.

```rust
struct Record {
    key: Bytes,
    value: Bytes,
}

struct WriteOptions {
    await_durable: bool,
}

impl OpenLog {
    async fn append(&self, record: Record, options: WriteOptions) -> Result<(), Error>;
    async fn append_batch(&self, records: Vec<Record>, options: WriteOptions) -> Result<(), Error>;
}
```

### Scan API

The scan API mirrors SlateDB's scan API. A key is provided along with a sequence number range.

```rust
struct LogEntry {
    key: Bytes,
    sequence: u64,
    value: Bytes,
}

// TODO: decide which SlateDB ScanOptions parameters to pass through
struct ScanOptions {
}

struct ScanIterator { ... }

impl ScanIterator {
    async fn next(&mut self) -> Result<Option<LogEntry>, Error>;
}

impl OpenLog {
    fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64>, options: ScanOptions) -> ScanIterator;
}
```

### Lag and Count (under consideration)

Without contiguous sequence numbers, computing lag requires additional bookkeeping. The approach under consideration augments SlateDB data structures:

- Each SST tracks the number of records it contains
- Each SST provides the relative index of a given entry within it

With this metadata, walking the LSM tree at the metadata level can compute the number of records within a given key/sequence range without scanning all entries.

```rust
impl OpenLog {
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> Result<u64, Error>;
}
```

This mirrors the scan API but returns a count rather than entries.

## Alternatives

### KeyMapper abstraction

An earlier design introduced a `KeyMapper` trait to map user keys to fixed-width `u64` log_ids:

```rust
trait KeyMapper {
    fn map(&self, key: &Bytes) -> u64;
}
```

Two built-in implementations were considered:

- **HashKeyMapper** — Hash the key to produce the log_id. Stateless, but collisions map multiple keys to the same log.
- **DictionaryKeyMapper** — Store key-to-log_id mappings in the LSM. Collision-free, but requires coordination for ID assignment.

This approach was rejected because:

1. **Scan ambiguity** — Multiple keys mapping to the same log_id complicates scans. Either store the original key in the value and filter, or accept that colliding keys share a log stream.
2. **Key-range scans** — Hashing destroys key ordering, making key-range scans impossible.
3. **Added complexity** — The mapping layer adds indirection without clear benefit over using keys directly.

The simpler key+sequence encoding preserves key ordering and avoids the collision problem entirely.

## Updates

| Date | Description |
|------|-------------|
| YYYY-MM-DD | Initial draft |
