# RFC 0001: Log Storage

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the storage model and core API for OpenData-Log. Keys are stored directly in the LSM with a sequence number suffix, enabling per-key log streams with global ordering. The API provides append and scan operations mirroring SlateDB's interface.

## Motivation

OpenData-Log is a key-oriented log system inspired by Kafka, but with a simpler model: keys are user-defined and every key is logically its own independent log. There is no concept of partitions or repartitioning—users simply write to new keys when their access patterns change.

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

SlateDB keys are a composite of the user key and a `u64` sequence number. A version prefix and record type discriminator provide forward compatibility.

```
Log Entry:
  SlateDB Key:   | version (u8) | type (u8) | key (bytes) | sequence (u64) |
  SlateDB Value: | record value (bytes) |
```

The initial version is `1`. The type discriminator `0x01` is reserved for log entries. Additional record types (e.g., metadata, indexes) may be introduced in future RFCs using different discriminators.

This encoding preserves lexicographic key ordering, enabling key-range scans. Entries for the same key are ordered by sequence number.

#### Variable-Length Key Considerations

With variable-length keys, the boundary between key and sequence number is ambiguous during comparison. Two approaches to handle this:

**Custom comparator** — SlateDB could support a custom comparator that treats the key and sequence number as separate components. This would enable correct ordering without encoding changes.

**Scan filtering** — Without a custom comparator, scans may return entries from adjacent keys when key bytes happen to interleave with sequence number bytes. Results can be filtered by exact key match.

In practice, users are likely to use fixed-length keys, which avoids this issue entirely.

### Sequence Numbers

Sequence numbers are assigned from a single counter that is maintained by the SlateDB writer and is incremented after every append. Each key's log is monotonically ordered by sequence number, but the sequence numbers are not contiguous—other keys' appends are interleaved in the global sequence.

This approach simplifies ingestion by avoiding per-key sequence tracking. The trade-off is that sequence numbers do not reflect the count of entries within a key's log.

If SlateDB supports multi-writer in the future, each writer would maintain its own sequence counter. This design assumes each key would still have a single writer—interleaving appends from multiple writers to the same key would break monotonic ordering within that key's log.

### SST Representation

OpenData-Log proposes two enhancements to SlateDB's SST structure to support efficient `scan` and `count` operations.

#### Block Record Counts

Each block entry in the SST index would include a cumulative record count:

```
Block Entry: | block_offset | cumulative_record_count | first_key |
```

This enables counting records in a range by scanning the LSM at the index level rather than reading all entries. Block boundaries may not align with the query range, so the first and last blocks in each overlapping level may need to be read for exact counts. An approximate count can be computed from the index alone.

#### Bloom Filter Granularity

SlateDB SSTs include bloom filters to accelerate point lookups. For OpenData-Log, the bloom filter should be keyed on the log key alone, not the composite SlateDB key which includes the sequence number. This allows the bloom filter to indicate whether a given log is present in an SST, reducing the blocks read during `scan` or `count` queries.

### Append-Only Scan Optimization

In a typical key-value store, range scans must concurrently merge all LSM levels because any level may contain the most recent value for a given key. The append-only structure of open-log provides a stronger guarantee: newer entries are always in higher levels (L0 and recent sorted runs), while older entries settle into deeper levels through compaction.

This ordering guarantee enables level-by-level iteration rather than concurrent merging. For queries targeting the tip of a log, we can avoid loading blocks from older levels entirely. This improves performance and prevents cache thrashing from loading historical data that isn't needed.

How this optimization can be exposed in SlateDB remains to be explored.

### Write API

The write API mirrors SlateDB's `write` API. The only supported operation is `append`.

```rust
struct Record {
    key: Bytes,
    value: Bytes,
}

#[derive(Default)]
struct WriteOptions {
    await_durable: bool,
}

impl Log {
    async fn append(&self, records: Vec<Record>) -> Result<(), Error>;
    async fn append_with_options(&self, records: Vec<Record>, options: WriteOptions) -> Result<(), Error>;
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
#[derive(Default)]
struct ScanOptions {
}

struct LogIterator { ... }

impl LogIterator {
    async fn next(&mut self) -> Result<Option<LogEntry>, Error>;
}

impl Log {
    fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> LogIterator;
    fn scan_with_options(&self, key: Bytes, seq_range: impl RangeBounds<u64>, options: ScanOptions) -> LogIterator;
}
```

### Count API (under consideration)

Lag is a critical metric for tracking progress reading from a log. Without contiguous sequence numbers, computing lag requires the SST enhancements described in [SST Representation](#sst-representation). This proposal adds an explicit API to count the number of records that are present within any range of the log for a key. 

```rust
// TODO: decide which SlateDB ScanOptions parameters to pass through
#[derive(Default)]
struct CountOptions {
    approximate: bool,  // default: false (precise counts)
}

impl Log {
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> Result<u64, Error>;
    async fn count_with_options(&self, key: Bytes, seq_range: impl RangeBounds<u64>, options: CountOptions) -> Result<u64, Error>;
}
```

This mirrors the scan API but returns a count rather than entries. The `approximate` option allows counting from the index alone without reading boundary blocks.

For example, to compute the current lag for a key from a given sequence number:

```rust
let current_seq: u64 = 1000;
let lag = log.count(key, current_seq..).await?;
```

### LogRead Trait and LogReader

Following SlateDB's pattern with `DbRead` and `DbReader`, we define a `LogRead` trait that abstracts read operations. Both `Log` and `LogReader` implement this trait.

```rust
trait LogRead {
    fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> LogIterator;
    fn scan_with_options(&self, key: Bytes, seq_range: impl RangeBounds<u64>, options: ScanOptions) -> LogIterator;
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> Result<u64, Error>;
    async fn count_with_options(&self, key: Bytes, seq_range: impl RangeBounds<u64>, options: CountOptions) -> Result<u64, Error>;
}

impl LogRead for Log { ... }
impl LogRead for LogReader { ... }
```

`LogReader` is a read-only view of the log, useful for consumers that should not have write access:

```rust
impl Log {
    fn reader(&self) -> LogReader;
}
```

This separation allows:

- **Access control** — Share `LogReader` with components that only need read access.
- **Generic programming** — Write functions that accept `impl LogRead` to work with either `Log` or `LogReader`.

## Alternatives

### KeyMapper Abstraction

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

### Headers

Messaging systems often expose a way to attach headers to messages in order to enable middleware use cases, such as routing. We opted not to include headers in order to keep our data model as simple as possible. Although the log abstraction could be used to build messaging system or any other system which relied on headers, we do not believe that headers are fundamental to the log data structure. There are many potential use cases which do not need headers. Instead, our position is that headers should be designed into systems built on top of the log as necessary. That allows those systems to define the header semantics that are appropriate for their system rather than trying to define a common semantics in the log.
 

## Updates

| Date       | Description |
|------------|-------------|
| 2025-12-15 | Initial draft |
