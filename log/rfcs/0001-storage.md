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
  SlateDB Key:   | version (u8) | type (u8) | key (TerminatedBytes) | sequence (u64) |
  SlateDB Value: | record value (bytes) |
```

The initial version is `1`. The type discriminator `0x01` is reserved for log entries. Additional record types (e.g., metadata, indexes) may be introduced in future RFCs using different discriminators.

This encoding preserves lexicographic key ordering, enabling key-range scans. Entries for the same key are ordered by sequence number.

#### TerminatedBytes

A `TerminatedBytes` is a variable-length byte sequence that terminates with a `0xFF` delimiter. This delimiter provides an unambiguous boundary between the user key and the sequence number, which is necessary for correct lexicographic ordering when keys have variable length.

To allow arbitrary byte sequences in user keys (including `0xFE` and `0xFF`), the key bytes are escaped before encoding:

| Raw Byte | Encoded As   |
|----------|--------------|
| `0xFE`   | `0xFE 0x00`  |
| `0xFF`   | `0xFE 0x01`  |
| other    | unchanged    |

The escape character `0xFE` is always followed by either `0x00` (representing a literal `0xFE`) or `0x01` (representing a literal `0xFF`). After escaping, the `0xFF` byte only appears as the terminating delimiter.

**Example:**

For a user key `hello` (no special bytes):
```
Encoded: | h | e | l | l | o | 0xFF |
```

For a user key containing `0xFE` and `0xFF` bytes (`a 0xFE b 0xFF c`):
```
Encoded: | a | 0xFE | 0x00 | b | 0xFE | 0x01 | c | 0xFF |
```

This encoding preserves lexicographic ordering: if key A < key B in their raw form, then their escaped forms maintain the same ordering. The delimiter ensures that no key can be a prefix of another key's encoding, preventing interleaving of entries from different logs.

#### Variable-Length Key Ordering

With variable-length keys, the boundary between key and sequence number would be ambiguous during lexicographic comparison if not explicitly delimited. Without a delimiter, entries from different keys could interleave incorrectly.

For example, consider keys `a` and `ab` with sequence numbers. Without delimiting:
```
Key "a"  + seq 0x0100: | a | 0x00 | ... | 0x01 | 0x00 |
Key "ab" + seq 0x0001: | a | b    | ... | 0x00 | 0x01 |
```

Depending on the sequence number bytes, entries from `a` and `ab` could interleave in unexpected ways.

The `TerminatedBytes` encoding solves this by inserting a `0xFF` delimiter after the escaped key bytes. Since `0xFF` is the highest byte value and only appears as the delimiter (never within the escaped key), all entries for a given key are guaranteed to be contiguous and ordered by sequence number.

With `TerminatedBytes`:
```
Key "a"  + seq: | a | 0xFF | <sequence bytes> |
Key "ab" + seq: | a | b | 0xFF | <sequence bytes> |
```

All entries for key `a` sort before all entries for key `ab`, and within each key, entries are ordered by sequence number.

### Sequence Numbers

Sequence numbers are assigned from a single counter that is maintained by the SlateDB writer and is incremented after every append. Each key's log is monotonically ordered by sequence number, but the sequence numbers are not contiguous—other keys' appends are interleaved in the global sequence. Additionally, sequence numbers may have gaps due to crash recovery (see below). The only guarantee is monotonicity: within a key's log, sequence numbers are strictly increasing.

This approach simplifies ingestion by avoiding per-key sequence tracking. The trade-off is that sequence numbers do not reflect the count of entries within a key's log.

If SlateDB supports multi-writer in the future, each writer would maintain its own sequence counter. This design assumes each key would still have a single writer—interleaving appends from multiple writers to the same key would break monotonic ordering within that key's log.

#### Block-Based Sequence Allocation

To efficiently track and recover the sequence counter, the writer uses block-based allocation. Rather than persisting the sequence number after every append, the writer pre-allocates a block of sequence numbers and records the allocation in the LSM.

A new record type `LastBlock` (type discriminator `0x02`) stores the current allocation:

```
LastBlock Record:
  SlateDB Key:   | version (u8) | type (u8) |
  SlateDB Value: | base_sequence (u64) | block_size (u64) |
```

The `LastBlock` key is static—it contains only the version and type discriminator with no user key component. This ensures there is exactly one such record in the database.

**Allocation procedure:**

1. On initialization, the writer reads the `LastBlock` record (if present) to determine the last allocated range `[base, base + size)`.
2. The writer allocates a new block starting at `base + size` and writes a new `LastBlock` record before processing any appends.
3. During normal operation, the writer assigns sequence numbers from the current block, incrementing after each append.
4. When the current block is exhausted, the writer allocates a new block and writes an updated `LastBlock` record.

**Recovery:**

On crash recovery, the writer reads the `LastBlock` record and allocates a fresh block starting after the previous range. Any sequence numbers that were allocated but not used before the crash are simply skipped. This may create gaps in the sequence space, but monotonicity is preserved.

**Block sizing:**

The block size is an internal implementation detail and is not exposed through configuration. The implementation may vary the block size to balance write amplification (larger blocks reduce `LastBlock` write frequency) against sequence space efficiency (smaller blocks waste fewer sequence numbers on crash).

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

In a typical key-value store, range scans must concurrently merge all LSM levels because any level may contain the most recent value for a given key. The append-only structure of OpenData-Log provides a stronger guarantee: newer entries are always in higher levels (L0 and recent sorted runs), while older entries settle into deeper levels through compaction.

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

### SlateDB Sequence Numbers

SlateDB maintains its own internal sequence number for MVCC versioning. Ideally, OpenData-Log could reuse this counter rather than implementing separate tracking with `LastBlock` records. However, SlateDB's sequence number is not currently exposed in its public API. Furthermore, since the sequence number is embedded into the key, we would need to align the sequence number prior to writing. Current proposals to expose SlateDb sequence numbers do not offer such a mechanism, but we can reevaluate once the effort is complete. Finally, we should reserve the ability to offer an `edit` API so that a corrupt/incorrect record may be overwritten. This may be more difficult if we are too coupled with the SlateDb sequence number.  

### Headers

Messaging systems often expose a way to attach headers to messages in order to enable middleware use cases, such as routing. We opted not to include headers in order to keep our data model as simple as possible. Although the log abstraction could be used to build messaging system or any other system which relied on headers, we do not believe that headers are fundamental to the log data structure. There are many potential use cases which do not need headers. Instead, our position is that headers should be designed into systems built on top of the log as necessary. That allows those systems to define the header semantics that are appropriate for their system rather than trying to define a common semantics in the log.
 

## Updates

| Date       | Description |
|------------|-------------|
| 2025-12-15 | Initial draft |
| 2026-01-05 | Added block-based sequence allocation |
| 2026-01-06 | Added TerminatedBytes encoding for variable-length keys |
