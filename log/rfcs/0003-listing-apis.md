# RFC 0003: Listing APIs

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC introduces listing APIs for OpenData-Log, enabling discovery of segments and keys without scanning the full log. The `list_segments` API exposes segments as a first-class concept, returning segment metadata including boundaries and creation time. The `list_keys` API returns distinct keys within a segment range, backed by per-segment listing records written during ingestion.

## Motivation

The log API provides `scan` for reading entries from a specific key and `count` for counting entries. However, there is no efficient way to answer questions like: "What segments exist?" or "What keys are present?"

Without listing APIs, users must scan the entire log to discover this information—an expensive operation that scales with total data size. This gap limits use cases such as:

- **Discovery**: Applications that need to enumerate available keys for user selection or auto-completion
- **Monitoring**: Dashboards that display active segments, keys, or key counts
- **Administration**: Tools that need to inspect segment boundaries or audit the keyspace

The listing APIs address this by exposing segment metadata directly and maintaining lightweight listing records that track key presence per segment. Tying key listings to segments fits naturally with segment-based retention—when segments are deleted, their listing records are removed as well, and keys that are no longer present in any remaining segment naturally fall out of scope.

## Goals

- Define a `list_segments` API for enumerating segments and their boundaries
- Define a `list_keys` API for enumerating distinct keys within a segment range
- Define the `ListingEntry` record type for tracking key presence per segment
- Specify when and how listing records are written during ingestion

## Non-Goals

- Key prefix filtering (left for future enhancement)
- Returning metadata alongside keys (e.g., first sequence number, entry count)
- Listing records for deleted or expired keys

## Design

### Type Aliases

The API introduces two type aliases to make range parameters self-documenting:

```rust
/// Global sequence number for log entries.
pub type Sequence = u64;

/// Unique identifier for a segment.
pub type SegmentId = u32;
```

These types clarify the intent of range parameters throughout the API. For example, `list_segments` accepts a `RangeBounds<Sequence>` while `list_keys` accepts a `RangeBounds<SegmentId>`.

### ListingEntry Record

A new record type `ListingEntry` (type discriminator `0x04`) tracks key presence within a segment:

```
ListingEntry Record:
  Key:   | version (u8) | type (u8=0x04) | segment_id (u32 BE) | key (Bytes) |
  Value: | (empty) |
```

The key structure places `segment_id` before the user key, ensuring that all listing records for a segment are contiguous. This enables efficient enumeration of keys within a segment via prefix scan.

Unlike log entry keys, the user key is stored as raw `Bytes` without `TerminatedBytes` encoding. Since the key occupies the suffix position, no delimiter is needed to establish boundaries.

The value is empty—presence of the record indicates the key exists in that segment.

### Write Path

Listing records are written lazily during ingestion. When the writer encounters a key for the first time within a segment, it writes a `ListingEntry` record and caches the key. Subsequent appends to the same key within the same segment do not write additional listing records.

```
On append(key, value):
  1. If key not in segment_key_cache:
     a. Write ListingEntry record for (current_segment_id, key)
     b. Add key to segment_key_cache
  2. Write log entry as usual
```

When a new segment starts, the cache is cleared.

This approach avoids read-before-write overhead in the ingest path. When writers change (e.g., after failover), the new writer starts with an empty cache and may re-insert listing records for keys already present in the segment. SlateDB will overwrite duplicate keys, though the duplicates exist until compaction removes them. Since writers should not change frequently, we do not expect this to be a significant concern. If it does become problematic, we can introduce read-before-write in the future.

### Segment API

Segments become a first-class concept in the public API. Segments provide a coarse-grained boundary for attaching metadata—key listings being one example. Tracking metadata at the sequence level would require state proportional to the number of entries, effectively reducing to range scanning over the full log. Segments give us a natural grouping where we can maintain lightweight indexes without exploding storage costs.

Exposing segments publicly allows users to understand these boundaries and query with precision. It also opens the door for additional segment-level metadata in the future, potentially including application-defined attributes.

```rust
/// A segment of the log.
pub struct Segment {
    /// Unique segment identifier (monotonically increasing).
    pub id: SegmentId,
    /// First sequence number in this segment.
    pub start_seq: Sequence,
    /// Wall-clock time when this segment was created (ms since epoch).
    pub start_time_ms: i64,
}
```

The segment listing API is added to the `LogRead` trait:

```rust
trait LogRead {
    // ... existing methods ...

    fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence>,
    ) -> Result<Vec<Segment>>;
}
```

The `list_segments` method returns all segments that overlap the given sequence range. This is a precise operation—segments have well-defined boundaries, so there is no approximation. Pass `..` to list all segments.

> **Note**: A `list_segments_with_options` variant can be added when options are needed.

### List Keys API

The `list_keys` API returns an iterator over distinct keys within a segment range:

```rust
struct LogKey {
    key: Bytes,
}

struct LogKeyIterator { ... }

impl LogKeyIterator {
    async fn next(&mut self) -> Result<Option<LogKey>, Error>;
}
```

The API is added to the `LogRead` trait:

```rust
trait LogRead {
    // ... existing methods ...

    fn list_keys(
        &self,
        segment_range: impl RangeBounds<SegmentId>,
    ) -> LogKeyIterator;
}
```

The segment range specifies which segments to scan for keys. Using `SegmentId` rather than `Sequence` ensures precise results—the iterator returns exactly the keys present in the specified segments, with no approximation. Pass `..` to list keys from all segments.

Users who want to query by sequence range can use `list_segments` to find the relevant segments, then pass those segment IDs to `list_keys`. This two-step approach makes the segment-granular nature of key tracking explicit.

> **Note**: A `list_keys_with_options` variant can be added when options are needed (e.g., prefix filtering).

### Deduplication

The iterator deduplicates keys before returning them to the caller. Within a single segment, SlateDB's key-value semantics ensure that only one `ListingEntry` record exists per key—duplicate writes (e.g., from writer failover) simply overwrite the existing record. However, the same key may appear in multiple segments within the query range.

The initial implementation collects all keys from the listing scan, deduplicates them in memory, and then returns the deduplicated collection to the caller. This approach is simple and enables sorted iteration order, though memory usage scales with the distinct key count. Future iterations may explore streaming approaches if memory becomes a concern for large keyspaces.

## Alternatives

### Scan-Based Listing

An alternative is to derive key listings from log entries directly by scanning and extracting unique keys. This was rejected because:

- Requires reading all log entries, not just lightweight listing records
- Cost scales with total data size rather than distinct key count
- Cannot leverage segment-based pruning

### Eager Listing Records

Writing a listing record on every append (not just first occurrence) would simplify the write path by removing the cache. This was rejected because:

- Dramatically increases write amplification for high-volume keys
- Most use cases only need to know key presence, not append frequency

### Global Key Index

Maintaining a single global index of all keys (outside the segment structure) would simplify queries but complicates retention. When segments are deleted, identifying which keys are no longer present requires scanning the remaining segments. Per-segment listings naturally handle this—deleting a segment removes its listing records, and keys fall out of scope when their last segment is removed.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-13 | Initial draft |
| 2026-01-20 | Broaden scope to listing APIs; expose segments as first-class concept; key listing uses segment ranges |

