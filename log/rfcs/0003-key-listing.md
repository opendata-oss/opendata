# RFC 0003: Key Listing

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC introduces a key listing API for OpenData-Log. Currently, there is no way to discover which keys are present in the log without scanning the entire keyspace. The new `list` API returns an iterator over distinct keys within a sequence number range, backed by per-segment listing records that are written during ingestion.

## Motivation

The log API provides `scan` for reading entries from a specific key and `count` for counting entries. However, there is no efficient way to answer the question: "What keys exist in the log?"

Without a listing API, users must scan the entire log to discover keys—an expensive operation that scales with the total data size rather than the number of distinct keys. This gap limits use cases such as:

- **Discovery**: Applications that need to enumerate available keys for user selection or auto-completion
- **Monitoring**: Dashboards that display active keys or key counts
- **Administration**: Tools that need to inspect or audit the keyspace

The key listing API addresses this by maintaining lightweight listing records that track key presence per segment. Tying listings to segments also fits naturally with segment-based retention—when segments are deleted, their listing records are removed as well, and keys that are no longer present in any remaining segment naturally fall out of scope.

## Goals

- Define a `list` API for enumerating distinct keys within a sequence number range
- Define the `ListingEntry` record type for tracking key presence per segment
- Specify when and how listing records are written during ingestion

## Non-Goals

- Key prefix filtering (left for future enhancement)
- Returning metadata alongside keys (e.g., first sequence number, entry count)
- Listing records for deleted or expired keys

## Design

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

This approach avoids read-before-write overhead in the ingest path. When writers change (e.g., after failover), the new writer starts with an empty cache and may re-insert listing records for keys already present in the segment. However, since SlateDB overwrites duplicate keys, no additional storage overhead results from this.

### List API

The `list` API returns an iterator over distinct keys within a sequence number range:

```rust
struct LogKeyIterator { ... }

impl LogKeyIterator {
    async fn next(&mut self) -> Result<Option<Bytes>, Error>;
}

#[derive(Default)]
struct ListOptions {
    // Reserved for future options (e.g., prefix filtering)
}
```

The API is added to the `LogRead` trait:

```rust
trait LogRead {
    // ... existing methods ...

    fn list(&self, seq_range: impl RangeBounds<u64>) -> LogKeyIterator;
    fn list_with_options(
        &self,
        seq_range: impl RangeBounds<u64>,
        options: ListOptions,
    ) -> LogKeyIterator;
}
```

The sequence number range is mapped internally to the corresponding segment range. The iterator scans `ListingEntry` records across those segments and returns distinct keys.

### Deduplication

The iterator deduplicates keys before returning them to the caller. Within a single segment, SlateDB's key-value semantics ensure that only one `ListingEntry` record exists per key—duplicate writes (e.g., from writer failover) simply overwrite the existing record. However, the same key may appear in multiple segments within the query range. The iterator must track seen keys across segments to ensure each key is returned exactly once.

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

## Open Questions

### Deduplication Strategy

The deduplication mechanism has two main approaches:

1. **In-memory collection**: Accumulate all keys in memory, deduplicate, then return. Simple but memory usage scales with distinct key count. Enables sorted iteration order.

2. **Streaming with tracking**: Return keys as encountered, tracking seen keys to skip duplicates. Lower memory latency for first results, but still requires tracking state. Order depends on segment/key ordering in storage.

The choice affects memory usage, latency characteristics, and whether keys can be returned in sorted order. Input welcome on which approach better serves expected use cases.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-13 | Initial draft |
