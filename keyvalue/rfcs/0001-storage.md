# RFC 0001: KeyValue Storage

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines KeyValue, a thin wrapper over SlateDB that provides general-purpose key-value storage with OpenData's common key conventions. KeyValue applies the standard 2-byte record prefix to user keys, ensuring compatibility with OpenData's key encoding patterns.

## Motivation

There are two primary drivers for KeyValue.

### Auxiliary Systems

Core OpenData systems like Log, Timeseries, and Vector each have supporting needs that require simple key-value storage. Examples include sequence tracking for log readers and registry or catalog metadata. We could implement these directly on SlateDB as we do for the primary data stores, but this means reimplementing the standard key encoding for each new system. KeyValue provides a shared foundation with the standardized key structure already in place.

### Internal Composition

Issue [#140](https://github.com/opendata-oss/opendata/issues/140) describes the broader vision for composition across OpenData systems. KeyValue encourages a mindset of building new systems as compositions of existing ones. For example, Log + KeyValue could store arbitrary materialized views derived from log data. Whatever key space partitioning solution emerges from issue #140 will apply to KeyValue as well.

### Naming

Issue [#111](https://github.com/opendata-oss/opendata/issues/111) discusses table-like abstractions ranging from raw key-value primitives to SQL-compatible tables. We use "KeyValue" for this system to reserve "Table" for a future SQL-oriented abstraction. KeyValue corresponds roughly to Option A in that issue—a thin wrapper over SlateDB's primitives.

## Goals

- Define the key encoding for KeyValue records
- Provide a thin wrapper that mirrors SlateDB's interface

## Non-Goals

- Namespace or key space partitioning (deferred to issue #140)
- Secondary indexes or query capabilities
- TTL or automatic expiration
- Compression or encoding of values

## Design

### Key Encoding

KeyValue keys follow OpenData's common 2-byte prefix format:

```
KeyValue Record:
  SlateDB Key:   | version (u8) | record_tag (u8) | user_key (bytes) |
  SlateDB Value: | user_value (bytes) |
```

**Version byte:** `0x01` (current version).

**Record tag:** `0x10` (record type `0x1` in high 4 bits, low 4 bits reserved as `0x0`).

**User key:** The application-provided key bytes, appended directly after the record tag. User keys are stored as raw bytes without escaping or termination.

#### Example

For user key `my-key`:

```
SlateDB Key: | 0x01 | 0x10 | m | y | - | k | e | y |
```

### Values

Values are passed through to SlateDB without modification. KeyValue does not impose any encoding or structure on values.

## Alternatives

### Direct SlateDB Usage

Users could use SlateDB directly rather than going through KeyValue. SlateDB is a full-featured LSM key-value store with extensive configuration options, merge operators, compaction tuning, and more.

KeyValue intentionally does not replicate this complexity. It provides a curated set of APIs and configurations that fit within the OpenData platform. The goal is to start minimal and add capabilities judiciously based on actual usage rather than exposing SlateDB's full surface area from the start.

This approach trades flexibility for simplicity. Users needing SlateDB's advanced features can use it directly, while KeyValue serves the common case of auxiliary storage within the OpenData ecosystem.

As an example, consider SlateDB's merge operator. Merge operators enable efficient read-modify-write patterns by deferring computation to compaction time, but they add considerable API and runtime complexity and their usage is relatively niche. Rather than exposing merge operators in KeyValue, we would tend to either wait for sufficient use cases to justify the complexity, or define a separate system tailored to merge-heavy workloads. This would keep KeyValue simple for users who don't need advanced features.

## Open Questions

None at this time.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-02-02 | Initial draft |
