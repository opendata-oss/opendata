# RFC 0001: Record Key Prefix

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC defines a common record key prefix format for OpenData storage systems built on SlateDB.
All records use a standardized 3-byte prefix consisting of a subsystem byte, a version byte, and a record tag byte.
The record tag stores the record type as a full byte.
This enables forward compatibility and consistent key organization across Opendata systems.

## Motivation

OpenData comprises multiple storage subsystems (e.g. log, timeseries) that share a common foundation on SlateDB. Each subsystem defines its own record types with distinct key structures, but they follow a similar pattern: a version prefix for forward compatibility and a type discriminator to distinguish record kinds.

Formalizing this pattern as a common specification provides several benefits:

1. **Consistency** — New subsystems and record types follow a predictable structure, making the codebase easier to understand and maintain.

2. **Forward compatibility** — The version byte allows schema evolution without breaking existing data.

3. **Documentation** — A single source of truth for the encoding conventions used across OpenData.

## Goals

- Define the 3-byte record key prefix format
- Specify the record tag encoding
- Document the versioning strategy for schema evolution
- Establish conventions for big-endian key encoding

## Non-Goals

- Subsystem-specific record definitions (covered by individual RFCs)
- Value encoding conventions (covered by individual RFCs)
- Key components beyond the 3-byte prefix

## Design

### Record Key Structure

All OpenData records stored in SlateDB use keys with the following prefix:

```
┌───────────┬─────────┬────────────┬─────────────────────┐
│ subsystem │ version │ record_tag │  ... record fields  │
│  1 byte   │ 1 byte  │   1 byte   │    (varies)         │
└───────────┴─────────┴────────────┴─────────────────────┘
```

The 3-byte prefix is followed by record-specific fields that vary by subsystem and record type.

### Subsystem Byte

The first byte identifies the subsystem that owns the key.

| Value         | Description        |
|---------------|--------------------|
| `0x00`        | Reserved (invalid) |
| `0x01`        | Timeseries         |
| `0x02`        | Vector             |
| `0x03`        | Log                |
| `0x04`        | KeyValue           |
| `0x01`–`0xFF` | Subsystem-defined  |


### Version Byte

The second byte identifies the key format version.
Each subsystem manages its version independently—bumping the version in one subsystem does not affect others.

| Value  | Description |
|--------|-------------|
| `0x00` | Reserved (invalid) |
| `0x01` | Current version (all subsystems) |
| `0x02`–`0xFF` | Reserved for future versions |

The version byte enables schema migration.
When the key format changes in a backwards-incompatible way, the subsystem increments its version.
Readers encountering an unknown version can reject the record or apply version-specific parsing logic.

**Guidelines for version changes:**
- Additive changes to value schemas do not require a version bump
- New record types do not require a version bump
- Changes to existing key field layouts require a version bump

### Record Tag Byte

The third byte stores the record tag as a full byte value.

**Record Tag:** Identifies the kind of record. Values `0x01`–`0xFF` are available.
Tag `0x00` is reserved in all subsystems.

### Record Type Allocation

Record types are allocated per-subsystem.
Since subsystems are stored in separate SlateDB instances, the same type value may be reused across subsystems without collision.
Type `0x0` is reserved in all subsystems.

See the following RFCs for subsystem-specific record type definitions:

- **Log:** [RFC 0001: Log Storage](../log/rfcs/0001-storage.md)
- **Timeseries:** [RFC 0001: TSDB Storage](../timeseries/rfcs/0001-tsdb-storage.md)

### Prefix Queries

The standardized prefix enables efficient filtering by record type:

```rust
// Build a 3-byte prefix
fn key_prefix(subsystem: u8, version: u8, record_tag: u8) -> [u8; 3] {
    [subsystem, version, record_tag]
}
```

### Record Type Ordering

Placing the record type in the key prefix means that records of different types are stored in separate key ranges and cannot be interleaved. This is a deliberate design choice with tradeoffs:

**Benefits:**
- Simpler access model — queries for a single record type are contiguous range scans
- Predictable key ordering — easier to reason about compaction and caching behavior
- Clean separation — different record types can evolve independently

**Limitations:**
- Rules out key designs that optimize for locality across record types. For example, a system that wants to co-locate metadata and data records for the same entity (e.g., `entity_id:metadata` adjacent to `entity_id:data`) cannot do so with this prefix structure.

This tradeoff favors simplicity and type-scoped access patterns over flexible cross-type locality. Subsystems requiring tight co-location of heterogeneous records would need to encode them as a single record type with internal discrimination.

## Alternatives

### Split Record Tag Byte

An earlier design split the second byte into a high-nibble record type and a low-nibble reserved field:

```
| version | record_tag | ... |
```

That approach reduced the record-type space to 15 values and baked subsystem-specific concerns into a shared encoding.
Using the full byte for the record tag keeps the common prefix simpler and gives each subsystem the full `0x01`-`0xFF` tag range.
Each subsystem can encode additional information in the record type.
For example, timeseries encodes time bucket granularity in the lower 4 bits of the record tag.

### Shared Prefix Version

An alternative would use a shared version across all subsystems, requiring coordinated bumps when the prefix format changes:

```
| subsystem | prefix_version | record_tag | ... |
```

A hybrid approach could reserve one bit (e.g., the high bit) to indicate prefix-level changes, leaving 7 bits for subsystem-specific versioning:

```
version byte layout:
┌─────────┬───────────────────┐
│  bit 7  │     bits 6-0      │
│ prefix  │ subsystem version │
└─────────┴───────────────────┘
```

These approaches were deferred because:

1. **Coordination overhead** — Requiring all subsystems to bump versions together is inconvenient and slows development.

2. **Unlikely need** — The 3-byte prefix structure is simple and stable. Changes requiring cross-subsystem coordination are unlikely.

3. **Simplicity** — Independent versioning is easier to reason about and implement.

## Open Questions

None at this time.

## Updates

| Date       | Description                                |
|------------|--------------------------------------------|
| 2026-01-09 | Initial draft                              |
| 2026-03-18 | Simplify record tag and add subsystem byte |
