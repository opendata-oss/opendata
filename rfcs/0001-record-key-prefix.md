# RFC 0001: Record Key Prefix

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC defines a common 2-byte record key prefix for OpenData storage systems built on SlateDB.
All records begin with a subsystem byte and a version byte; everything after that is subsystem-defined.
The shared prefix gives each subsystem an isolated keyspace and a path for schema evolution without dictating any further key layout.

## Motivation

OpenData comprises multiple storage subsystems (e.g. log, timeseries, vector, keyvalue) that share a common foundation on SlateDB.
Each subsystem defines its own record types with distinct key structures, but they all benefit from a couple of shared invariants:

1. **Subsystem isolation** — a shared keyspace needs a way to namespace records by subsystem.
2. **Forward compatibility** — schema evolution needs an explicit version signal.

Formalizing those two bytes as a common prefix gives every subsystem the same foundation without coupling them to a specific key layout.
Earlier drafts of this RFC also reserved a third "record tag" byte at byte position 2.
That mandate has been removed (see [Alternatives](#common-record-tag-at-byte-2)) — record-type discrimination is now left entirely to each subsystem to design.

## Goals

- Define a minimal common prefix shared by every subsystem
- Document the versioning strategy for schema evolution

## Non-Goals

- Subsystem-specific record definitions (covered by individual RFCs)
- Value encoding conventions (covered by individual RFCs)
- Record-type discrimination — left to each subsystem

## Design

### Record Key Structure

All OpenData records stored in SlateDB use keys with the following prefix:

```
┌───────────┬─────────┬─────────────────────────────────┐
│ subsystem │ version │  ... subsystem-defined fields   │
│  1 byte   │ 1 byte  │            (varies)             │
└───────────┴─────────┴─────────────────────────────────┘
```

The 2-byte prefix is followed by subsystem-defined fields that vary by subsystem and record type.
Subsystems typically include a record-type discriminator somewhere after the prefix to distinguish record kinds within their keyspace; the exact position and semantics are at each subsystem's discretion.

### Subsystem Byte

The first byte identifies the subsystem that owns the key.

| Value         | Description        |
|---------------|--------------------|
| `0x00`        | Reserved (invalid) |
| `0x01`        | Timeseries         |
| `0x02`        | Vector             |
| `0x03`        | Log                |
| `0x04`        | KeyValue           |
| `0x05`–`0xFF` | Available          |

### Version Byte

The second byte identifies the key format version.
Each subsystem manages its version independently — bumping the version in one subsystem does not affect others.

| Value         | Description       |
|---------------|-------------------|
| `0x00`        | Reserved (invalid)|
| `0x01`–`0xFF` | Subsystem-defined |

The version byte enables schema migration.
When the key format changes in a backwards-incompatible way, the subsystem increments its version.
Readers encountering an unknown version can reject the record or apply version-specific parsing logic.

**Guidelines for version changes:**
- Additive changes to value schemas do not require a version bump
- New record types do not require a version bump
- Changes to existing key field layouts require a version bump

## Alternatives

### Common record tag at byte 2

Earlier drafts of this RFC mandated a third "record tag" byte at position 2, available `0x01`–`0xFF`.
The intent was to enable cross-subsystem introspection tooling that could classify records by type without parsing subsystem-specific fields.

Two things led us away from it:

1. **No common tooling materialized.**
   No introspection tools came to rely on the position, so the constraint paid no rent.

2. **Subsystem needs evolved past it.**
   The log subsystem's prefix-routed segmentation (see [`log/rfcs/0002`](../log/rfcs/0002-logical-segmentation.md)) requires a routing identifier (segment id) immediately after the version byte, ahead of any record-type discriminator.
   A common byte-2 record tag is incompatible with that layout.

Subsystems that want a record-type discriminator are free to keep one — they just choose where it sits.
Timeseries, vector, and keyvalue still place a tag at byte 2; the log subsystem (v2 onwards) places its tag after a 4-byte segment id.

### Shared prefix version

An alternative would use a shared version across all subsystems, requiring coordinated bumps when the prefix format changes:

```
| subsystem | prefix_version | ... |
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

2. **Unlikely need** — The 2-byte prefix is simple and stable. Changes requiring cross-subsystem coordination are unlikely.

3. **Simplicity** — Independent versioning is easier to reason about and implement.

## Open Questions

None at this time.

## Updates

| Date       | Description                                |
|------------|--------------------------------------------|
| 2026-01-09 | Initial draft                              |
| 2026-03-18 | Simplify record tag and add subsystem byte |
| 2026-05-19 | Drop the record_tag mandate from the common prefix. The common prefix is now `[subsystem, version]` (2 bytes); record-type discrimination is subsystem-defined. |
| 2026-05-20 | Slim `common::serde::key_prefix::KeyPrefix` to the new 2-byte shape and move tag-byte handling into the timeseries/vector subsystems. `common::serde::record_tag::RecordTag` remains as an opt-in 4+4 bit tag-byte utility for subsystems that want it. |
