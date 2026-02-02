# RFC 0004: Common Encodings

**Status**: Draft

**Authors**:
- [Bruno Cadonna](https://github.com/cadonna)

## Summary

This RFC defines common encoding primitives shared across OpenData subsystems (Log, Timeseries, Vector).
These encodings provide consistent serialization formats ensuring interoperability and reducing code duplication.

## Motivation

Storage systems built on ordered key-value stores like SlateDB require careful encoding of variable-length data to 
preserve lexicographic ordering in keys and efficiently pack data in values. 
These encoding primitives are fundamental building blocks that appear repeatedly across different storage schemas.

Defining these encodings as shared primitives provides several benefits:

1. **Correctness** — Encoding edge cases (escape sequences, endianness, boundary conditions) are handled once and tested thoroughly
2. **Consistency** — All subsystems encode data identically, enabling shared tooling for debugging, migration, and analysis
3. **Maintainability** — Bug fixes and optimizations benefit all consumers automatically
4. **Documentation** — A single specification serves as the authoritative reference for encoding behavior

## Goals

- Define common encodings that are used across all OpenData subsystems.

## Non-Goals

- Domain-specific encodings (e.g., Gorilla compression, RoaringBitmap) — these remain in their respective subsystems
- Record key prefix encoding — covered in RFC 0001

## Design

### Endianness Convention

All common encodings follow the OpenData endianness convention:

- **Key encodings** (used in SlateDB keys): Big-endian for lexicographic ordering
- **Value encodings** (used in SlateDB values): Little-endian for performance on common architectures (x86, ARM)

#### Key encodings
- `TerminatedBytes` 

#### Value encodings
- `Utf8`, 
- `OptionalNonEmptyUtf8`, 
- `Array<T>`,
- `FixedElementArray<T>`.

### TerminatedBytes

A variable-length byte sequence with escape sequences and a `0x00` terminator, designed for use
in SlateDB keys where lexicographic ordering must be preserved.

#### Encoding Format

```
┌─────────────────────────────────┬────────────┐
│         escaped_bytes           │ terminator │
│        (variable length)        │   0x00     │
└─────────────────────────────────┴────────────┘
```

To allow arbitrary byte sequences (including `0x00`, `0x01`, and `0xFF`), the input bytes are escaped before encoding:

| Raw Byte | Encoded As   |
|----------|--------------|
| `0x00`   | `0x01 0x01`  |
| `0x01`   | `0x01 0x02`  |
| `0xFF`   | `0x01 0x03`  |
| other    | unchanged    |

The escape character `0x01` is always followed by `0x01` (literal `0x00`), `0x02` (literal `0x01`), or `0x03` (literal `0xFF`). 
After escaping, `0x00` only appears as the terminating delimiter.

#### Properties

1. **Lexicographic ordering preserved** — If key A < key B in raw form, their encoded forms maintain the same ordering
2. **Unambiguous boundaries** — The `0x00` terminator ensures no encoded key can be a prefix of another
3. **Prefix-friendly** — Using `0x00` as terminator (lowest byte value) ensures shorter keys sort before longer keys with the same prefix (e.g., `/foo` < `/foo/bar`)

#### Examples

Simple key `hello` (no special bytes):
```
Input:   | h | e | l | l | o |
Encoded: | h | e | l | l | o | 0x00 |
```

Key containing special bytes (`a 0x00 b 0x01 c 0xFF d`):
```
Input:   | a | 0x00 | b | 0x01 | c | 0xFF | d |
Encoded: | a | 0x01 | 0x01 | b | 0x01 | 0x02 | c | 0x01 | 0x03 | d | 0x00 |
```

#### Prefix-Based Range Queries

To scan all keys with a given prefix, the range bounds are:

- **Start (inclusive)**: `prefix + 0x00` — the exact prefix key
- **End (exclusive)**: `prefix + 0xFF` — beyond all keys with this prefix

For example, to scan all keys starting with `/foo`:
```
Start: | / | f | o | o | 0x00 |  (includes exact match "/foo")
End:   | / | f | o | o | 0xFF |  (excludes all non-"/foo*" keys)
```

### Utf8

A length-prefixed UTF-8 string encoding for use in SlateDB values.

#### Encoding Format

```
┌─────────────────┬─────────────────────────┐
│   len (u16 LE)  │   utf8_bytes[0..len]    │
│    2 bytes      │      len bytes          │
└─────────────────┴─────────────────────────┘
```

- `len`: Length of the UTF-8 payload in bytes (little-endian u16)
- `utf8_bytes`: Raw UTF-8 encoded string bytes

**Total size**: 2 + len bytes

#### Properties

1. **Empty strings allowed** — `len = 0` is valid and represents an empty string
2. **Maximum length** — 65,535 bytes (u16 max)
3. **UTF-8 validity** — Payload must be valid UTF-8

#### Example

String `"hello"`:
```
Encoded: | 0x05 | 0x00 | h | e | l | l | o |
           └─ len ─┘    └─── payload ───┘
```

### OptionalNonEmptyUtf8

A variant of `Utf8` where a zero-length payload indicates the field was not provided (None), rather than an empty string.


#### Encoding Format

Same as `Utf8`:

```
┌─────────────────┬─────────────────────────┐
│   len (u16 LE)  │   utf8_bytes[0..len]    │
│    2 bytes      │      len bytes          │
└─────────────────┴─────────────────────────┘
```

#### Semantics

| len   | Meaning                          |
|-------|----------------------------------|
| 0     | Field not provided (None)        |
| > 0   | Non-empty UTF-8 string (Some)    |

Ingestion should never emit empty strings for these fields.
If a value is present, it must be non-empty.

### Array\<T\>

A count-prefixed collection of serialized elements for use in SlateDB values.

#### Encoding Format

```
┌──────────────────┬───────────┬───────────┬─────┬─────────────────┐
│  count (u16 LE)  │ element[0]│ element[1]│ ... │ element[count-1]│
│     2 bytes      │           │           │     │                 │
└──────────────────┴───────────┴───────────┴─────┴─────────────────┘
```

- `count`: Number of elements (little-endian u16)
- Elements are serialized back-to-back with no padding or delimiters

**Total size**: 2 + (count × element_size) bytes for fixed-size elements, or 2 + sum of element sizes for variable-size elements.

#### Properties

1. **Maximum count** — 65,535 elements (u16 max)
2. **Empty arrays allowed** — `count = 0` is valid
3. **No padding** — Elements are packed contiguously
4. **Element encoding** — Each element uses its own encoding rules (may be nested)

#### Example

Array of two `Utf8` strings `["ab", "c"]`:
```
Encoded: | 0x02 | 0x00 | 0x02 | 0x00 | a | b | 0x01 | 0x00 | c |
           └─ count ─┘  └── "ab" ──────────┘  └─── "c" ────┘
```

### FixedElementArray\<T\>

A collection of fixed-size elements with no count prefix, where the element count is derived from the buffer length.

#### Encoding Format

```
┌───────────┬───────────┬─────┬─────────────────┐
│ element[0]│ element[1]│ ... │ element[count-1]│
└───────────┴───────────┴─────┴─────────────────┘
```

- No count prefix
- Elements are serialized back-to-back with no padding

**Total size**: count × element_size bytes (count is not stored)

#### Properties

1. **Fixed-size elements only** — All elements must have identical, known sizes
2. **Count derivation** — `count = buffer_length / element_size`
3. **Schema restriction** — Only use when the schema contains a single array and nothing else (so the buffer length is unambiguous)
4. **Space efficient** — Saves 2 bytes compared to `Array<T>` by omitting the count

#### Example

Array of three `u32` values `[1, 2, 3]`:
```
Encoded: | 0x01 | 0x00 | 0x00 | 0x00 | 0x02 | 0x00 | 0x00 | 0x00 | 0x03 | 0x00 | 0x00 | 0x00 |
           └────── 1 (LE) ─────────┘  └────── 2 (LE) ─────────┘  └────── 3 (LE) ─────────┘
```

Buffer length = 12 bytes, element size = 4 bytes, so count = 3.

## Alternatives

### Variable-Length Integer for Counts

Using varint encoding for array counts was considered but rejected:

1. **Complexity** — Varint parsing adds overhead for minimal space savings (1 byte saved only for counts < 128)
2. **Consistency** — Fixed 2-byte counts align with other length fields (`Utf8`)
3. **Random access** — Fixed-width counts enable O(1) offset calculation

### Null-Terminated Strings Instead of Length-Prefixed

Null-terminated strings (like C strings) were considered but rejected:

1. **Binary safety** — Length-prefixed strings can contain `0x00` bytes
2. **Efficiency** — Length is known upfront without scanning for terminator
3. **Alignment** — Consistent with `TerminatedBytes` using `0x00` as a special marker

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-01-30 | Initial draft |
