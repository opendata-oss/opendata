# AGENTS.md

**Start here**: Read [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow, RFC process, code style, and testing patterns.

## Project Overview

OpenData is a collection of deployable database systems that share common infrastructure. Each database is its own crate but leverages shared logic from `common`. All databases are built on [SlateDB](https://github.com/slatedb/slatedb).

**Important**: When working on a specific database, focus on that crate's directory. Each database has its own `AGENTS.md` with architecture details. Avoid loading context from other database crates unless explicitly needed.

### Crates

- **common**: Shared library containing storage abstractions, serde utilities, and sequence allocation
- **timeseries**: Time series database with Prometheus-compatible semantics (time buckets, inverted indexes, Gorilla compression)
- **vector**: Vector database with SPANN-style ANN search (centroids, posting lists, metadata filtering)
- **log**: Kafka-like log with per-key streams and global sequence ordering

### Shared Architectural Patterns

All databases follow these patterns (see RFCs in each crate's `rfcs/` directory):

1. **Key encoding**: 2-byte prefix (version u8 + record_tag u8), big-endian for lexicographic ordering
2. **Value encoding**: Little-endian for performance on common architectures
3. **Common types**: `Utf8`, `Array<T>`, `FixedElementArray<T>`, `TerminatedBytes`, `RoaringBitmap/Treemap`
4. **Sequence allocation**: Block-based `SeqBlock` for crash-safe ID generation (see `common/src/sequence.rs`)
5. **Storage abstraction**: `Storage` trait in common wraps SlateDB with merge operators

## Development

### Building

```bash
cargo build              # Build all crates
cargo build -p timeseries # Build specific crate
```

### Testing

```bash
cargo test --all         # Run all tests
cargo test -p timeseries  # Test specific crate
```

### Formatting and Linting

Always run these before committing:

```bash
cargo fmt                # Format code
cargo clippy             # Run lints
```

**Important**: After modifying any `.rs` file, run `cargo fmt`.

## Code Conventions

### Tests

- Use the **given/when/then** pattern for test structure
- Name tests `should_xyz` to describe expected behavior

```rust
#[test]
fn should_return_error_when_key_not_found() {
    // given
    let store = Store::new();

    // when
    let result = store.get("missing_key");

    // then
    assert!(result.is_err());
}
```

### Encoding and Serialization

- Prefer `bytes::Bytes` and `bytes::BytesMut` over `Vec<u8>` for byte buffers
- Use `Bytes` for immutable byte slices (return types, function parameters)
- Use `BytesMut` for mutable byte buffers during encoding
- The `bytes` crate is available as a workspace dependency

### Imports

- Place all `use` statements at the module level, not inside functions or methods
- Group imports logically (standard library, external crates, local modules)

```rust
// Good
use bytes::{BufMut, BytesMut};

fn some_function() {
    let mut buf = BytesMut::new();
}

// Bad
fn some_function() {
    use bytes::BufMut;  // Don't do this
}
```

### Dependencies

- **slatedb**: The underlying storage engine for all database implementations
- **bytes**: Byte buffer types (`Bytes`, `BytesMut`) for efficient zero-copy operations
- Workspace dependencies are defined in the root `Cargo.toml`
- Prefer adding dependencies at the workspace level when shared across crates

### Adding a New Crate

1. Create the crate directory with `Cargo.toml` and `src/`
2. Add the crate name to `members` in the root `Cargo.toml`
3. Use `version.workspace = true` and `edition.workspace = true`
4. Add shared dependencies with `.workspace = true`
