//! Shared workload-data helpers used by both the ingest and follow benchmarks:
//! deterministic key generation, the record value template, and the per-record
//! size model. Keeping these in one place ensures both benchmarks address the
//! same key space and size records the same way.

use bytes::Bytes;

/// Generate `n` deterministic keys, each the zero-padded decimal index of width
/// `key_length` (e.g. `0000`, `0001`, ...). Both benchmarks key their records
/// this way, so the same `(n, key_length)` always yields the same key space.
pub fn keys(n: usize, key_length: usize) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("{:0>width$}", i, width = key_length)))
        .collect()
}

/// A filler record value of `value_size` bytes.
pub fn value_template(value_size: usize) -> Bytes {
    Bytes::from(vec![b'x'; value_size])
}

/// On-the-wire size of one record: key bytes plus value bytes.
pub fn record_size(key_length: usize, value_size: usize) -> usize {
    key_length + value_size
}
