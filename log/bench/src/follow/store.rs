//! The narrow `LogStore` interface the follow workload is written against, plus
//! the LogDb implementation.
//!
//! RFC 0006 specifies the workload against a minimal interface rather than
//! LogDb's APIs directly, so the same offered load can later be replayed against
//! other systems. LogDb is the first and primary implementation
//! (`append` -> `try_append`, `poll` -> `scan(key, cursor..)`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use log::{AppendError, LogDb, LogDbReader, LogRead, Record};

/// An opaque resume position handed back on the next poll.
///
/// The workload stores whatever the implementation returns and never interprets
/// it. For LogDb it is the next [`Sequence`](log::Sequence) to read from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cursor(pub u64);

/// The result of a single poll.
///
/// The workload tracks lag itself (appended-minus-acknowledged per key), so a
/// poll only needs to report how much it drained and where to resume — we do not
/// materialize a `Vec<Record>`. The iterator is still fully drained up to `max`,
/// which is what actually drives the underlying reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollOutput {
    /// Number of records returned by this poll.
    pub n_records: usize,
    /// Total value bytes returned by this poll.
    pub n_bytes: usize,
    /// The cursor to resume from on the next poll.
    pub cursor: Cursor,
}

/// The system-under-test interface (RFC 0006).
#[async_trait]
pub trait LogStore: Send + Sync {
    /// Append one record to a key's log; the arrival path.
    async fn append(&self, key: Bytes, payload: Bytes) -> anyhow::Result<()>;

    /// Read up to `max` records for `key` starting at `cursor`; the follow path.
    /// Returns how much was drained and the cursor to resume from next time.
    async fn poll(&self, key: Bytes, cursor: Cursor, max: usize) -> anyhow::Result<PollOutput>;
}

/// Where the follow path's reads are served from.
///
/// `append` always goes to the writer; only the read path varies:
/// - [`ReadSource::Writer`] — polls go through the writer's own [`LogDb`] handle
///   (MEMORY visibility, read concurrency bounded by the one instance).
/// - [`ReadSource::Readers`] — polls are served by a pool of independent
///   [`LogDbReader`]s over the shared object store, which scales read
///   concurrency past the writer at the cost of refresh-interval visibility lag.
///   Keys are sharded across the pool by hash, so a key is always read by the
///   same reader (stable cache locality, like partitioned read replicas).
enum ReadSource {
    Writer,
    Readers(Vec<LogDbReader>),
}

/// LogDb implementation of [`LogStore`].
///
/// Holds the single writer [`LogDb`] (the only handle that can append) plus the
/// read source used for polls.
pub struct LogDbStore {
    writer: LogDb,
    readers: ReadSource,
    /// Count of `QueueFull` retries seen on the arrival path. This is the
    /// ingest-capacity ceiling the RFC warns about: when offered write load
    /// exceeds what the writer can absorb, appends spin here until space frees.
    queue_full: Arc<AtomicU64>,
}

impl LogDbStore {
    /// Serve both appends and polls from the writer (MEMORY visibility; read
    /// concurrency bounded by the single instance).
    pub fn new(writer: LogDb) -> Self {
        Self {
            writer,
            readers: ReadSource::Writer,
            queue_full: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Append through `writer` but serve polls from a pool of independent
    /// readers over the shared object store. Requires a shared (Local/Aws)
    /// object store — an in-memory store is per-handle, so a reader would never
    /// observe the writer's data.
    pub fn with_readers(writer: LogDb, readers: Vec<LogDbReader>) -> Self {
        Self {
            writer,
            readers: ReadSource::Readers(readers),
            queue_full: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Borrow the writer (e.g. to flush it).
    pub fn db(&self) -> &LogDb {
        &self.writer
    }

    /// A shared handle to the `QueueFull`-retry counter, for reporting.
    pub fn queue_full_counter(&self) -> Arc<AtomicU64> {
        self.queue_full.clone()
    }

    /// Close the store: shut down any reader pool, then close the writer.
    pub async fn close(self) -> anyhow::Result<()> {
        if let ReadSource::Readers(readers) = self.readers {
            for reader in readers {
                reader.close().await;
            }
        }
        self.writer.close().await?;
        Ok(())
    }

    /// Drain up to `max` records for `key` starting at `cursor`, using `reader`
    /// (either the writer's [`LogDb`] or a pooled [`LogDbReader`] — both
    /// implement [`LogRead`]).
    async fn drain<R: LogRead + Sync>(
        reader: &R,
        key: Bytes,
        cursor: Cursor,
        max: usize,
    ) -> anyhow::Result<PollOutput> {
        // `scan` lower bound is inclusive; we store `last_sequence + 1` as the next
        // cursor so a resumed poll never re-reads an entry. The scan API has no page
        // limit, so we bound the drain to `max` here — a deep catch-up resumes from
        // the returned cursor over several polls.
        let mut iter = reader.scan(key, cursor.0..).await?;
        let mut n_records = 0;
        let mut n_bytes = 0;
        let mut next = cursor.0;
        while n_records < max {
            match iter.next().await? {
                Some(entry) => {
                    n_records += 1;
                    n_bytes += entry.value.len();
                    next = entry.sequence + 1;
                }
                None => break,
            }
        }
        Ok(PollOutput {
            n_records,
            n_bytes,
            cursor: Cursor(next),
        })
    }

    /// Append a batch of records, retrying on `QueueFull`. Used for bulk pre-fill,
    /// where batching amortizes the write path and avoids one round-trip per record.
    pub async fn append_batch(&self, mut records: Vec<Record>) -> anyhow::Result<()> {
        loop {
            match self.writer.try_append(records).await {
                Ok(_) => return Ok(()),
                Err(AppendError::QueueFull(returned)) => {
                    self.queue_full.fetch_add(1, Ordering::Relaxed);
                    records = returned;
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[async_trait]
impl LogStore for LogDbStore {
    async fn append(&self, key: Bytes, payload: Bytes) -> anyhow::Result<()> {
        // `try_append` is non-blocking and surfaces backpressure as `QueueFull`;
        // `append_batch` retries on it. Awaiting durability is not required on the
        // arrival path; the writer treats queue pressure as a capacity signal.
        self.append_batch(vec![Record {
            key,
            value: payload,
        }])
        .await
    }

    async fn poll(&self, key: Bytes, cursor: Cursor, max: usize) -> anyhow::Result<PollOutput> {
        match &self.readers {
            ReadSource::Writer => Self::drain(&self.writer, key, cursor, max).await,
            ReadSource::Readers(readers) => {
                let idx = reader_index(&key, readers.len());
                Self::drain(&readers[idx], key, cursor, max).await
            }
        }
    }
}

/// Choose which pooled reader serves `key`, sharding by key hash so a key is
/// always read by the same reader.
fn reader_index(key: &Bytes, n: usize) -> usize {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(key.as_ref());
    (hasher.finish() % n as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::StorageConfig;
    use log::Config;

    async fn open_store() -> LogDbStore {
        let config = Config {
            storage: StorageConfig::InMemory,
            ..Default::default()
        };
        LogDbStore::new(LogDb::open(config).await.expect("open"))
    }

    fn val(n: usize) -> Bytes {
        Bytes::from(vec![b'x'; n])
    }

    #[tokio::test]
    async fn poll_empty_key_returns_nothing_and_keeps_cursor() {
        let store = open_store().await;
        let out = store
            .poll(Bytes::from("missing"), Cursor(0), 16)
            .await
            .unwrap();
        assert_eq!(out.n_records, 0);
        assert_eq!(out.n_bytes, 0);
        assert_eq!(out.cursor, Cursor(0));
    }

    #[tokio::test]
    async fn poll_drains_all_records_and_advances_cursor() {
        let store = open_store().await;
        let key = Bytes::from("k");
        for _ in 0..5 {
            store.append(key.clone(), val(10)).await.unwrap();
        }

        let out = store.poll(key.clone(), Cursor(0), 16).await.unwrap();
        assert_eq!(out.n_records, 5);
        assert_eq!(out.n_bytes, 50);
        // Cursor advanced past the last sequence; a follow-up poll sees nothing new.
        let again = store.poll(key, out.cursor, 16).await.unwrap();
        assert_eq!(again.n_records, 0);
        assert_eq!(again.cursor, out.cursor);
    }

    #[tokio::test]
    async fn bounded_page_resumes_from_returned_cursor() {
        let store = open_store().await;
        let key = Bytes::from("k");
        for _ in 0..10 {
            store.append(key.clone(), val(4)).await.unwrap();
        }

        // Page size of 4 drains in three bounded polls: 4, 4, 2.
        let p1 = store.poll(key.clone(), Cursor(0), 4).await.unwrap();
        assert_eq!(p1.n_records, 4);
        let p2 = store.poll(key.clone(), p1.cursor, 4).await.unwrap();
        assert_eq!(p2.n_records, 4);
        let p3 = store.poll(key.clone(), p2.cursor, 4).await.unwrap();
        assert_eq!(p3.n_records, 2);
        let p4 = store.poll(key, p3.cursor, 4).await.unwrap();
        assert_eq!(p4.n_records, 0);
    }

    #[tokio::test]
    async fn scan_isolates_one_key_despite_interleaved_global_sequence() {
        let store = open_store().await;
        let (a, b) = (Bytes::from("a"), Bytes::from("b"));
        // Interleave appends so the two keys' sequences are not contiguous.
        for _ in 0..3 {
            store.append(a.clone(), val(1)).await.unwrap();
            store.append(b.clone(), val(2)).await.unwrap();
        }

        let out_a = store.poll(a, Cursor(0), 16).await.unwrap();
        assert_eq!(out_a.n_records, 3);
        assert_eq!(out_a.n_bytes, 3); // three 1-byte values, none of b's

        let out_b = store.poll(b, Cursor(0), 16).await.unwrap();
        assert_eq!(out_b.n_records, 3);
        assert_eq!(out_b.n_bytes, 6); // three 2-byte values
    }
}
