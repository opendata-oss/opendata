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

    /// Offer a batch in one write-path call, without retrying: `Ok(true)` if
    /// accepted, `Ok(false)` if the write queue was full (the batch is dropped —
    /// the open-loop saturation signal, no coordinated omission). The default
    /// appends sequentially and always reports accepted; implementations with a
    /// real backpressure signal should override.
    async fn try_offer(&self, records: Vec<Record>) -> anyhow::Result<bool> {
        for r in records {
            self.append(r.key, r.value).await?;
        }
        Ok(true)
    }

    /// Read up to `max` records for `key` starting at `cursor`; the follow path.
    /// Returns how much was drained and the cursor to resume from next time.
    async fn poll(&self, key: Bytes, cursor: Cursor, max: usize) -> anyhow::Result<PollOutput>;
}

/// Where the follow path's reads are served from.
///
/// `append` always goes to the writer; only polls vary:
/// - [`ReadSource::Writer`] — polls go through the writer's own [`LogDb`] handle,
///   serving the writer's process state (subject to `read_visibility`).
/// - [`ReadSource::Reader`] — polls go through one standalone [`LogDbReader`] over
///   the shared object store: a read replica decoupled from the writer, with its
///   own block cache and refresh-interval visibility lag.
///
/// Deliberately a *single* reader, not a pool. One node serves many concurrent
/// scans from one handle, and the cost of N independent readers is derivable from
/// the single-reader numbers (modulo shared-cache effects) — running a real pool
/// just multiplies cache/GET work without modeling anything new.
enum ReadSource {
    Writer,
    Reader(LogDbReader),
}

/// LogDb implementation of [`LogStore`].
///
/// Holds the writer [`LogDb`] (the only handle that can append) plus the read
/// source used for polls.
pub struct LogDbStore {
    writer: LogDb,
    read_source: ReadSource,
    /// Count of `QueueFull` retries seen on the arrival path. This is the
    /// ingest-capacity ceiling the RFC warns about: when offered write load
    /// exceeds what the writer can absorb, appends spin here until space frees.
    queue_full: Arc<AtomicU64>,
}

impl LogDbStore {
    /// Serve both appends and polls from the writer.
    pub fn new(writer: LogDb) -> Self {
        Self {
            writer,
            read_source: ReadSource::Writer,
            queue_full: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Append through `writer` but serve polls from a standalone `reader` over the
    /// shared object store. Requires a shared (Local/Aws) object store — an
    /// in-memory store is per-handle, so the reader would never observe the
    /// writer's data.
    pub fn with_reader(writer: LogDb, reader: LogDbReader) -> Self {
        Self {
            writer,
            read_source: ReadSource::Reader(reader),
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

    /// Close the store: shut down the reader (if any), then the writer.
    pub async fn close(self) -> anyhow::Result<()> {
        if let ReadSource::Reader(reader) = self.read_source {
            reader.close().await;
        }
        self.writer.close().await?;
        Ok(())
    }

    /// Drain up to `max` records for `key` starting at `cursor`, using `reader`
    /// (either the writer's [`LogDb`] or a standalone [`LogDbReader`] — both
    /// implement [`LogRead`]). `pub(crate)` so role-split benchmarks can poll a
    /// raw reader handle directly.
    pub(crate) async fn drain<R: LogRead + Sync>(
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

    /// Append one arrival batch on the live write path, open-loop: a full write
    /// queue (`QueueFull`) is the saturation signal, so the batch is dropped and
    /// `Ok(false)` returned (the caller counts it as offered-but-rejected) rather
    /// than retried — retrying would be coordinated omission against the offered
    /// rate. `Ok(true)` = accepted; any other append error is terminal.
    ///
    /// Contrast with [`append_batch`](Self::append_batch), which *retries* on
    /// `QueueFull` and is used for bulk pre-fill where we want every record landed.
    pub async fn try_arrival(&self, records: Vec<Record>) -> anyhow::Result<bool> {
        match self.writer.try_append(records).await {
            Ok(_) => Ok(true),
            Err(AppendError::QueueFull(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Like [`try_arrival`](Self::try_arrival), but on accept returns the new
    /// append frontier — the sequence just past the last record written — so a
    /// "start at the tail" reader can resume from it. `None` means the batch was
    /// dropped (`QueueFull`).
    pub async fn try_append_frontier(&self, records: Vec<Record>) -> anyhow::Result<Option<u64>> {
        let n = records.len() as u64;
        match self.writer.try_append(records).await {
            Ok(out) => Ok(Some(out.start_sequence + n)),
            Err(AppendError::QueueFull(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

    async fn try_offer(&self, records: Vec<Record>) -> anyhow::Result<bool> {
        // Non-retrying offer: one `try_append`, drop on `QueueFull`.
        self.try_arrival(records).await
    }

    async fn poll(&self, key: Bytes, cursor: Cursor, max: usize) -> anyhow::Result<PollOutput> {
        match &self.read_source {
            ReadSource::Writer => Self::drain(&self.writer, key, cursor, max).await,
            ReadSource::Reader(reader) => Self::drain(reader, key, cursor, max).await,
        }
    }
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
