//! Observed object-store wrapper for per-query aggregation.
//!
//! Wraps an inner [`ObjectStore`] and records call counts, byte counts,
//! and repeated-read signatures into the task-local [`QueryIoCollector`].
//!
//! Byte counting for streaming responses (from `get_opts`) is done by
//! wrapping the payload stream and counting chunks as they are consumed.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use slatedb::object_store::{
    self, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use super::query_io::{ObjectReadSignature, QueryIoCollector, try_get_collector};

/// Extract (range_start, range_end) from a `GetRange` for logging.
///
/// For `Bounded`, returns the exact start/end. For `Offset`, returns (start, None).
/// For `Suffix`, returns (None, suffix_bytes) — note this is "last N bytes", not an
/// absolute offset, but is still useful for diagnosing the request shape.
fn get_range_params(range: &Option<object_store::GetRange>) -> (Option<u64>, Option<u64>) {
    match range {
        Some(object_store::GetRange::Bounded(r)) => (Some(r.start), Some(r.end)),
        Some(object_store::GetRange::Offset(offset)) => (Some(*offset), None),
        Some(object_store::GetRange::Suffix(n)) => (None, Some(*n)),
        None => (None, None),
    }
}

// ─── Per-call structured logging ──────────────────────────────────────────────

/// Emit one structured log event for a completed object-store call.
///
/// All fields are always present so JSON parsers don't need conditional logic.
/// Fields that are not applicable for a given op are set to 0 or "".
#[allow(clippy::too_many_arguments)]
fn log_object_store_call(
    op: &'static str,
    path: &str,
    range_start: Option<u64>,
    range_end: Option<u64>,
    range_count: Option<u32>,
    bytes_returned: Option<u64>,
    entries_returned: Option<u64>,
    status: &'static str,
    error: Option<&str>,
    started_at: SystemTime,
    started: Instant,
) {
    let started_at_unix_ms = started_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let duration_ms = started.elapsed().as_millis() as u64;
    let finished_at_unix_ms = started_at_unix_ms + duration_ms;

    tracing::debug!(
        op,
        path,
        range_start = range_start.unwrap_or(0),
        range_end = range_end.unwrap_or(0),
        range_count = range_count.unwrap_or(0),
        bytes_returned = bytes_returned.unwrap_or(0),
        entries_returned = entries_returned.unwrap_or(0),
        status,
        error = error.unwrap_or(""),
        started_at_unix_ms,
        finished_at_unix_ms,
        duration_ms,
        "object_store_call"
    );
}

/// Wraps an [`ObjectStore`] and records per-query object-store stats.
///
/// When a task-local [`QueryIoCollector`] is active, every read operation
/// records its call count and byte count into the collector. When no
/// collector is active, the wrapper delegates without overhead.
#[derive(Debug)]
pub struct ObservedObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl ObservedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Wrap the store in an Arc for use with SlateDB.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Self::new(inner))
    }
}

impl Display for ObservedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ObservedObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ObservedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let collector = try_get_collector();
        let is_head = options.head;
        let original_range = options.range.clone();
        let is_range = original_range.is_some();
        let (log_range_start, log_range_end) = get_range_params(&original_range);
        let path: Arc<str> = Arc::from(location.as_ref());
        let started_at = SystemTime::now();
        let started = Instant::now();

        match self.inner.get_opts(location, options).await {
            Ok(result) => {
                if is_head {
                    if let Some(c) = &collector {
                        c.record_os_head();
                    }
                    log_object_store_call(
                        "head_via_get_opts",
                        &path,
                        None,
                        None,
                        None,
                        Some(0),
                        None,
                        "ok",
                        None,
                        started_at,
                        started,
                    );
                    Ok(result)
                } else {
                    // Capture metadata before consuming the result into a stream.
                    let meta = result.meta.clone();
                    let range = result.range.clone();
                    let attributes = result.attributes.clone();

                    let op = if is_range { "get_opts_range" } else { "get" };
                    let sig = ObjectReadSignature {
                        path,
                        range_start: log_range_start,
                        range_end: log_range_end,
                    };

                    let stream = result.into_stream();
                    let counting_stream = ByteCountingStream::new(
                        stream, collector, sig, is_range, op, started_at, started,
                    );
                    Ok(GetResult {
                        payload: GetResultPayload::Stream(counting_stream.boxed()),
                        meta,
                        range,
                        attributes,
                    })
                }
            }
            Err(e) => {
                if let Some(c) = &collector {
                    if is_head {
                        c.record_os_head_error();
                    } else {
                        c.record_os_read_error(is_range);
                    }
                }
                let op = if is_head {
                    "head_via_get_opts"
                } else if is_range {
                    "get_opts_range"
                } else {
                    "get"
                };
                log_object_store_call(
                    op,
                    &path,
                    log_range_start,
                    log_range_end,
                    None,
                    None,
                    None,
                    "err",
                    Some(&e.to_string()),
                    started_at,
                    started,
                );
                Err(e)
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let collector = try_get_collector();
        let started_at = SystemTime::now();
        let started = Instant::now();

        match self.inner.get_range(location, range.clone()).await {
            Ok(bytes) => {
                if let Some(c) = &collector {
                    let sig = ObjectReadSignature {
                        path: Arc::from(location.as_ref()),
                        range_start: Some(range.start),
                        range_end: Some(range.end),
                    };
                    c.record_os_read(sig, bytes.len() as u64, true);
                }
                log_object_store_call(
                    "get_range",
                    location.as_ref(),
                    Some(range.start),
                    Some(range.end),
                    None,
                    Some(bytes.len() as u64),
                    None,
                    "ok",
                    None,
                    started_at,
                    started,
                );
                Ok(bytes)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_read_error(true);
                }
                log_object_store_call(
                    "get_range",
                    location.as_ref(),
                    Some(range.start),
                    Some(range.end),
                    None,
                    None,
                    None,
                    "err",
                    Some(&e.to_string()),
                    started_at,
                    started,
                );
                Err(e)
            }
        }
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let collector = try_get_collector();
        let started_at = SystemTime::now();
        let started = Instant::now();

        match self.inner.get_ranges(location, ranges).await {
            Ok(results) => {
                if let Some(c) = &collector {
                    let path = Arc::from(location.as_ref());
                    c.record_os_get_ranges(&path, ranges, &results);
                }
                let total_bytes: u64 = results.iter().map(|b| b.len() as u64).sum();
                let range_start = ranges.iter().map(|r| r.start).min();
                let range_end = ranges.iter().map(|r| r.end).max();
                log_object_store_call(
                    "get_ranges",
                    location.as_ref(),
                    range_start,
                    range_end,
                    Some(ranges.len() as u32),
                    Some(total_bytes),
                    None,
                    "ok",
                    None,
                    started_at,
                    started,
                );
                Ok(results)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_get_ranges_error();
                }
                let range_start = ranges.iter().map(|r| r.start).min();
                let range_end = ranges.iter().map(|r| r.end).max();
                log_object_store_call(
                    "get_ranges",
                    location.as_ref(),
                    range_start,
                    range_end,
                    Some(ranges.len() as u32),
                    None,
                    None,
                    "err",
                    Some(&e.to_string()),
                    started_at,
                    started,
                );
                Err(e)
            }
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let collector = try_get_collector();
        let started_at = SystemTime::now();
        let started = Instant::now();

        match self.inner.head(location).await {
            Ok(meta) => {
                if let Some(c) = &collector {
                    c.record_os_head();
                }
                log_object_store_call(
                    "head",
                    location.as_ref(),
                    None,
                    None,
                    None,
                    Some(0),
                    None,
                    "ok",
                    None,
                    started_at,
                    started,
                );
                Ok(meta)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_head_error();
                }
                log_object_store_call(
                    "head",
                    location.as_ref(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    "err",
                    Some(&e.to_string()),
                    started_at,
                    started,
                );
                Err(e)
            }
        }
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let collector = try_get_collector();
        let inner_stream = self.inner.list(prefix);
        let prefix_str: Arc<str> = Arc::from(prefix.map(|p| p.as_ref()).unwrap_or(""));
        let started_at = SystemTime::now();
        let started = Instant::now();

        // Always wrap to ensure per-call logging regardless of collector presence.
        ListCountingStream::new(
            inner_stream,
            collector,
            false,
            prefix_str,
            started_at,
            started,
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let collector = try_get_collector();
        let path_str = prefix.map(|p| p.as_ref()).unwrap_or("");
        let started_at = SystemTime::now();
        let started = Instant::now();

        match self.inner.list_with_delimiter(prefix).await {
            Ok(result) => {
                if let Some(c) = &collector {
                    c.record_os_list_with_delimiter(result.objects.len() as u64);
                }
                log_object_store_call(
                    "list_with_delimiter",
                    path_str,
                    None,
                    None,
                    None,
                    Some(0),
                    Some(result.objects.len() as u64),
                    "ok",
                    None,
                    started_at,
                    started,
                );
                Ok(result)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_list_with_delimiter_error();
                }
                log_object_store_call(
                    "list_with_delimiter",
                    path_str,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "err",
                    Some(&e.to_string()),
                    started_at,
                    started,
                );
                Err(e)
            }
        }
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ─── ByteCountingStream ────────────────────────────────────────────────────

/// Wraps a `GetResult` payload stream and counts bytes as they are consumed.
///
/// On drop (or stream completion), records the total bytes and signature
/// into the collector. We record on each chunk poll rather than waiting
/// for drop, because drop may not happen on the right task.
struct ByteCountingStream {
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    collector: Option<QueryIoCollector>,
    signature: ObjectReadSignature,
    is_range: bool,
    bytes_so_far: u64,
    recorded: bool,
    error_recorded: bool,
    // Per-call logging fields.
    op: &'static str,
    started_at: SystemTime,
    started: Instant,
    error_msg: Option<String>,
}

impl ByteCountingStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<Bytes>>,
        collector: Option<QueryIoCollector>,
        signature: ObjectReadSignature,
        is_range: bool,
        op: &'static str,
        started_at: SystemTime,
        started: Instant,
    ) -> Self {
        Self {
            inner: stream,
            collector,
            signature,
            is_range,
            bytes_so_far: 0,
            recorded: false,
            error_recorded: false,
            op,
            started_at,
            started,
            error_msg: None,
        }
    }

    fn record_final(&mut self) {
        if !self.recorded {
            self.recorded = true;
            if let Some(c) = &self.collector {
                c.record_os_read(self.signature.clone(), self.bytes_so_far, self.is_range);
            }
            log_object_store_call(
                self.op,
                &self.signature.path,
                self.signature.range_start,
                self.signature.range_end,
                None,
                Some(self.bytes_so_far),
                None,
                if self.error_msg.is_some() {
                    "err"
                } else {
                    "ok"
                },
                self.error_msg.as_deref(),
                self.started_at,
                self.started,
            );
        }
    }

    fn record_error(&mut self) {
        if !self.error_recorded {
            self.error_recorded = true;
            if let Some(c) = &self.collector {
                c.record_os_read_error(self.is_range);
            }
        }
    }
}

impl Stream for ByteCountingStream {
    type Item = object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(Some(Ok(bytes))) => {
                self.bytes_so_far += bytes.len() as u64;
            }
            Poll::Ready(Some(Err(e))) => {
                // Capture error message before record_final so the log includes it.
                self.error_msg = Some(e.to_string());
                self.record_final();
                self.record_error();
            }
            Poll::Ready(None) => {
                // Stream complete — record total.
                self.record_final();
            }
            Poll::Pending => {}
        }
        poll
    }
}

impl Drop for ByteCountingStream {
    fn drop(&mut self) {
        // Safety net: if the stream is dropped before completion, record what we have.
        self.record_final();
    }
}

// ─── ListCountingStream ────────────────────────────────────────────────────

/// Wraps a list stream and counts entries yielded.
struct ListCountingStream {
    inner: BoxStream<'static, object_store::Result<ObjectMeta>>,
    collector: Option<QueryIoCollector>,
    count: u64,
    recorded: bool,
    error_recorded: bool,
    is_with_delimiter: bool,
    // Per-call logging fields.
    prefix: Arc<str>,
    started_at: SystemTime,
    started: Instant,
    error_msg: Option<String>,
}

impl ListCountingStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
        collector: Option<QueryIoCollector>,
        is_with_delimiter: bool,
        prefix: Arc<str>,
        started_at: SystemTime,
        started: Instant,
    ) -> Self {
        Self {
            inner: stream,
            collector,
            count: 0,
            recorded: false,
            error_recorded: false,
            is_with_delimiter,
            prefix,
            started_at,
            started,
            error_msg: None,
        }
    }

    fn record_final(&mut self) {
        if !self.recorded {
            self.recorded = true;
            if let Some(c) = &self.collector {
                if self.is_with_delimiter {
                    c.record_os_list_with_delimiter(self.count);
                } else {
                    c.record_os_list(self.count);
                }
            }
            log_object_store_call(
                "list",
                &self.prefix,
                None,
                None,
                None,
                Some(0),
                Some(self.count),
                if self.error_msg.is_some() {
                    "err"
                } else {
                    "ok"
                },
                self.error_msg.as_deref(),
                self.started_at,
                self.started,
            );
        }
    }

    fn record_error(&mut self) {
        if !self.error_recorded {
            self.error_recorded = true;
            if let Some(c) = &self.collector {
                c.record_os_list_error();
            }
        }
    }
}

impl Stream for ListCountingStream {
    type Item = object_store::Result<ObjectMeta>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(Some(Ok(_))) => {
                self.count += 1;
            }
            Poll::Ready(Some(Err(e))) => {
                // Capture error message before record_final so the log includes it.
                self.error_msg = Some(e.to_string());
                self.record_final();
                self.record_error();
            }
            Poll::Ready(None) => {
                self.record_final();
            }
            Poll::Pending => {}
        }
        poll
    }
}

impl Drop for ListCountingStream {
    fn drop(&mut self) {
        self.record_final();
    }
}

#[cfg(test)]
mod tests {
    use super::super::query_io::{self, QueryIoCollector};
    use super::*;
    use slatedb::object_store::memory::InMemory;

    async fn setup_store_with_data() -> Arc<dyn ObjectStore> {
        let mem = Arc::new(InMemory::new());
        mem.put(
            &Path::from("test/obj1.sst"),
            PutPayload::from_static(b"hello world 12345"),
        )
        .await
        .unwrap();
        mem.put(
            &Path::from("test/obj2.sst"),
            PutPayload::from_static(b"second object data here"),
        )
        .await
        .unwrap();
        ObservedObjectStore::wrap(mem)
    }

    #[tokio::test]
    async fn should_record_get_range_bytes() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let bytes = store
                .get_range(&Path::from("test/obj1.sst"), 0..5)
                .await
                .unwrap();
            assert_eq!(bytes.len(), 5);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_range_calls, 1);
        assert_eq!(stats.os_get_range_bytes, 5);
        assert_eq!(stats.os_distinct_read_signatures, 1);
    }

    #[tokio::test]
    async fn should_record_get_ranges_bytes() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let results = store
                .get_ranges(&Path::from("test/obj1.sst"), &[0..3, 5..10])
                .await
                .unwrap();
            assert_eq!(results[0].len(), 3);
            assert_eq!(results[1].len(), 5);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_ranges_calls, 1);
        assert_eq!(stats.os_get_ranges_bytes, 8);
    }

    #[tokio::test]
    async fn should_record_head_calls_without_bytes() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let meta = store.head(&Path::from("test/obj1.sst")).await.unwrap();
            assert_eq!(meta.size, 17);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_head_calls, 1);
        assert_eq!(stats.os_read_bytes_total(), 0);
    }

    #[tokio::test]
    async fn should_record_list_entries() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let entries: Vec<_> = store
                .list(Some(&Path::from("test")))
                .collect::<Vec<_>>()
                .await;
            assert_eq!(entries.len(), 2);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_list_calls, 1);
        assert_eq!(stats.os_list_entries, 2);
    }

    #[tokio::test]
    async fn should_record_repeated_reads_for_same_signature() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            // Read the same range 3 times.
            for _ in 0..3 {
                let _ = store
                    .get_range(&Path::from("test/obj1.sst"), 0..5)
                    .await
                    .unwrap();
            }
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_range_calls, 3);
        assert_eq!(stats.os_distinct_read_signatures, 1);
        assert_eq!(stats.os_repeated_read_calls, 2);
        assert_eq!(stats.os_max_repeats_single_signature, 3);
    }

    #[tokio::test]
    async fn should_not_record_when_no_collector_is_active() {
        let store = setup_store_with_data().await;

        // No collector scope — should not panic.
        let bytes = store
            .get_range(&Path::from("test/obj1.sst"), 0..5)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 5);

        let _ = store.head(&Path::from("test/obj1.sst")).await.unwrap();
    }

    #[tokio::test]
    async fn should_record_full_get_with_stream_byte_counting() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let result = store.get(&Path::from("test/obj1.sst")).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.len(), 17);
        })
        .await;

        let stats = collector.snapshot();
        // get_opts without range → full get
        assert_eq!(stats.os_get_calls + stats.os_get_range_calls, 1);
        assert_eq!(stats.os_read_bytes_total(), 17);
        assert_eq!(stats.os_distinct_read_signatures, 1);
    }

    // ─── Stream error tests ────────────────────────────────────────────────

    /// An object store that yields `n` chunks of `chunk_size` bytes, then an error.
    #[derive(Debug)]
    struct FailingChunkStore {
        inner: Arc<dyn ObjectStore>,
        /// Number of successful chunks before the error.
        ok_chunks: usize,
        /// Size of each successful chunk.
        chunk_size: usize,
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingChunkStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            // Delegate to inner for metadata, then replace payload with our failing stream.
            let result = self.inner.get_opts(location, options).await?;
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();

            let ok_chunks = self.ok_chunks;
            let chunk_size = self.chunk_size;
            let stream = futures::stream::iter((0..ok_chunks + 1).map(move |i| {
                if i < ok_chunks {
                    Ok(Bytes::from(vec![0xAB; chunk_size]))
                } else {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: "injected error".into(),
                    })
                }
            }));

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream.boxed()),
                meta,
                range,
                attributes,
            })
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    impl Display for FailingChunkStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "FailingChunkStore({})", self.inner)
        }
    }

    #[tokio::test]
    async fn should_record_get_stream_error_after_partial_bytes() {
        let mem = Arc::new(InMemory::new());
        mem.put(
            &Path::from("test/obj.sst"),
            PutPayload::from_static(b"hello world 12345"),
        )
        .await
        .unwrap();

        let failing: Arc<dyn ObjectStore> = Arc::new(FailingChunkStore {
            inner: mem,
            ok_chunks: 3,
            chunk_size: 10,
        });
        let store = ObservedObjectStore::wrap(failing);
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let result = store.get(&Path::from("test/obj.sst")).await.unwrap();
            // Consume the stream — it will yield 3 chunks then error.
            let err = result.bytes().await;
            assert!(err.is_err());
        })
        .await;

        let stats = collector.snapshot();
        // 3 chunks of 10 bytes each were delivered before the error.
        assert_eq!(stats.os_get_bytes, 30);
        assert_eq!(stats.os_get_calls, 1);
        assert_eq!(stats.os_get_errors, 1);
    }

    /// A list stream that yields `n` entries then errors.
    #[derive(Debug)]
    struct FailingListStore {
        inner: Arc<dyn ObjectStore>,
        /// Number of successful entries before the error.
        ok_entries: usize,
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let ok_entries = self.ok_entries;
            let inner_stream = self.inner.list(prefix);

            // Take ok_entries items, then yield an error.
            let capped = futures::stream::unfold(
                (inner_stream, 0usize, ok_entries, false),
                |(mut stream, count, limit, done)| async move {
                    if done {
                        return None;
                    }
                    if count >= limit {
                        return Some((
                            Err(object_store::Error::Generic {
                                store: "test",
                                source: "injected list error".into(),
                            }),
                            (stream, count, limit, true),
                        ));
                    }
                    match stream.next().await {
                        Some(item) => Some((item, (stream, count + 1, limit, false))),
                        None => None,
                    }
                },
            );
            capped.boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    impl Display for FailingListStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "FailingListStore({})", self.inner)
        }
    }

    #[tokio::test]
    async fn should_record_list_stream_error_after_partial_entries() {
        let mem = Arc::new(InMemory::new());
        for i in 0..5 {
            mem.put(
                &Path::from(format!("test/obj{i}.sst")),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();
        }

        let failing: Arc<dyn ObjectStore> = Arc::new(FailingListStore {
            inner: mem,
            ok_entries: 3,
        });
        let store = ObservedObjectStore::wrap(failing);
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let entries: Vec<_> = store
                .list(Some(&Path::from("test")))
                .collect::<Vec<_>>()
                .await;
            // 3 Ok entries + 1 Err.
            assert_eq!(entries.len(), 4);
            assert!(entries[3].is_err());
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_list_calls, 1);
        assert_eq!(stats.os_list_entries, 3);
        assert_eq!(stats.os_list_errors, 1);
    }

    // ─── Deterministic multi-chunk stream tests ──────────────────────────

    /// An object store that splits get_opts responses into fixed-size chunks.
    #[derive(Debug)]
    struct ChunkedStore {
        inner: Arc<dyn ObjectStore>,
        chunk_size: usize,
    }

    #[async_trait::async_trait]
    impl ObjectStore for ChunkedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let result = self.inner.get_opts(location, options).await?;
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();

            // Collect the full payload, then re-chunk it.
            let full = result.bytes().await?;
            let chunk_size = self.chunk_size;
            let chunks: Vec<Bytes> = full
                .chunks(chunk_size)
                .map(|c| Bytes::copy_from_slice(c))
                .collect();
            let stream = futures::stream::iter(chunks.into_iter().map(Ok));

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream.boxed()),
                meta,
                range,
                attributes,
            })
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    impl Display for ChunkedStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "ChunkedStore({})", self.inner)
        }
    }

    #[tokio::test]
    async fn should_count_exact_bytes_across_multiple_stream_chunks() {
        let mem = Arc::new(InMemory::new());
        // 17 bytes, chunked into 5-byte pieces → 4 chunks (5+5+5+2).
        mem.put(
            &Path::from("test/obj.sst"),
            PutPayload::from_static(b"hello world 12345"),
        )
        .await
        .unwrap();

        let chunked: Arc<dyn ObjectStore> = Arc::new(ChunkedStore {
            inner: mem,
            chunk_size: 5,
        });
        let store = ObservedObjectStore::wrap(chunked);
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            let result = store.get(&Path::from("test/obj.sst")).await.unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.len(), 17);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_calls, 1);
        assert_eq!(stats.os_get_bytes, 17);
        assert_eq!(stats.os_get_errors, 0);
        assert_eq!(stats.os_distinct_read_signatures, 1);
    }

    #[tokio::test]
    async fn should_count_exact_bytes_for_ranged_stream() {
        let mem = Arc::new(InMemory::new());
        mem.put(
            &Path::from("test/obj.sst"),
            PutPayload::from_static(b"hello world 12345"),
        )
        .await
        .unwrap();

        let chunked: Arc<dyn ObjectStore> = Arc::new(ChunkedStore {
            inner: mem,
            chunk_size: 3,
        });
        let store = ObservedObjectStore::wrap(chunked);
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            // Range get [2..10) → 8 bytes, chunked into 3-byte pieces → 3 chunks (3+3+2).
            let result = store
                .get_opts(
                    &Path::from("test/obj.sst"),
                    GetOptions {
                        range: Some(object_store::GetRange::Bounded(std::ops::Range {
                            start: 2,
                            end: 10,
                        })),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let bytes = result.bytes().await.unwrap();
            assert_eq!(bytes.len(), 8);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_range_calls, 1);
        assert_eq!(stats.os_get_range_bytes, 8);
        assert_eq!(stats.os_get_errors, 0);
        assert_eq!(stats.os_get_range_errors, 0);
        assert_eq!(stats.os_distinct_read_signatures, 1);
    }

    // ─── get_ranges signature tracking tests ───────────────────────────────

    #[tokio::test]
    async fn should_track_distinct_signatures_per_get_ranges_subrange() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            // Single get_ranges call with 2 subranges.
            let _ = store
                .get_ranges(&Path::from("test/obj1.sst"), &[0..3, 5..10])
                .await
                .unwrap();
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_ranges_calls, 1);
        // Each subrange is a distinct signature.
        assert_eq!(stats.os_distinct_read_signatures, 2);
        assert_eq!(stats.os_repeated_read_calls, 0);
    }

    #[tokio::test]
    async fn should_track_repeated_subranges_across_get_ranges_calls() {
        let store = setup_store_with_data().await;
        let collector = QueryIoCollector::new();

        query_io::run_with_collector(&collector, async {
            // First call: subranges [0..3] and [5..10].
            let _ = store
                .get_ranges(&Path::from("test/obj1.sst"), &[0..3, 5..10])
                .await
                .unwrap();
            // Second call: subrange [0..3] again (repeat) and [10..15].
            let _ = store
                .get_ranges(&Path::from("test/obj1.sst"), &[0..3, 10..15])
                .await
                .unwrap();
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_ranges_calls, 2);
        // 3 distinct signatures: [0..3], [5..10], [10..15]
        assert_eq!(stats.os_distinct_read_signatures, 3);
        // [0..3] was read twice → 1 repeated call
        assert_eq!(stats.os_repeated_read_calls, 1);
        assert_eq!(stats.os_max_repeats_single_signature, 2);
    }

    // ─── Per-call log capture tests ─────────────────────────────────────

    /// Tests that validate the actual tracing events emitted by each method.
    /// Uses a custom capturing subscriber to collect events, avoiding the
    /// subscriber-isolation problems that plague global/thread-local defaults.
    mod log_capture {
        use super::*;
        use std::collections::HashMap;
        use std::sync::Mutex;
        use tracing::field::{Field, Visit};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Debug)]
        struct CapturedEvent {
            fields: HashMap<String, String>,
        }

        impl CapturedEvent {
            fn get(&self, key: &str) -> Option<&str> {
                self.fields.get(key).map(|s| s.as_str())
            }
        }

        struct CapturingLayer {
            events: Arc<Mutex<Vec<CapturedEvent>>>,
        }

        #[derive(Default)]
        struct FieldCollector {
            fields: HashMap<String, String>,
        }

        impl Visit for FieldCollector {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.fields
                    .insert(field.name().to_string(), format!("{:?}", value));
            }
            fn record_str(&mut self, field: &Field, value: &str) {
                self.fields
                    .insert(field.name().to_string(), value.to_string());
            }
            fn record_u64(&mut self, field: &Field, value: u64) {
                self.fields
                    .insert(field.name().to_string(), value.to_string());
            }
            fn record_i64(&mut self, field: &Field, value: i64) {
                self.fields
                    .insert(field.name().to_string(), value.to_string());
            }
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturingLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut visitor = FieldCollector::default();
                event.record(&mut visitor);
                self.events.lock().unwrap().push(CapturedEvent {
                    fields: visitor.fields,
                });
            }
        }

        /// Run an async block with a capturing subscriber scoped to this thread.
        /// Returns only the `object_store_call` events.
        fn run_capturing<F: std::future::Future<Output = ()>>(
            f: impl FnOnce() -> F,
        ) -> Vec<CapturedEvent> {
            let events = Arc::new(Mutex::new(Vec::new()));
            let layer = CapturingLayer {
                events: events.clone(),
            };
            let subscriber = tracing_subscriber::registry().with(layer);

            tracing::subscriber::with_default(subscriber, || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(f())
            });

            let all = Arc::try_unwrap(events).unwrap().into_inner().unwrap();
            all.into_iter()
                .filter(|e| e.get("message") == Some("object_store_call"))
                .collect()
        }

        fn setup_store() -> Arc<dyn ObjectStore> {
            ObservedObjectStore::wrap(Arc::new(InMemory::new()))
        }

        async fn put_data(store: &Arc<dyn ObjectStore>) {
            store
                .put(
                    &Path::from("test/obj.sst"),
                    PutPayload::from_static(b"hello world 12345"),
                )
                .await
                .unwrap();
        }

        #[test]
        fn emits_log_for_get_range() {
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let _ = store
                    .get_range(&Path::from("test/obj.sst"), 2..10)
                    .await
                    .unwrap();
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("get_range"));
            assert_eq!(e.get("path"), Some("test/obj.sst"));
            assert_eq!(e.get("range_start"), Some("2"));
            assert_eq!(e.get("range_end"), Some("10"));
            assert_eq!(e.get("bytes_returned"), Some("8"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_for_get_without_collector() {
            // Finding 1 regression: get must log even without a QueryIoCollector.
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let result = store.get(&Path::from("test/obj.sst")).await.unwrap();
                let bytes = result.bytes().await.unwrap();
                assert_eq!(bytes.len(), 17);
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("get"));
            assert_eq!(e.get("bytes_returned"), Some("17"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_for_list_without_collector() {
            // Finding 1 regression: list must log even without a QueryIoCollector.
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let entries: Vec<_> = store
                    .list(Some(&Path::from("test")))
                    .collect::<Vec<_>>()
                    .await;
                assert_eq!(entries.len(), 1);
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("list"));
            assert_eq!(e.get("entries_returned"), Some("1"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_for_get_ranges_with_boundaries() {
            // Finding 2: get_ranges logs range_start/range_end from actual ranges.
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let _ = store
                    .get_ranges(&Path::from("test/obj.sst"), &[2..5, 8..12])
                    .await
                    .unwrap();
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("get_ranges"));
            assert_eq!(e.get("range_start"), Some("2"));
            assert_eq!(e.get("range_end"), Some("12"));
            assert_eq!(e.get("range_count"), Some("2"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_for_head() {
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let _ = store.head(&Path::from("test/obj.sst")).await.unwrap();
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("head"));
            assert_eq!(e.get("bytes_returned"), Some("0"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_for_get_opts_range_with_original_params() {
            // Finding 2: uses original request range, not resolved range.
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let result = store
                    .get_opts(
                        &Path::from("test/obj.sst"),
                        GetOptions {
                            range: Some(object_store::GetRange::Bounded(std::ops::Range {
                                start: 3,
                                end: 10,
                            })),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let bytes = result.bytes().await.unwrap();
                assert_eq!(bytes.len(), 7);
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("get_opts_range"));
            assert_eq!(e.get("range_start"), Some("3"));
            assert_eq!(e.get("range_end"), Some("10"));
            assert_eq!(e.get("bytes_returned"), Some("7"));
            assert_eq!(e.get("status"), Some("ok"));
        }

        #[test]
        fn emits_log_with_range_on_error() {
            // Finding 2: get_opts error path must log original range params.
            let events = run_capturing(|| async {
                let store = setup_store();
                let err = store
                    .get_opts(
                        &Path::from("test/missing.sst"),
                        GetOptions {
                            range: Some(object_store::GetRange::Bounded(std::ops::Range {
                                start: 0,
                                end: 100,
                            })),
                            ..Default::default()
                        },
                    )
                    .await;
                assert!(err.is_err());
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("get_opts_range"));
            assert_eq!(e.get("range_start"), Some("0"));
            assert_eq!(e.get("range_end"), Some("100"));
            assert_eq!(e.get("status"), Some("err"));
            assert!(e.get("error").is_some());
            assert!(!e.get("error").unwrap().is_empty());
        }

        #[test]
        fn emits_log_for_list_with_delimiter() {
            let events = run_capturing(|| async {
                let store = setup_store();
                put_data(&store).await;
                let _ = store
                    .list_with_delimiter(Some(&Path::from("test")))
                    .await
                    .unwrap();
            });

            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.get("op"), Some("list_with_delimiter"));
            assert_eq!(e.get("status"), Some("ok"));
        }
    }
}
