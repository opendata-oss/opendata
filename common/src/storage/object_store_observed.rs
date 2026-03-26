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

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use slatedb::object_store::{
    self, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use super::query_io::{ObjectReadSignature, QueryIoCollector, try_get_collector};

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
        let is_range = options.range.is_some();
        let path: Arc<str> = Arc::from(location.as_ref());

        match self.inner.get_opts(location, options).await {
            Ok(result) => {
                if is_head {
                    if let Some(c) = &collector {
                        c.record_os_head();
                    }
                    Ok(result)
                } else if let Some(c) = collector {
                    // Capture metadata before consuming the result into a stream.
                    let meta = result.meta.clone();
                    let range = result.range.clone();
                    let attributes = result.attributes.clone();

                    let sig = ObjectReadSignature {
                        path,
                        range_start: if is_range { Some(range.start) } else { None },
                        range_end: if is_range { Some(range.end) } else { None },
                    };

                    let stream = result.into_stream();
                    let counting_stream = ByteCountingStream::new(stream, c, sig, is_range);
                    Ok(GetResult {
                        payload: GetResultPayload::Stream(counting_stream.boxed()),
                        meta,
                        range,
                        attributes,
                    })
                } else {
                    Ok(result)
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
                Err(e)
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let collector = try_get_collector();

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
                Ok(bytes)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_read_error(true);
                }
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

        match self.inner.get_ranges(location, ranges).await {
            Ok(results) => {
                if let Some(c) = &collector {
                    let path = Arc::from(location.as_ref());
                    c.record_os_get_ranges(&path, ranges, &results);
                }
                Ok(results)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_get_ranges_error();
                }
                Err(e)
            }
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let collector = try_get_collector();

        match self.inner.head(location).await {
            Ok(meta) => {
                if let Some(c) = &collector {
                    c.record_os_head();
                }
                Ok(meta)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_head_error();
                }
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

        if let Some(c) = collector {
            ListCountingStream::new(inner_stream, c, false).boxed()
        } else {
            inner_stream
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let collector = try_get_collector();

        match self.inner.list_with_delimiter(prefix).await {
            Ok(result) => {
                if let Some(c) = &collector {
                    c.record_os_list_with_delimiter(result.objects.len() as u64);
                }
                Ok(result)
            }
            Err(e) => {
                if let Some(c) = &collector {
                    c.record_os_list_with_delimiter_error();
                }
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
    collector: QueryIoCollector,
    signature: ObjectReadSignature,
    is_range: bool,
    bytes_so_far: u64,
    recorded: bool,
    error_recorded: bool,
}

impl ByteCountingStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<Bytes>>,
        collector: QueryIoCollector,
        signature: ObjectReadSignature,
        is_range: bool,
    ) -> Self {
        Self {
            inner: stream,
            collector,
            signature,
            is_range,
            bytes_so_far: 0,
            recorded: false,
            error_recorded: false,
        }
    }

    fn record_final(&mut self) {
        if !self.recorded {
            self.recorded = true;
            self.collector
                .record_os_read(self.signature.clone(), self.bytes_so_far, self.is_range);
        }
    }

    fn record_error(&mut self) {
        if !self.error_recorded {
            self.error_recorded = true;
            self.collector.record_os_read_error(self.is_range);
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
            Poll::Ready(Some(Err(_))) => {
                // Record bytes consumed so far AND the error.
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
    collector: QueryIoCollector,
    count: u64,
    recorded: bool,
    error_recorded: bool,
    is_with_delimiter: bool,
}

impl ListCountingStream {
    fn new(
        stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
        collector: QueryIoCollector,
        is_with_delimiter: bool,
    ) -> Self {
        Self {
            inner: stream,
            collector,
            count: 0,
            recorded: false,
            error_recorded: false,
            is_with_delimiter,
        }
    }

    fn record_final(&mut self) {
        if !self.recorded {
            self.recorded = true;
            if self.is_with_delimiter {
                self.collector.record_os_list_with_delimiter(self.count);
            } else {
                self.collector.record_os_list(self.count);
            }
        }
    }

    fn record_error(&mut self) {
        if !self.error_recorded {
            self.error_recorded = true;
            self.collector.record_os_list_error();
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
            Poll::Ready(Some(Err(_))) => {
                // Record entries consumed so far AND the error.
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
}
