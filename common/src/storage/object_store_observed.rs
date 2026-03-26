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
                    let bytes: Vec<u64> = results.iter().map(|b| b.len() as u64).collect();
                    c.record_os_get_ranges(Arc::from(location.as_ref()), &bytes);
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
        }
    }

    fn record_final(&mut self) {
        if !self.recorded {
            self.recorded = true;
            self.collector
                .record_os_read(self.signature.clone(), self.bytes_so_far, self.is_range);
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
                // Record what we have so far, then mark as error.
                self.record_final();
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
                self.record_final();
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
}
