//! [`TracedObjectStore`]: an [`ObjectStore`] wrapper that emits a trace span
//! for every operation, so per-request object store traffic shows up in
//! Chrome trace output.
//!
//! Each async method wraps the inner call in a `trace_span!("objstore.<op>",
//! ...)` via [`tracing::Instrument`]. Streaming methods ([`ObjectStore::list`],
//! [`ObjectStore::list_with_offset`]) return a stream adapter that enters
//! the span on every `poll_next`, so work driven by later polling (network
//! page fetches, deserialization) is also attributed to the span.

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tracing::{Instrument, Span};

use crate::tracing::trace_span;

/// An [`ObjectStore`] wrapper that emits a trace span for every operation.
///
/// Wrap any `Arc<dyn ObjectStore>` with [`TracedObjectStore::new`] and pass
/// it anywhere an `Arc<dyn ObjectStore>` is expected — SlateDB's
/// `DbBuilder::new`, `create_storage_read`, etc.
pub struct TracedObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl TracedObjectStore {
    /// Wraps `inner` so every operation emits a trace span.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }
}

impl Display for TracedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TracedObjectStore({})", self.inner)
    }
}

impl Debug for TracedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TracedObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for TracedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let span = trace_span!(
            "objstore.put_opts",
            location = %location,
            size = payload.content_length(),
        );
        self.inner
            .put_opts(location, payload, opts)
            .instrument(span)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let span = trace_span!("objstore.put_multipart_opts", location = %location);
        self.inner
            .put_multipart_opts(location, opts)
            .instrument(span)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let span = trace_span!("objstore.get_opts", location = %location);
        self.inner
            .get_opts(location, options)
            .instrument(span)
            .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        let size = range.end.saturating_sub(range.start);
        let span = trace_span!(
            "objstore.get_range",
            location = %location,
            range_start = range.start,
            range_end = range.end,
            size = size,
        );
        self.inner.get_range(location, range).instrument(span).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        let span = trace_span!(
            "objstore.get_ranges",
            location = %location,
            n_ranges = ranges.len(),
        );
        self.inner
            .get_ranges(location, ranges)
            .instrument(span)
            .await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let span = trace_span!("objstore.head", location = %location);
        self.inner.head(location).instrument(span).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let span = trace_span!("objstore.delete", location = %location);
        self.inner.delete(location).instrument(span).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let span = trace_span!("objstore.list", prefix = %OptPath(prefix));
        Box::pin(InstrumentedStream::new(self.inner.list(prefix), span))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let span = trace_span!(
            "objstore.list_with_offset",
            prefix = %OptPath(prefix),
            offset = %offset,
        );
        Box::pin(InstrumentedStream::new(
            self.inner.list_with_offset(prefix, offset),
            span,
        ))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let span = trace_span!("objstore.list_with_delimiter", prefix = %OptPath(prefix));
        self.inner
            .list_with_delimiter(prefix)
            .instrument(span)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let span = trace_span!("objstore.copy", from = %from, to = %to);
        self.inner.copy(from, to).instrument(span).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let span = trace_span!("objstore.copy_if_not_exists", from = %from, to = %to);
        self.inner
            .copy_if_not_exists(from, to)
            .instrument(span)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let span = trace_span!("objstore.rename", from = %from, to = %to);
        self.inner.rename(from, to).instrument(span).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let span = trace_span!("objstore.rename_if_not_exists", from = %from, to = %to);
        self.inner
            .rename_if_not_exists(from, to)
            .instrument(span)
            .await
    }
}

/// Display wrapper for `Option<&Path>` that renders `None` as `<none>`
/// instead of the `Some(Path("…"))` noise a `?prefix` field would emit.
struct OptPath<'a>(Option<&'a Path>);

impl Display for OptPath<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(p) => write!(f, "{}", p),
            None => f.write_str("<none>"),
        }
    }
}

/// Stream adapter that enters `span` on every `poll_next`. Work driven by
/// polling (network page fetches, deserialization) is thus attributed to the
/// span in trace output, not just the call that constructed the stream.
struct InstrumentedStream<S> {
    stream: S,
    span: Span,
}

impl<S> InstrumentedStream<S> {
    fn new(stream: S, span: Span) -> Self {
        Self { stream, span }
    }
}

impl<S: Stream + Unpin> Stream for InstrumentedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _enter = this.span.enter();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn should_delegate_put_and_get() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = TracedObjectStore::new(inner);
        let path = Path::from("hello");

        // when
        store
            .put(&path, Bytes::from_static(b"world").into())
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();

        // then
        assert_eq!(&got[..], b"world");
    }

    #[tokio::test]
    async fn should_delegate_list() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = TracedObjectStore::new(inner);
        store
            .put(&Path::from("a/1"), Bytes::from_static(b"x").into())
            .await
            .unwrap();
        store
            .put(&Path::from("a/2"), Bytes::from_static(b"y").into())
            .await
            .unwrap();

        // when
        let prefix = Path::from("a");
        let mut stream = store.list(Some(&prefix));
        let mut locations: Vec<String> = Vec::new();
        while let Some(item) = stream.next().await {
            locations.push(item.unwrap().location.to_string());
        }
        locations.sort();

        // then
        assert_eq!(locations, vec!["a/1".to_string(), "a/2".to_string()]);
    }

    #[tokio::test]
    async fn should_delegate_delete() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = TracedObjectStore::new(inner);
        let path = Path::from("doomed");
        store
            .put(&path, Bytes::from_static(b"v").into())
            .await
            .unwrap();

        // when
        store.delete(&path).await.unwrap();

        // then
        assert!(store.get(&path).await.is_err());
    }
}
