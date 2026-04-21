//! Adapter from the per-bucket [`QueryReader`] surface to [`SeriesSource`].
//!
//! Fans out per-bucket selector resolution / sample streaming. Callers see
//! single streams; cross-bucket stitching lives inside this source.
//!
//! A series spanning multiple buckets yields one [`SampleBatch`] per bucket
//! in bucket-timestamp order; operators merge across buckets. Selector
//! matcher logic lives in [`selector_util`].

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use futures::stream::{self, StreamExt, TryStreamExt};
use promql_parser::parser::VectorSelector;

use crate::model::{Label, Labels, Sample, SeriesId, TimeBucket};
use crate::query::QueryReader;

use super::index_cache::IndexCache;
use super::memory::QueryError;
use super::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest, SeriesSource,
    TimeRange,
};

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------
//
// Buckets are fully independent keyspaces in RFC 0001's layout (all record
// keys are bucket-prefixed, series IDs are bucket-scoped), so cross-bucket
// fan-out cannot affect correctness. RFC 0007 §"Execution Model" (line 256)
// prohibits implicit spawn-per-series, so there is no intra-bucket sample
// fan-out; the constants below act as both scheduler and I/O ceiling.

/// Cross-bucket readahead for resolve.
const METADATA_STAGE_READAHEAD: usize = 32;

/// Cross-bucket readahead for sample batching.
const SAMPLE_STAGE_READAHEAD: usize = 32;

/// Per-key index-fetch fan-out inside one bucket's resolve path. Worst-case
/// in-flight gets during build_physical = `METADATA_STAGE_READAHEAD * INDEX_PER_KEY`.
const INDEX_PER_KEY_CONCURRENCY: usize = 32;

// ---------------------------------------------------------------------------
// Bucket-id encoding
// ---------------------------------------------------------------------------

/// Pack `(start, size)` into the opaque `u64` carried by
/// [`ResolvedSeriesRef::bucket_id`].
#[inline]
fn encode_bucket(bucket: TimeBucket) -> u64 {
    ((bucket.start as u64) << 8) | (bucket.size as u64)
}

/// Inverse of [`encode_bucket`]. `None` only on defensive size=0 guard.
#[inline]
fn decode_bucket(bucket_id: u64) -> Option<TimeBucket> {
    let size_bits = (bucket_id & 0xFF) as u8;
    let start = (bucket_id >> 8) as u32;
    if size_bits == 0 {
        return None;
    }
    Some(TimeBucket {
        start,
        size: size_bits,
    })
}

/// `[start_ms, end_ms)` covered by a bucket.
#[inline]
fn bucket_ms_window(bucket: TimeBucket) -> (i64, i64) {
    let start_ms = (bucket.start as i64) * 60 * 1000;
    let end_ms = start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
    (start_ms, end_ms)
}

/// Matches the retain clause in `QueryPlan::for_matrix`.
#[inline]
fn bucket_overlaps(bucket: TimeBucket, time_range: TimeRange) -> bool {
    if time_range.is_empty() {
        return false;
    }
    let (bucket_start_ms, bucket_end_ms) = bucket_ms_window(bucket);
    !(bucket_end_ms <= time_range.start_ms || bucket_start_ms >= time_range.end_ms_exclusive)
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// [`SeriesSource`] over the crate-internal [`QueryReader`]. One instance per
/// query. `resolve` consults the query-scoped index cache; `samples` needs no
/// cache at all — metric names ride on `ResolvedSeriesRef`.
///
/// The cache lives for the entire query lifetime rather than being dropped
/// after `build_physical_plan`: subquery operators re-enter the planner at
/// execution time (see `build_subquery` → `build_node` → `resolve_leaf`),
/// and that path needs the cache warm for the inner subtree's resolves.
pub(crate) struct QueryReaderSource<R: QueryReader> {
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
}

impl<R: QueryReader> QueryReaderSource<R> {
    /// Creates a fresh query-scoped index cache alongside the adapter.
    pub(crate) fn new(reader: Arc<R>) -> Self {
        Self {
            reader,
            index_cache: Arc::new(IndexCache::new()),
        }
    }
}

impl<R: QueryReader + 'static> SeriesSource for QueryReaderSource<R> {
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        let reader = self.reader.clone();
        let index_cache = self.index_cache.clone();
        let selector = selector.clone();
        resolve_stream(reader, index_cache, selector, time_range)
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let reader = self.reader.clone();
        samples_stream(reader, request)
    }
}

// ---------------------------------------------------------------------------
// resolve()
// ---------------------------------------------------------------------------

/// Fan out selector resolution across overlapping buckets and emit one chunk
/// per non-empty bucket.
fn resolve_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    async_stream_resolve(reader, index_cache, selector, time_range)
}

/// Up to [`METADATA_STAGE_READAHEAD`] buckets resolve in parallel; emission
/// order is chronological (oldest first) via [`StreamExt::buffered`]. Empty
/// buckets are skipped.
fn async_stream_resolve<R: QueryReader + 'static>(
    reader: Arc<R>,
    index_cache: Arc<IndexCache>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    stream::once(async move {
        let buckets = match reader.list_buckets().await {
            Ok(bs) => bs,
            Err(e) => return vec![Err(internal_err(e.to_string()))],
        };

        let mut filtered: Vec<TimeBucket> = buckets
            .into_iter()
            .filter(|b| bucket_overlaps(*b, time_range))
            .collect();
        // Deterministic emission order so tests and downstream planner
        // see stable ordering across calls; chronological (oldest first).
        filtered.sort_by_key(|b| b.start);

        let selector = Arc::new(selector);
        let mut fut_stream =
            stream::iter(
                filtered.into_iter().map(|bucket| {
                    let reader = reader.clone();
                    let index_cache = index_cache.clone();
                    let selector = selector.clone();
                    async move {
                        resolve_one_bucket(reader.as_ref(), &index_cache, bucket, &selector).await
                    }
                }),
            )
            .buffered(METADATA_STAGE_READAHEAD);

        let mut out: Vec<Result<ResolvedSeriesChunk, QueryError>> = Vec::new();
        while let Some(r) = fut_stream.next().await {
            match r {
                Ok(Some(chunk)) => out.push(Ok(chunk)),
                Ok(None) => {}
                Err(e) => {
                    // On error we stop consuming; the remaining in-flight
                    // buckets are dropped (their futures cancel) when
                    // `fut_stream` goes out of scope.
                    out.push(Err(e));
                    break;
                }
            }
        }
        out
    })
    .flat_map(stream::iter)
}

/// `Ok(None)` when no series match. Forward-index entries populate the
/// per-`(bucket, series_id)` cache, which the samples path later reuses.
async fn resolve_one_bucket<R: QueryReader + ?Sized>(
    reader: &R,
    index_cache: &IndexCache,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> Result<Option<ResolvedSeriesChunk>, QueryError> {
    let candidates = selector_util::find_candidates(reader, index_cache, &bucket, selector).await?;
    if candidates.is_empty() {
        return Ok(None);
    }

    // Fan out: one forward-index get per candidate, parallel through the
    // index cache. Duplicate ids are filtered with a sort+dedup first so
    // we don't waste concurrency slots.
    let mut unique_ids = candidates.clone();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    let specs: HashMap<SeriesId, crate::index::SeriesSpec> =
        stream::iter(unique_ids.into_iter().map(|sid| async move {
            let slot = index_cache
                .forward_index_one(reader, &bucket, sid)
                .await
                .map_err(|e| internal_err(e.to_string()))?;
            Ok::<_, QueryError>(slot.as_ref().as_ref().map(|spec| (sid, spec.clone())))
        }))
        .buffer_unordered(INDEX_PER_KEY_CONCURRENCY)
        .try_filter_map(|x| async move { Ok(x) })
        .try_collect()
        .await?;

    // Apply negative / empty-string matchers using the materialised specs.
    let needs_filter = selector_util::has_negative_matchers(selector)
        || selector_util::has_empty_string_matchers(selector);
    let filtered_ids: Vec<SeriesId> = if needs_filter {
        selector_util::apply_post_filters_map(&specs, candidates, selector)?
    } else {
        candidates
    };

    if filtered_ids.is_empty() {
        return Ok(None);
    }

    let bucket_id = encode_bucket(bucket);
    let mut labels_vec: Vec<Labels> = Vec::with_capacity(filtered_ids.len());
    let mut handles: Vec<ResolvedSeriesRef> = Vec::with_capacity(filtered_ids.len());
    for sid in &filtered_ids {
        let spec = specs.get(sid).ok_or_else(|| {
            internal_err(format!(
                "series {} missing from forward index in bucket {:?}",
                sid, bucket
            ))
        })?;
        let mut labs = spec.labels.clone();
        labs.sort();
        let metric_name: Arc<str> = labs
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| Arc::from(l.value.as_str()))
            .unwrap_or_else(|| Arc::from(""));
        labels_vec.push(Labels::new(labs));
        handles.push(ResolvedSeriesRef::new(bucket_id, *sid, metric_name));
    }

    Ok(Some(ResolvedSeriesChunk {
        bucket_id,
        labels: Arc::from(labels_vec),
        series: Arc::from(handles),
    }))
}

// ---------------------------------------------------------------------------
// samples()
// ---------------------------------------------------------------------------

/// One [`SampleBatch`] per contiguous same-bucket run of the caller's series
/// slice; series spanning buckets yield one batch per bucket. Operators merge.
fn samples_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    request: SamplesRequest,
) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
    stream::once(async move {
        match build_sample_batches(reader.as_ref(), &request).await {
            Ok(batches) => batches.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(e) => vec![Err(e)],
        }
    })
    .flat_map(stream::iter)
}

/// Order-preserving: runs are dispatched concurrently up to
/// [`SAMPLE_STAGE_READAHEAD`] but yielded via `buffered` so each batch's
/// `series_range` indexes into the caller's `request.series`.
async fn build_sample_batches<R: QueryReader + ?Sized>(
    reader: &R,
    request: &SamplesRequest,
) -> Result<Vec<SampleBatch>, QueryError> {
    if request.series.is_empty() || request.time_range.is_empty() {
        return Ok(Vec::new());
    }

    // Group contiguous same-bucket series into runs, preserving the
    // caller's order inside each run.
    let runs = contiguous_bucket_runs(&request.series);

    let time_range = request.time_range;
    let out: Vec<SampleBatch> = stream::iter(runs.into_iter().map(|run| {
        let series = request.series.clone();
        async move { build_batch_for_run(reader, run, series, time_range).await }
    }))
    .buffered(SAMPLE_STAGE_READAHEAD)
    .try_collect()
    .await?;

    Ok(out)
}

/// Per-series loop stays sequential — implicit spawn-per-series is prohibited.
/// Concurrency is the per-run dispatch one layer up. Metric names ride on the
/// `ResolvedSeriesRef`, populated by `resolve_one_bucket` — no forward-index
/// lookup here.
async fn build_batch_for_run<R: QueryReader + ?Sized>(
    reader: &R,
    run: BucketRun,
    series: Arc<[ResolvedSeriesRef]>,
    time_range: TimeRange,
) -> Result<SampleBatch, QueryError> {
    let bucket = decode_bucket(run.bucket_id)
        .ok_or_else(|| internal_err(format!("invalid bucket id: {}", run.bucket_id)))?;

    let series_count = run.range.end - run.range.start;
    let mut block = SampleBlock::with_series_count(series_count);

    // The source's time_range is inclusive-exclusive. The existing
    // QueryReader::samples contract is inclusive/inclusive with
    // `timestamp > start_ms && timestamp <= end_ms`
    // (see mock + MiniQueryReader). Translate by passing
    // `start_ms = time_range.start_ms - 1` and
    // `end_ms = time_range.end_ms_exclusive - 1`.
    let start_ms = time_range.start_ms.saturating_sub(1);
    let end_ms = time_range.end_ms_exclusive.saturating_sub(1);

    for (col_idx, series_ref) in series[run.range.clone()].iter().enumerate() {
        let sid = series_ref.series_id as SeriesId;
        let samples: Vec<Sample> = reader
            .samples(&bucket, sid, &series_ref.metric_name, start_ms, end_ms)
            .await
            .map_err(|e| internal_err(e.to_string()))?;

        let (ts_col, val_col) = (&mut block.timestamps[col_idx], &mut block.values[col_idx]);
        ts_col.reserve(samples.len());
        val_col.reserve(samples.len());
        // Preserve stale markers verbatim as STALE_NAN — the
        // storage layer encodes them as `f64::from_bits(STALE_NAN)`,
        // which survives the unmodified `s.value` copy below
        // (see `crate::model::is_stale_nan` and RFC 0007 source→caller
        // contract).
        for s in samples {
            ts_col.push(s.timestamp_ms);
            val_col.push(s.value);
        }
    }

    Ok(SampleBatch {
        series_range: run.range,
        samples: block,
    })
}

/// A same-bucket contiguous sub-slice of `request.series`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketRun {
    bucket_id: u64,
    range: Range<usize>,
}

/// Preserves caller ordering so each run's `series_range` indexes directly
/// into the caller's request.
fn contiguous_bucket_runs(series: &[ResolvedSeriesRef]) -> Vec<BucketRun> {
    if series.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut start = 0usize;
    let mut current_bucket = series[0].bucket_id;
    for (idx, sref) in series.iter().enumerate().skip(1) {
        if sref.bucket_id != current_bucket {
            runs.push(BucketRun {
                bucket_id: current_bucket,
                range: start..idx,
            });
            current_bucket = sref.bucket_id;
            start = idx;
        }
    }
    runs.push(BucketRun {
        bucket_id: current_bucket,
        range: start..series.len(),
    });
    runs
}

// ---------------------------------------------------------------------------
// QueryError bridging
// ---------------------------------------------------------------------------

/// Free helper (not `impl From`) so both `String` and `Error::to_string()`
/// flow through one path.
#[inline]
fn internal_err(msg: impl Into<String>) -> QueryError {
    QueryError::Internal(msg.into())
}

// ---------------------------------------------------------------------------
// selector_util — pure selector/matcher helpers reused across resolve /
// estimate. Equivalent to the logic in `promql::selector` (which ties to
// `CachedQueryReader` and cannot be used here); the two should stay in
// sync behaviourally.
// ---------------------------------------------------------------------------

pub(crate) mod selector_util {
    use super::{
        IndexCache, Label, QueryError, QueryReader, SeriesId, TimeBucket, VectorSelector,
        internal_err,
    };
    use crate::index::ForwardIndexLookup;
    use promql_parser::label::{METRIC_NAME, MatchOp};
    use regex_syntax::Parser;
    use regex_syntax::hir::{Hir, HirKind};
    use std::collections::HashSet;

    /// Parses `value1|value2|…` into literal alternatives.
    pub(super) fn parse_limited_regex(pattern: &str) -> Result<Vec<String>, String> {
        let hir = Parser::new()
            .parse(pattern)
            .map_err(|e| format!("invalid regex pattern '{}': {}", pattern, e))?;
        match hir.kind() {
            HirKind::Alternation(alts) => {
                let mut out = Vec::with_capacity(alts.len());
                for alt in alts {
                    out.push(parse_literal(alt, pattern)?);
                }
                Ok(out)
            }
            HirKind::Literal(_) | HirKind::Concat(_) => Ok(vec![parse_literal(&hir, pattern)?]),
            _ => Err(format!(
                "regex '{}' not supported (only literal alternations)",
                pattern
            )),
        }
    }

    fn parse_literal(hir: &Hir, pattern: &str) -> Result<String, String> {
        match hir.kind() {
            HirKind::Empty => Err(format!("empty alternative in pattern: {}", pattern)),
            HirKind::Literal(l) => {
                String::from_utf8(l.0.to_vec()).map_err(|_| "non-UTF-8 literal".to_string())
            }
            HirKind::Concat(hirs) => {
                let mut s = String::new();
                for h in hirs {
                    s.push_str(&parse_literal(h, pattern)?);
                }
                Ok(s)
            }
            _ => Err(format!(
                "regex '{}' not supported (only literal alternations)",
                pattern
            )),
        }
    }

    pub(crate) fn has_negative_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::NotEqual | MatchOp::NotRe(_)))
    }

    pub(crate) fn has_empty_string_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::Equal) && m.value.is_empty())
    }

    /// Mirrors `promql::selector::find_candidates_with_reader` but consults
    /// the reader directly (no `CachedQueryReader`).
    pub(crate) async fn find_candidates<R: QueryReader + ?Sized>(
        reader: &R,
        index_cache: &IndexCache,
        bucket: &TimeBucket,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut and_terms: Vec<Label> = Vec::new();
        let mut or_groups: Vec<Vec<Label>> = Vec::new();

        if let Some(name) = &selector.name {
            and_terms.push(Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            });
        }

        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::Equal if !m.value.is_empty() => and_terms.push(Label {
                    name: m.name.clone(),
                    value: m.value.clone(),
                }),
                MatchOp::Equal => {}
                MatchOp::Re(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    let or_terms: Vec<Label> = values
                        .into_iter()
                        .map(|v| Label {
                            name: m.name.clone(),
                            value: v,
                        })
                        .collect();
                    or_groups.push(or_terms);
                }
                _ => {}
            }
        }

        // No positive terms → either "empty string matcher only" (fall
        // back to metric-name scan) or "nothing to do".
        if and_terms.is_empty() && or_groups.is_empty() {
            if !has_empty_string_matchers(selector) {
                return Ok(Vec::new());
            }
            if let Some(name) = &selector.name {
                let metric_term = Label {
                    name: METRIC_NAME.to_string(),
                    value: name.clone(),
                };
                let inv = index_cache
                    .inverted_index(reader, bucket, std::slice::from_ref(&metric_term))
                    .await
                    .map_err(|e| internal_err(e.to_string()))?;
                let res: Vec<SeriesId> = inv.intersect(vec![metric_term]).iter().collect();
                return Ok(res);
            }
            return Err(internal_err(
                "must specify a metric name when using empty label matcher".to_string(),
            ));
        }

        let all_terms: Vec<Label> = or_groups
            .iter()
            .flat_map(|t| t.iter().cloned())
            .chain(and_terms.iter().cloned())
            .collect();
        let inv = index_cache
            .inverted_index(reader, bucket, &all_terms)
            .await
            .map_err(|e| internal_err(e.to_string()))?;

        let mut result_set: HashSet<SeriesId> = if !and_terms.is_empty() {
            inv.intersect(and_terms.clone()).iter().collect()
        } else {
            HashSet::new()
        };

        for or_terms in &or_groups {
            let mut or_result: HashSet<SeriesId> = HashSet::new();
            for term in or_terms {
                let per_term = inv.intersect(vec![term.clone()]);
                or_result.extend(per_term.iter());
            }
            if and_terms.is_empty() && result_set.is_empty() {
                result_set = or_result;
            } else {
                result_set = result_set.intersection(&or_result).cloned().collect();
            }
        }

        let mut v: Vec<SeriesId> = result_set.into_iter().collect();
        v.sort();
        Ok(v)
    }

    /// Matches the post-filter block in `promql::selector::evaluate_selector_with_reader`.
    pub(crate) fn apply_post_filters(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        if has_negative_matchers(selector) {
            out = apply_negative(forward, out, selector)?;
        }
        if has_empty_string_matchers(selector) {
            out = apply_empty_string(forward, out, selector);
        }
        Ok(out)
    }

    /// HashMap-backed variant of [`apply_post_filters`] for the resolve path.
    pub(super) fn apply_post_filters_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        if has_negative_matchers(selector) {
            out = apply_negative_map(specs, out, selector)?;
        }
        if has_empty_string_matchers(selector) {
            out = apply_empty_string_map(specs, out, selector);
        }
        Ok(out)
    }

    fn apply_negative_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::NotEqual => {
                    out.retain(|id| {
                        specs
                            .get(id)
                            .map(|spec| !has_label(&spec.labels, &m.name, &m.value))
                            .unwrap_or(false)
                    });
                }
                MatchOp::NotRe(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    out.retain(|id| {
                        specs
                            .get(id)
                            .map(|spec| !values.iter().any(|v| has_label(&spec.labels, &m.name, v)))
                            .unwrap_or(false)
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn apply_empty_string_map(
        specs: &std::collections::HashMap<SeriesId, crate::index::SeriesSpec>,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Vec<SeriesId> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            if matches!(m.op, MatchOp::Equal) && m.value.is_empty() {
                out.retain(|id| {
                    specs
                        .get(id)
                        .map(|spec| !has_label_with_non_empty_value(&spec.labels, &m.name))
                        .unwrap_or(false)
                });
            }
        }
        out
    }

    fn apply_negative(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Result<Vec<SeriesId>, QueryError> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            match &m.op {
                MatchOp::NotEqual => {
                    out.retain(|id| {
                        forward
                            .get_spec(id)
                            .map(|spec| !has_label(&spec.labels, &m.name, &m.value))
                            .unwrap_or(false)
                    });
                }
                MatchOp::NotRe(_) => {
                    let values = parse_limited_regex(&m.value).map_err(internal_err)?;
                    out.retain(|id| {
                        forward
                            .get_spec(id)
                            .map(|spec| !values.iter().any(|v| has_label(&spec.labels, &m.name, v)))
                            .unwrap_or(false)
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn apply_empty_string(
        forward: &dyn ForwardIndexLookup,
        candidates: Vec<SeriesId>,
        selector: &VectorSelector,
    ) -> Vec<SeriesId> {
        let mut out = candidates;
        for m in &selector.matchers.matchers {
            if matches!(m.op, MatchOp::Equal) && m.value.is_empty() {
                out.retain(|id| {
                    forward
                        .get_spec(id)
                        .map(|spec| !has_label_with_non_empty_value(&spec.labels, &m.name))
                        .unwrap_or(false)
                });
            }
        }
        out
    }

    fn has_label(labels: &[Label], name: &str, value: &str) -> bool {
        labels.iter().any(|l| l.name == name && l.value == value)
    }

    fn has_label_with_non_empty_value(labels: &[Label], name: &str) -> bool {
        labels.iter().any(|l| l.name == name && !l.value.is_empty())
    }
}

// ---------------------------------------------------------------------------
// Tests (pure helpers only; full storage-backed integration is 2.3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_bucket_round_trip() {
        // given: a TimeBucket with realistic start + size values
        let bucket = TimeBucket {
            start: 1_234_567u32,
            size: 3u8,
        };

        // when: round-trip through encode/decode
        let id = encode_bucket(bucket);
        let decoded = decode_bucket(id).expect("decode should succeed");

        // then: fields round-trip exactly
        assert_eq!(decoded.start, bucket.start);
        assert_eq!(decoded.size, bucket.size);
    }

    #[test]
    fn should_reject_decoded_bucket_with_zero_size() {
        // given: a bucket id with size = 0 (invalid)
        // when: decode it
        let decoded = decode_bucket(42u64 << 8);

        // then: decoding returns None
        assert!(decoded.is_none());
    }

    #[test]
    fn should_detect_bucket_overlap_with_time_range() {
        // given: a 1-hour bucket starting at minute 60 (→ [3_600_000, 7_200_000) ms)
        let bucket = TimeBucket {
            start: 60u32,
            size: 1u8,
        };
        let (b_start, b_end) = bucket_ms_window(bucket);
        assert_eq!(b_start, 3_600_000);
        assert_eq!(b_end, 7_200_000);

        // when / then: windows fully inside, overlapping, and disjoint
        assert!(bucket_overlaps(
            bucket,
            TimeRange::new(4_000_000, 5_000_000),
        ));
        assert!(bucket_overlaps(bucket, TimeRange::new(0, 4_000_000),));
        assert!(bucket_overlaps(
            bucket,
            TimeRange::new(6_000_000, 8_000_000),
        ));
        // touching at start: bucket_end == time.start_ms → no overlap
        assert!(!bucket_overlaps(
            bucket,
            TimeRange::new(7_200_000, 8_000_000),
        ));
        // touching at end: bucket_start == time.end_ms_exclusive → no overlap
        assert!(!bucket_overlaps(bucket, TimeRange::new(0, 3_600_000),));
        // empty range never overlaps
        assert!(!bucket_overlaps(
            bucket,
            TimeRange::new(4_000_000, 4_000_000),
        ));
    }

    #[test]
    fn should_group_contiguous_bucket_runs_preserving_order() {
        // given: a request-series slice with three buckets in mixed order
        let name: Arc<str> = Arc::from("m");
        let series = vec![
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket { start: 0, size: 1 }),
                1,
                name.clone(),
            ),
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket { start: 0, size: 1 }),
                2,
                name.clone(),
            ),
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket { start: 60, size: 1 }),
                7,
                name.clone(),
            ),
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket { start: 60, size: 1 }),
                8,
                name.clone(),
            ),
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket {
                    start: 120,
                    size: 1,
                }),
                5,
                name.clone(),
            ),
            // back to the first bucket — a new run, not merged.
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket { start: 0, size: 1 }),
                9,
                name.clone(),
            ),
        ];

        // when: partition into runs
        let runs = contiguous_bucket_runs(&series);

        // then: four runs, each a contiguous sub-range
        assert_eq!(runs.len(), 4);
        assert_eq!(runs[0].range, 0..2);
        assert_eq!(runs[1].range, 2..4);
        assert_eq!(runs[2].range, 4..5);
        assert_eq!(runs[3].range, 5..6);
    }

    #[test]
    fn should_return_empty_runs_for_empty_series_slice() {
        // given: no series
        let series: Vec<ResolvedSeriesRef> = Vec::new();

        // when: ask for runs
        let runs = contiguous_bucket_runs(&series);

        // then: no runs emitted
        assert!(runs.is_empty());
    }

    #[test]
    fn should_treat_whole_series_as_one_run_when_all_share_bucket() {
        // given: all series in the same bucket
        let bid = encode_bucket(TimeBucket { start: 0, size: 1 });
        let name: Arc<str> = Arc::from("m");
        let series = vec![
            ResolvedSeriesRef::new(bid, 1, name.clone()),
            ResolvedSeriesRef::new(bid, 2, name.clone()),
            ResolvedSeriesRef::new(bid, 3, name.clone()),
        ];

        // when: partition
        let runs = contiguous_bucket_runs(&series);

        // then: one run spanning the whole slice
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].range, 0..3);
        assert_eq!(runs[0].bucket_id, bid);
    }
}

// ---------------------------------------------------------------------------
// Integration tests
//
// These exercise the [`SeriesSource`] contract end-to-end against the
// existing in-memory [`MockQueryReader`] fixture (the same one that backs
// `promql/selector.rs` and `promql/pipeline.rs` tests). They verify:
//
//   - RFC §"Storage Contract" caller → source and source → caller
//     guarantees (series set, time window, ordering, stale markers).
//   - The inclusive-start / exclusive-end translation over `QueryReader`'s
//     `(start, end]` contract.
//   - Cross-bucket stitching (per-bucket batches in chronological order).
//   - `selector_util` matcher parity with the shapes covered in
//     `promql/selector.rs`: metric-only, equality, negation, regex OR,
//     empty-string, AND-combination.
//
// ---------------------------------------------------------------------------

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::model::{Label, MetricType, STALE_NAN, Sample, TimeBucket};
    use crate::query::test_utils::{MockMultiBucketQueryReaderBuilder, MockQueryReader};
    use futures::StreamExt;
    use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
    use promql_parser::parser::VectorSelector;
    use regex::Regex;

    // ---------- Fixture helpers -----------------------------------------

    /// Build an empty `Matchers`. Mirrors `empty_matchers()` in
    /// `promql/selector.rs` tests.
    fn empty_matchers() -> Matchers {
        Matchers {
            matchers: vec![],
            or_matchers: vec![],
        }
    }

    /// Build a bare metric-name-only selector.
    fn sel_metric(name: &str) -> VectorSelector {
        VectorSelector {
            name: Some(name.to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        }
    }

    /// Build a selector with a metric name plus one or more matchers.
    fn sel_with(name: &str, matchers: Vec<Matcher>) -> VectorSelector {
        VectorSelector {
            name: Some(name.to_string()),
            matchers: Matchers {
                matchers,
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        }
    }

    /// Build a `Matcher::Re(..)` pair for a `label=~pattern` matcher.
    fn re_matcher(name: &str, pattern: &str) -> Matcher {
        let regex = Regex::new(pattern).expect("valid regex");
        Matcher {
            op: MatchOp::Re(regex),
            name: name.to_string(),
            value: pattern.to_string(),
        }
    }

    /// Labels that identify one test series: `{__name__, ...extra}`.
    fn labels(name: &str, extra: &[(&str, &str)]) -> Vec<Label> {
        let mut out = vec![Label {
            name: METRIC_NAME.to_string(),
            value: name.to_string(),
        }];
        for (k, v) in extra {
            out.push(Label {
                name: (*k).to_string(),
                value: (*v).to_string(),
            });
        }
        out
    }

    /// One-bucket, single-series builder. Convenience for simple boundary
    /// / staleness tests.
    fn one_series_reader(
        bucket: TimeBucket,
        name: &str,
        samples: &[(i64, f64)],
    ) -> MockQueryReader {
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for &(ts, v) in samples {
            b.add_sample(
                bucket,
                labels(name, &[]),
                MetricType::Gauge,
                Sample::new(ts, v),
            );
        }
        b.build()
    }

    /// Drain a `Stream<Item=Result<T, QueryError>>` into `Vec<T>`,
    /// unwrapping errors inline (tests panic with the adapter's message).
    /// The stream is pinned on the heap inside so callers don't have to
    /// care about `Unpin` — the adapter's RPITIT-returned streams aren't.
    async fn collect_ok<S, T>(stream: S) -> Vec<T>
    where
        S: futures::Stream<Item = Result<T, QueryError>>,
    {
        let mut stream = Box::pin(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.expect("source adapter returned error"));
        }
        out
    }

    /// Build a `QueryReaderSource` from a concrete `MockQueryReader`.
    fn source(reader: MockQueryReader) -> QueryReaderSource<MockQueryReader> {
        QueryReaderSource::new(Arc::new(reader))
    }

    // Bucket `h0` covers [0, 3_600_000) ms. `h60` covers [3_600_000,
    // 7_200_000). `h120` covers [7_200_000, 10_800_000).
    fn h0() -> TimeBucket {
        TimeBucket::hour(0)
    }
    fn h60() -> TimeBucket {
        TimeBucket::hour(60)
    }
    fn h120() -> TimeBucket {
        TimeBucket::hour(120)
    }

    // ---------- RFC source → caller contract ----------------------------

    #[tokio::test]
    async fn should_return_series_range_covering_contiguous_slice_of_input_series() {
        // given: a single-bucket reader with three series under one metric name
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for (env, ts, v) in [("a", 1_000i64, 1.0), ("b", 1_000, 2.0), ("c", 1_000, 3.0)] {
            b.add_sample(
                h0(),
                labels("m", &[("env", env)]),
                MetricType::Gauge,
                Sample::new(ts, v),
            );
        }
        let src = source(b.build());

        // when: resolve then ask for all three series' samples
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        assert_eq!(chunks.len(), 1);
        let series: Arc<[ResolvedSeriesRef]> = chunks[0].series.clone();
        let batches = collect_ok(src.samples(SamplesRequest::new(
            series.clone(),
            TimeRange::new(0, 3_600_000),
        )))
        .await;

        // then: each batch's series_range is a contiguous slice of the request
        assert!(!batches.is_empty());
        let total: usize = batches.iter().map(|b| b.series_range.len()).sum();
        assert_eq!(total, series.len());
        for batch in &batches {
            assert!(batch.series_range.end <= series.len());
            assert!(batch.series_range.start < batch.series_range.end);
            assert_eq!(batch.samples.series_count(), batch.series_range.len());
        }
    }

    #[tokio::test]
    async fn should_return_samples_in_timestamp_order_per_series() {
        // given: one series ingested in timestamp order (production
        // `QueryReader::samples` always returns sorted; the adapter's
        // job is to forward that without reordering, which is what this
        // test verifies — ingest order == expected output order).
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        let ingested: Vec<(i64, f64)> = vec![(500, 1.0), (1_000, 2.0), (1_500, 3.0), (2_000, 4.0)];
        for (ts, v) in &ingested {
            b.add_sample(
                h0(),
                labels("m", &[]),
                MetricType::Gauge,
                Sample::new(*ts, *v),
            );
        }
        let src = source(b.build());

        // when: resolve and fetch samples
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: per-series timestamps are monotone non-decreasing, in
        // the order the backing store produced them — the adapter did
        // not reorder or de-duplicate.
        assert_eq!(batches.len(), 1);
        let ts_col = &batches[0].samples.timestamps[0];
        let val_col = &batches[0].samples.values[0];
        for w in ts_col.windows(2) {
            assert!(w[0] <= w[1], "timestamps not monotone: {:?}", ts_col);
        }
        // Ordering is preserved end-to-end: the adapter's output column
        // matches the (already-sorted) ingest sequence.
        let expected_ts: Vec<i64> = ingested.iter().map(|(t, _)| *t).collect();
        let expected_val: Vec<f64> = ingested.iter().map(|(_, v)| *v).collect();
        assert_eq!(ts_col, &expected_ts);
        assert_eq!(val_col, &expected_val);
    }

    #[tokio::test]
    async fn should_preserve_stale_nan_markers() {
        // given: a series with a real value then a stale marker
        let stale = f64::from_bits(STALE_NAN);
        let reader = one_series_reader(h0(), "m", &[(1_000, 42.0), (2_000, stale)]);
        let src = source(reader);

        // when: resolve + fetch the full bucket window
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: the stale bit pattern round-trips verbatim, not normalised
        // to NaN or dropped
        let values = &batches[0].samples.values[0];
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], 42.0);
        assert_eq!(
            values[1].to_bits(),
            STALE_NAN,
            "stale marker must round-trip bit-exact"
        );
        assert!(crate::model::is_stale_nan(values[1]));
    }

    // ---------- RFC caller → source contract ----------------------------

    #[tokio::test]
    async fn should_not_widen_series_set_beyond_selector_match() {
        // given: a reader with two metrics; only one matches the selector
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m1", &[]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("m2", &[]),
            MetricType::Gauge,
            Sample::new(1_000, 2.0),
        );
        let src = source(b.build());

        // when: resolve `m1`
        let chunks = collect_ok(src.resolve(&sel_metric("m1"), TimeRange::new(0, 3_600_000))).await;

        // then: exactly one series returned; `m2` must not leak in
        let total: usize = chunks.iter().map(|c| c.series.len()).sum();
        assert_eq!(total, 1);
        for chunk in &chunks {
            for lab in chunk.labels.iter() {
                assert_eq!(lab.metric_name(), "m1");
            }
        }
    }

    #[tokio::test]
    async fn should_respect_caller_time_range() {
        // given: samples at 500, 1_500, 2_500
        let reader = one_series_reader(h0(), "m", &[(500, 1.0), (1_500, 2.0), (2_500, 3.0)]);
        let src = source(reader);

        // when: query window [1_000, 2_000)
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: only the sample at 1_500 survives — 500 is before the
        // window and 2_500 is after
        let ts = &batches[0].samples.timestamps[0];
        assert_eq!(ts, &vec![1_500]);
    }

    // ---------- Boundary / edge cases ---------------

    #[tokio::test]
    async fn should_include_sample_at_start_ms_boundary() {
        // given: a single sample at exactly time_range.start_ms
        let reader = one_series_reader(h0(), "m", &[(1_000, 7.0)]);
        let src = source(reader);

        // when: request [1_000, 2_000)
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: sample at 1_000 is included (inclusive start)
        let ts = &batches[0].samples.timestamps[0];
        assert_eq!(ts, &vec![1_000]);
    }

    #[tokio::test]
    async fn should_exclude_sample_at_end_ms_exclusive_boundary() {
        // given: a single sample at exactly time_range.end_ms_exclusive
        let reader = one_series_reader(h0(), "m", &[(2_000, 7.0)]);
        let src = source(reader);

        // when: request [1_000, 2_000) — note exclusive end
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: the sample is NOT included
        let ts = &batches[0].samples.timestamps[0];
        assert!(ts.is_empty(), "sample at end_ms_exclusive leaked: {:?}", ts);
    }

    #[tokio::test]
    async fn should_include_sample_just_before_end_ms_exclusive() {
        // given: a single sample at end_ms_exclusive - 1 (the last
        // included timestamp)
        let reader = one_series_reader(h0(), "m", &[(1_999, 7.0)]);
        let src = source(reader);

        // when: request [1_000, 2_000)
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: sanity pair with the excluded-end test above — 1_999 IS
        // included
        let ts = &batches[0].samples.timestamps[0];
        assert_eq!(ts, &vec![1_999]);
    }

    #[tokio::test]
    async fn should_emit_empty_stream_for_empty_time_range() {
        // given: a reader with real samples
        let reader = one_series_reader(h0(), "m", &[(1_000, 1.0), (2_000, 2.0)]);
        let src = source(reader);

        // resolve the series first (over a non-empty window)
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let series = chunks[0].series.clone();

        // when: ask for samples over an empty window [1_500, 1_500)
        let request = SamplesRequest::new(series, TimeRange::new(1_500, 1_500));
        let batches = collect_ok(src.samples(request)).await;

        // then: the stream terminates immediately with no batches
        assert!(
            batches.is_empty(),
            "expected empty stream, got {} batch(es)",
            batches.len()
        );
    }

    // ---------- Cross-bucket stitching ---------------------------------

    #[tokio::test]
    async fn should_emit_one_batch_per_bucket_for_multi_bucket_series() {
        // given: the *same* label set ingested into two buckets
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h60(),
            labels("m", &[]),
            MetricType::Gauge,
            Sample::new(3_700_000, 2.0),
        );
        let src = source(b.build());

        // when: resolve (two chunks — one per bucket, chronological) and
        // concatenate their handles before requesting samples
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 7_200_000))).await;
        assert_eq!(chunks.len(), 2);
        // chronological: h0 (start=0) before h60 (start=60)
        let h0_id = encode_bucket(h0());
        let h60_id = encode_bucket(h60());
        assert_eq!(chunks[0].bucket_id, h0_id);
        assert_eq!(chunks[1].bucket_id, h60_id);

        let mut merged: Vec<ResolvedSeriesRef> = Vec::new();
        for c in &chunks {
            merged.extend(c.series.iter().cloned());
        }
        let request = SamplesRequest::new(Arc::from(merged), TimeRange::new(0, 7_200_000));
        let batches = collect_ok(src.samples(request)).await;

        // then: one batch per bucket in the order the caller supplied the
        // runs (which was chronological after our merge)
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].samples.timestamps[0], vec![1_000]);
        assert_eq!(batches[1].samples.timestamps[0], vec![3_700_000]);
    }

    #[tokio::test]
    async fn should_skip_buckets_that_do_not_overlap_time_range() {
        // given: series in h0, h60, h120. Query window covers only h0+h60.
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for (bucket, ts) in [(h0(), 1_000i64), (h60(), 3_700_000), (h120(), 7_300_000)] {
            b.add_sample(
                bucket,
                labels("m", &[]),
                MetricType::Gauge,
                Sample::new(ts, 1.0),
            );
        }
        let src = source(b.build());

        // when: resolve over [0, 7_200_000) — h120 is entirely outside
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 7_200_000))).await;

        // then: exactly the two overlapping buckets emit chunks; h120 is
        // not represented
        let ids: Vec<u64> = chunks.iter().map(|c| c.bucket_id).collect();
        assert!(ids.contains(&encode_bucket(h0())));
        assert!(ids.contains(&encode_bucket(h60())));
        assert!(
            !ids.contains(&encode_bucket(h120())),
            "h120 bucket leaked into resolve: {:?}",
            ids
        );
        assert_eq!(chunks.len(), 2);
    }

    // ---------- Selector-matcher parity --------------------------------
    //
    // `selector_util` is validated behaviourally against hand-built
    // fixtures rather than by calling into `promql::selector`,
    // whose `CachedQueryReader`-coupled API is private to the
    // evaluator). The existing selector.rs tests already cover
    // `evaluate_selector_with_reader` over the same matcher shapes, so a
    // failure here alongside a green `promql::selector` test strongly
    // implies a `selector_util` drift.

    #[tokio::test]
    async fn should_match_metric_name_only() {
        // given: two series under the same metric name plus one under a
        // different name
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[("env", "a")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("m", &[("env", "b")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("other", &[]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: resolve `m`
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;

        // then: two series returned, both `m`
        let total: usize = chunks.iter().map(|c| c.series.len()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn should_match_label_equality() {
        // given: three series, one matches method=GET
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[("method", "GET")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("m", &[("method", "POST")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("m", &[("method", "DELETE")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: resolve m{method="GET"}
        let sel = sel_with("m", vec![Matcher::new(MatchOp::Equal, "method", "GET")]);
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: exactly one match
        let mut got_methods: Vec<String> = Vec::new();
        for c in &chunks {
            for lab in c.labels.iter() {
                if let Some(v) = lab.get("method") {
                    got_methods.push(v.to_string());
                }
            }
        }
        assert_eq!(got_methods, vec!["GET".to_string()]);
    }

    #[tokio::test]
    async fn should_match_label_negation() {
        // given: three series with method labels
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for method in ["GET", "POST", "DELETE"] {
            b.add_sample(
                h0(),
                labels("m", &[("method", method)]),
                MetricType::Gauge,
                Sample::new(1_000, 1.0),
            );
        }
        let src = source(b.build());

        // when: resolve m{method!="GET"}
        let sel = sel_with("m", vec![Matcher::new(MatchOp::NotEqual, "method", "GET")]);
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: POST + DELETE match, GET is excluded
        let mut methods: Vec<String> = Vec::new();
        for c in &chunks {
            for lab in c.labels.iter() {
                if let Some(v) = lab.get("method") {
                    methods.push(v.to_string());
                }
            }
        }
        methods.sort();
        assert_eq!(methods, vec!["DELETE".to_string(), "POST".to_string()]);
    }

    #[tokio::test]
    async fn should_match_label_regex() {
        // given: three series with env labels
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for env in ["prod-a", "prod-b", "staging-a"] {
            b.add_sample(
                h0(),
                labels("m", &[("env", env)]),
                MetricType::Gauge,
                Sample::new(1_000, 1.0),
            );
        }
        let src = source(b.build());

        // when: resolve m{env=~"prod-a|prod-b"}
        let sel = sel_with("m", vec![re_matcher("env", "prod-a|prod-b")]);
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: both prod-* series match, staging-a does not
        let mut envs: Vec<String> = Vec::new();
        for c in &chunks {
            for lab in c.labels.iter() {
                if let Some(v) = lab.get("env") {
                    envs.push(v.to_string());
                }
            }
        }
        envs.sort();
        assert_eq!(envs, vec!["prod-a".to_string(), "prod-b".to_string()]);
    }

    #[tokio::test]
    async fn should_match_empty_string_matcher() {
        // given: two series — one with a label `foo`, one without
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[("foo", "bar")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        b.add_sample(
            h0(),
            labels("m", &[]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: resolve m{foo=""}
        let sel = sel_with("m", vec![Matcher::new(MatchOp::Equal, "foo", "")]);
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: only the series without `foo` matches (`{foo=""}` matches
        // absent)
        let total: usize = chunks.iter().map(|c| c.series.len()).sum();
        assert_eq!(total, 1);
        for c in &chunks {
            for lab in c.labels.iter() {
                assert!(lab.get("foo").is_none());
            }
        }
    }

    #[tokio::test]
    async fn should_match_combined_and_of_matchers() {
        // given: four series with (env, method) combinations
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for (env, method) in [
            ("prod", "GET"),
            ("prod", "POST"),
            ("staging", "GET"),
            ("staging", "POST"),
        ] {
            b.add_sample(
                h0(),
                labels("m", &[("env", env), ("method", method)]),
                MetricType::Gauge,
                Sample::new(1_000, 1.0),
            );
        }
        let src = source(b.build());

        // when: resolve m{env="prod", method="GET"}
        let sel = sel_with(
            "m",
            vec![
                Matcher::new(MatchOp::Equal, "env", "prod"),
                Matcher::new(MatchOp::Equal, "method", "GET"),
            ],
        );
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: exactly one match (intersection)
        let total: usize = chunks.iter().map(|c| c.series.len()).sum();
        assert_eq!(total, 1);
        for c in &chunks {
            for lab in c.labels.iter() {
                assert_eq!(lab.get("env"), Some("prod"));
                assert_eq!(lab.get("method"), Some("GET"));
            }
        }
    }

    #[tokio::test]
    async fn should_match_regex_or_group() {
        // given: three instances
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for inst in ["host-38", "host-39", "host-40"] {
            b.add_sample(
                h0(),
                labels("m", &[("instance", inst)]),
                MetricType::Gauge,
                Sample::new(1_000, 1.0),
            );
        }
        b.add_sample(
            h0(),
            labels("m", &[("instance", "host-99")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: resolve m{instance=~"host-38|host-39|host-40"}
        let sel = sel_with("m", vec![re_matcher("instance", "host-38|host-39|host-40")]);
        let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

        // then: three matches, host-99 excluded
        let mut insts: Vec<String> = Vec::new();
        for c in &chunks {
            for lab in c.labels.iter() {
                if let Some(v) = lab.get("instance") {
                    insts.push(v.to_string());
                }
            }
        }
        insts.sort();
        assert_eq!(
            insts,
            vec![
                "host-38".to_string(),
                "host-39".to_string(),
                "host-40".to_string(),
            ]
        );
    }
}
