//! Adapter from the existing per-bucket [`QueryReader`] surface to the v2
//! [`SeriesSource`] contract.
//!
//! Wraps an `Arc<R: QueryReader>` and fans out per-bucket selector
//! resolution / sample streaming. Cross-bucket stitching happens inside
//! this source — callers see a single stream of metadata chunks and a
//! single stream of sample batches, not per-bucket state.
//!
//! Decisions (see §5 Decisions Log entries for 2.2):
//! - Batch emission strategy: **per-bucket batches**. A series that lives
//!   in several buckets yields one [`SampleBatch`] per bucket (in bucket
//!   timestamp order). Operators perform the cross-bucket merge — same
//!   split as the existing `BucketSampleData → shape_matrix_results`
//!   pipeline.
//! - Cardinality estimate: per-bucket **upper bound from the inverted
//!   index**. Intersects only the positive (AND) equality terms of the
//!   selector; negative / empty-string / regex matchers are ignored so
//!   the result is always `≥` the true post-resolve count. Flagged
//!   `approx: true` whenever the selector carries anything beyond plain
//!   equality. When the selector has no positive terms the adapter
//!   returns `(0, u64::MAX)` and defers the real match to `resolve`.
//! - Selector matcher logic is re-implemented in [`selector_util`] rather
//!   than pulled from `promql::selector` (which ties to the evaluator's
//!   `CachedQueryReader`). Matches the source file's positive/OR/negative
//!   handling 1:1; the RFC mandates leaving `selector.rs` untouched.
//!
//! The adapter is gated behind the `promql-v2` feature through its parent
//! module.

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use futures::stream::{self, StreamExt};
use promql_parser::parser::VectorSelector;

use crate::model::{Label, Labels, Sample, SeriesId, TimeBucket};
use crate::query::QueryReader;

use super::memory::QueryError;
use super::source::{
    CardinalityEstimate, ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock,
    SampleHint, SeriesSource, TimeRange,
};

// ---------------------------------------------------------------------------
// Bucket-id encoding
// ---------------------------------------------------------------------------

/// Encode a [`TimeBucket`] into the opaque `u64` carried by
/// [`ResolvedSeriesRef::bucket_id`].
///
/// Shape: `(start as u64) << 8 | size as u64`. `size` is a `u8`
/// (exponent, 1..=15), `start` is a `u32` (minutes-since-epoch). A `u64`
/// is enough for both pieces with 24 spare high bits.
#[inline]
fn encode_bucket(bucket: TimeBucket) -> u64 {
    ((bucket.start as u64) << 8) | (bucket.size as u64)
}

/// Decode a bucket id back into a [`TimeBucket`]. Inverse of
/// [`encode_bucket`]. Returns `None` when the encoded `size` is out of
/// range — defensive; the adapter only mints ids via `encode_bucket`, so
/// this is a cheap sanity guard rather than a real fallible conversion.
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

/// Absolute millisecond window covered by a bucket: `[start_ms, end_ms)`.
#[inline]
fn bucket_ms_window(bucket: TimeBucket) -> (i64, i64) {
    let start_ms = (bucket.start as i64) * 60 * 1000;
    let end_ms = start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
    (start_ms, end_ms)
}

/// True iff `bucket`'s window overlaps the caller-supplied
/// `[time_range)` window. Equivalent to the retain clause in
/// `QueryPlan::for_matrix` (pipeline.rs:186-190).
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

/// [`SeriesSource`] implementation over the crate-internal
/// [`QueryReader`].
///
/// The adapter is parameterised by a generic `R: QueryReader` and holds
/// it behind an [`Arc`] so its streams can own a handle independent of
/// the caller's lifetime. A single [`QueryReaderSource`] instance
/// services one query; nothing in the adapter maintains per-query scratch
/// state between calls (the planner owns scratch).
///
/// The adapter is **stateless**: `resolve` emits all metadata needed by
/// the caller, and `samples` re-consults the forward index on-demand to
/// map `(bucket, series_id)` back to a metric name (the existing
/// `QueryReader::samples` method needs it as an argument). The cost is
/// one extra forward-index lookup per bucket touched by `samples`, which
/// is cached by the reader itself; the benefit is no interior mutability.
pub(crate) struct QueryReaderSource<R: QueryReader> {
    reader: Arc<R>,
}

impl<R: QueryReader> QueryReaderSource<R> {
    /// Build an adapter around an `Arc<R>`.
    pub(crate) fn new(reader: Arc<R>) -> Self {
        Self { reader }
    }
}

impl<R: QueryReader + 'static> SeriesSource for QueryReaderSource<R> {
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
        let reader = self.reader.clone();
        let selector = selector.clone();
        resolve_stream(reader, selector, time_range)
    }

    fn estimate_cardinality(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Future<Output = Result<CardinalityEstimate, QueryError>> + Send {
        let reader = self.reader.clone();
        let selector = selector.clone();
        estimate_cardinality_inner(reader, selector, time_range)
    }

    fn samples(
        &self,
        hint: SampleHint,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
        let reader = self.reader.clone();
        samples_stream(reader, hint)
    }
}

// ---------------------------------------------------------------------------
// resolve()
// ---------------------------------------------------------------------------

/// Fan out selector resolution across overlapping buckets and emit one
/// [`ResolvedSeriesChunk`] per non-empty bucket. Buckets are polled
/// serially here — matches the "no implicit spawn-per-series" rule
/// (RFC §"Concurrency Model"); `Concurrent` / `Coalesce` at the operator
/// layer can parallelise across buckets when the planner chooses.
fn resolve_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    async_stream_resolve(reader, selector, time_range)
}

/// `async fn`-returning-`impl Stream` via `stream::unfold`. One yielded
/// item per *non-empty* bucket. Empty buckets are silently skipped so
/// the caller does not need to look at empty chunks (callers of the
/// 2.1 trait treat `series: Arc<[]>` as "no match" already, but
/// skipping keeps the stream tidy).
fn async_stream_resolve<R: QueryReader + 'static>(
    reader: Arc<R>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
    stream::once(async move {
        let buckets = match reader.list_buckets().await {
            Ok(bs) => bs,
            Err(e) => return vec![Err(internal_err(e.to_string()))],
        };

        let mut out: Vec<Result<ResolvedSeriesChunk, QueryError>> = Vec::new();
        let mut filtered: Vec<TimeBucket> = buckets
            .into_iter()
            .filter(|b| bucket_overlaps(*b, time_range))
            .collect();
        // Deterministic emission order so tests and downstream planner
        // see stable ordering across calls; chronological (oldest first).
        filtered.sort_by_key(|b| b.start);

        for bucket in filtered {
            match resolve_one_bucket(reader.as_ref(), bucket, &selector).await {
                Ok(Some(chunk)) => out.push(Ok(chunk)),
                Ok(None) => {}
                Err(e) => {
                    out.push(Err(e));
                    break;
                }
            }
        }
        out
    })
    .flat_map(stream::iter)
}

/// Resolve the selector in a single bucket. Returns `Ok(None)` when no
/// series match (the caller suppresses the empty chunk).
async fn resolve_one_bucket<R: QueryReader + ?Sized>(
    reader: &R,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> Result<Option<ResolvedSeriesChunk>, QueryError> {
    let candidates = selector_util::find_candidates(reader, &bucket, selector).await?;
    if candidates.is_empty() {
        return Ok(None);
    }

    // Apply negative / empty-string matchers (require forward index).
    let needs_filter = selector_util::has_negative_matchers(selector)
        || selector_util::has_empty_string_matchers(selector);

    let forward = reader
        .forward_index(&bucket, &candidates)
        .await
        .map_err(|e| internal_err(e.to_string()))?;

    let filtered_ids: Vec<SeriesId> = if needs_filter {
        selector_util::apply_post_filters(forward.as_ref(), candidates, selector)?
    } else {
        candidates
    };

    if filtered_ids.is_empty() {
        return Ok(None);
    }

    // Now translate each series_id → Labels via the forward index.
    let bucket_id = encode_bucket(bucket);
    let mut labels_vec: Vec<Labels> = Vec::with_capacity(filtered_ids.len());
    let mut handles: Vec<ResolvedSeriesRef> = Vec::with_capacity(filtered_ids.len());
    for sid in &filtered_ids {
        let spec = forward.get_spec(sid).ok_or_else(|| {
            internal_err(format!(
                "series {} missing from forward index in bucket {:?}",
                sid, bucket
            ))
        })?;
        let mut labs = spec.labels.clone();
        labs.sort();
        labels_vec.push(Labels::new(labs));
        handles.push(ResolvedSeriesRef::new(bucket_id, *sid));
    }

    Ok(Some(ResolvedSeriesChunk {
        bucket_id,
        labels: Arc::from(labels_vec),
        series: Arc::from(handles),
    }))
}

// ---------------------------------------------------------------------------
// estimate_cardinality()
// ---------------------------------------------------------------------------

/// Sum a cheap upper bound across overlapping buckets.
///
/// Strategy: for each bucket, intersect the selector's positive equality
/// terms (metric name + `Label=value` matchers) on the inverted index
/// and take the resulting bitmap's cardinality. This does **not** apply
/// regex / negative / empty-string filters — the result is an upper
/// bound. Flagged `approx: true` whenever the selector uses anything
/// beyond plain equality (in which case the resolver may shrink the set),
/// or when the selector has no positive terms at all (in which case we
/// cannot even guess a tight bound without resolving).
async fn estimate_cardinality_inner<R: QueryReader + 'static>(
    reader: Arc<R>,
    selector: VectorSelector,
    time_range: TimeRange,
) -> Result<CardinalityEstimate, QueryError> {
    let positive_terms = selector_util::positive_equality_terms(&selector);
    let approx = selector_util::has_non_equality_matchers(&selector);

    if positive_terms.is_empty() {
        // Cannot derive a cheap bound — planner should treat this as
        // "unknown upper, must resolve". See §5 Decisions Log 2.2.
        return Ok(CardinalityEstimate {
            series_lower_bound: 0,
            series_upper_bound: u64::MAX,
            approx: true,
        });
    }

    let buckets = reader
        .list_buckets()
        .await
        .map_err(|e| internal_err(e.to_string()))?;

    let mut total: u64 = 0;
    for bucket in buckets
        .into_iter()
        .filter(|b| bucket_overlaps(*b, time_range))
    {
        let inverted = reader
            .inverted_index(&bucket, &positive_terms)
            .await
            .map_err(|e| internal_err(e.to_string()))?;
        let count = inverted.intersect(positive_terms.clone()).len();
        total = total.saturating_add(count);
    }

    // Lower bound: 0 when we may have overcounted with regex/negative
    // filters, else the sum itself (plain-equality selector already
    // matches what resolve() would return up to bucket dedup).
    let lower = if approx { 0 } else { total };

    Ok(CardinalityEstimate {
        series_lower_bound: lower,
        series_upper_bound: total,
        approx,
    })
}

// ---------------------------------------------------------------------------
// samples()
// ---------------------------------------------------------------------------

/// Stream [`SampleBatch`]es for the post-resolution series set.
///
/// Batch emission strategy: **per-bucket**. The hint's `series` slice is
/// grouped by bucket (preserving the caller's series ordering), and one
/// batch is emitted per contiguous same-bucket run. Series that span
/// multiple buckets therefore produce one batch per bucket — operators
/// own cross-bucket merging (same split as `shape_matrix_results`).
fn samples_stream<R: QueryReader + 'static>(
    reader: Arc<R>,
    hint: SampleHint,
) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
    stream::once(async move {
        match build_sample_batches(reader.as_ref(), &hint).await {
            Ok(batches) => batches.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(e) => vec![Err(e)],
        }
    })
    .flat_map(stream::iter)
}

/// Build the full list of per-bucket [`SampleBatch`]es for a hint.
///
/// Broken out as an async fn (not a generator) so it's straightforward to
/// unit-test future extensions. The function:
/// 1. Walks the hint's series slice to identify contiguous same-bucket
///    runs — each run becomes one batch covering a
///    `series_range` into `hint.series`.
/// 2. For each bucket that appears in a run, loads the forward index
///    once to resolve `series_id → metric_name`.
/// 3. Calls `QueryReader::samples` per series with the hint's time
///    range and pushes the columnar data into the batch's `SampleBlock`.
async fn build_sample_batches<R: QueryReader + ?Sized>(
    reader: &R,
    hint: &SampleHint,
) -> Result<Vec<SampleBatch>, QueryError> {
    if hint.series.is_empty() || hint.time_range.is_empty() {
        return Ok(Vec::new());
    }

    // Group contiguous same-bucket series into runs, preserving the
    // caller's order inside each run.
    let runs = contiguous_bucket_runs(&hint.series);

    // Pre-load forward index per distinct bucket (for metric_name).
    let mut per_bucket_forward: HashMap<
        u64,
        Arc<dyn crate::index::ForwardIndexLookup + Send + Sync>,
    > = HashMap::new();
    for run in &runs {
        if per_bucket_forward.contains_key(&run.bucket_id) {
            continue;
        }
        let bucket = decode_bucket(run.bucket_id)
            .ok_or_else(|| internal_err(format!("invalid bucket id: {}", run.bucket_id)))?;
        // Collect unique SeriesIds touched inside this bucket across
        // all runs (typically one run per bucket, but be correct).
        let mut ids: Vec<SeriesId> = hint
            .series
            .iter()
            .filter(|r| r.bucket_id == run.bucket_id)
            .map(|r| r.series_id as SeriesId)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let forward = reader
            .forward_index(&bucket, &ids)
            .await
            .map_err(|e| internal_err(e.to_string()))?;
        // `forward_index` returns `Box<dyn ... + 'static>`. Upgrade to
        // `Arc<dyn ...>` so multiple runs for the same bucket share one
        // lookup without re-loading. Box → Arc conversion is free.
        per_bucket_forward.insert(run.bucket_id, Arc::from(forward));
    }

    let mut out = Vec::with_capacity(runs.len());
    for run in runs {
        let bucket = decode_bucket(run.bucket_id)
            .ok_or_else(|| internal_err(format!("invalid bucket id: {}", run.bucket_id)))?;
        let forward = per_bucket_forward
            .get(&run.bucket_id)
            .expect("forward index pre-loaded above");

        let series_count = run.range.end - run.range.start;
        let mut block = SampleBlock::with_series_count(series_count);

        // The source's time_range is inclusive-exclusive. The existing
        // QueryReader::samples contract is inclusive/inclusive with
        // `timestamp > start_ms && timestamp <= end_ms`
        // (see mock + MiniQueryReader). Translate by passing
        // `start_ms = time_range.start_ms - 1` and
        // `end_ms = time_range.end_ms_exclusive - 1`. See §5 Decisions
        // Log 2.2 for this quirk.
        let start_ms = hint.time_range.start_ms.saturating_sub(1);
        let end_ms = hint.time_range.end_ms_exclusive.saturating_sub(1);

        for (col_idx, series_ref) in hint.series[run.range.clone()].iter().enumerate() {
            let sid = series_ref.series_id as SeriesId;
            let spec = forward.get_spec(&sid).ok_or_else(|| {
                internal_err(format!(
                    "series {} missing from forward index in bucket {:?}",
                    sid, bucket
                ))
            })?;
            let metric_name = spec
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.as_str())
                .unwrap_or("");
            let samples: Vec<Sample> = reader
                .samples(&bucket, sid, metric_name, start_ms, end_ms)
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

        out.push(SampleBatch {
            series_range: run.range,
            samples: block,
        });
    }

    Ok(out)
}

/// A contiguous sub-slice of `hint.series` whose entries all share a
/// single bucket. Produced by [`contiguous_bucket_runs`] so that
/// `series_range` in each emitted batch is a single `Range<usize>` that
/// indexes directly into the caller's hint.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketRun {
    bucket_id: u64,
    range: Range<usize>,
}

/// Partition `series` into contiguous runs with the same `bucket_id`.
///
/// Preserves the caller's ordering — crucial for the `SampleBatch`
/// contract that `series_range` indexes into the caller-supplied hint.
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

/// Wrap a crate-level error message into the v2 [`QueryError::Internal`]
/// variant. Kept as a free helper (not an `impl From`) so the adapter can
/// feed both `String` and `Error::to_string()` through the same path.
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

mod selector_util {
    use super::{
        Label, QueryError, QueryReader, SeriesId, TimeBucket, VectorSelector, internal_err,
    };
    use crate::index::ForwardIndexLookup;
    use promql_parser::label::{METRIC_NAME, MatchOp};
    use regex_syntax::Parser;
    use regex_syntax::hir::{Hir, HirKind};
    use std::collections::HashSet;

    /// Parse a limited regex of the shape `value1|value2|…` into its
    /// literal alternatives. Matches the behaviour of the equivalent
    /// helper in `promql::selector` (RFC mandates we leave that module
    /// untouched, so we re-implement here).
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

    /// Positive equality terms (metric name + `label="literal"`
    /// matchers). Used as the upper-bound cheap estimator in
    /// `estimate_cardinality`.
    pub(super) fn positive_equality_terms(selector: &VectorSelector) -> Vec<Label> {
        let mut terms = Vec::new();
        if let Some(name) = &selector.name {
            terms.push(Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            });
        }
        for m in &selector.matchers.matchers {
            if matches!(m.op, MatchOp::Equal) && !m.value.is_empty() {
                terms.push(Label {
                    name: m.name.clone(),
                    value: m.value.clone(),
                });
            }
        }
        terms
    }

    pub(super) fn has_non_equality_matchers(selector: &VectorSelector) -> bool {
        selector.matchers.matchers.iter().any(|m| {
            matches!(m.op, MatchOp::Re(_) | MatchOp::NotEqual | MatchOp::NotRe(_))
                || (matches!(m.op, MatchOp::Equal) && m.value.is_empty())
        })
    }

    pub(super) fn has_negative_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::NotEqual | MatchOp::NotRe(_)))
    }

    pub(super) fn has_empty_string_matchers(selector: &VectorSelector) -> bool {
        selector
            .matchers
            .matchers
            .iter()
            .any(|m| matches!(m.op, MatchOp::Equal) && m.value.is_empty())
    }

    /// Resolve candidate series IDs using just a [`QueryReader`] and the
    /// inverted index. Mirrors `promql::selector::find_candidates_with_reader`
    /// except that it consults the reader directly (no
    /// `CachedQueryReader`).
    pub(super) async fn find_candidates<R: QueryReader + ?Sized>(
        reader: &R,
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
                let inv = reader
                    .inverted_index(bucket, std::slice::from_ref(&metric_term))
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
        let inv = reader
            .inverted_index(bucket, &all_terms)
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

    /// Apply negative and empty-string matchers using a loaded
    /// `ForwardIndexLookup`. Equivalent to the post-filter block in
    /// `promql::selector::evaluate_selector_with_reader`.
    pub(super) fn apply_post_filters(
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
        // given: a hint-series slice with three buckets in mixed order
        let series = vec![
            ResolvedSeriesRef::new(encode_bucket(TimeBucket { start: 0, size: 1 }), 1),
            ResolvedSeriesRef::new(encode_bucket(TimeBucket { start: 0, size: 1 }), 2),
            ResolvedSeriesRef::new(encode_bucket(TimeBucket { start: 60, size: 1 }), 7),
            ResolvedSeriesRef::new(encode_bucket(TimeBucket { start: 60, size: 1 }), 8),
            ResolvedSeriesRef::new(
                encode_bucket(TimeBucket {
                    start: 120,
                    size: 1,
                }),
                5,
            ),
            // back to the first bucket — a new run, not merged.
            ResolvedSeriesRef::new(encode_bucket(TimeBucket { start: 0, size: 1 }), 9),
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
        let series = vec![
            ResolvedSeriesRef::new(bid, 1),
            ResolvedSeriesRef::new(bid, 2),
            ResolvedSeriesRef::new(bid, 3),
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
// Integration tests (unit 2.3)
//
// These exercise the [`SeriesSource`] contract end-to-end against the
// existing in-memory [`MockQueryReader`] fixture (the same one that backs
// `promql/selector.rs` and `promql/pipeline.rs` tests). They verify:
//
//   - RFC §"Storage Contract" caller → source and source → caller
//     guarantees (series set, time window, ordering, stale markers).
//   - The boundary quirks called out in §5 Decisions Log 2.2 (inclusive
//     start / exclusive end translation over `QueryReader`'s `(start, end]`
//     contract).
//   - Cross-bucket stitching (per-bucket batches in chronological order).
//   - Cardinality-estimate monotonicity + the `u64::MAX` sentinel case.
//   - `selector_util` matcher parity with the shapes covered in
//     `promql/selector.rs`: metric-only, equality, negation, regex OR,
//     empty-string, AND-combination.
//
// Adapter bugs surfaced during authoring: none. If a future test exposes
// one, file it in §5 with a failing case, mark 2.3 `blocked`, and hand
// back — per 2.3 scope we do not patch the adapter here.
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
        let batches = collect_ok(src.samples(SampleHint::new(
            series.clone(),
            TimeRange::new(0, 3_600_000),
        )))
        .await;

        // then: each batch's series_range is a contiguous slice of the hint
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
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
        let batches = collect_ok(src.samples(hint)).await;

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
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
        let batches = collect_ok(src.samples(hint)).await;

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
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(hint)).await;

        // then: only the sample at 1_500 survives — 500 is before the
        // window and 2_500 is after
        let ts = &batches[0].samples.timestamps[0];
        assert_eq!(ts, &vec![1_500]);
    }

    // ---------- Boundary / edge cases (§5 2.2 decisions) ---------------

    #[tokio::test]
    async fn should_include_sample_at_start_ms_boundary() {
        // given: a single sample at exactly time_range.start_ms
        let reader = one_series_reader(h0(), "m", &[(1_000, 7.0)]);
        let src = source(reader);

        // when: request [1_000, 2_000)
        let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(hint)).await;

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
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(hint)).await;

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
        let hint = SampleHint::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
        let batches = collect_ok(src.samples(hint)).await;

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
        let hint = SampleHint::new(series, TimeRange::new(1_500, 1_500));
        let batches = collect_ok(src.samples(hint)).await;

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
            merged.extend(c.series.iter().copied());
        }
        let hint = SampleHint::new(Arc::from(merged), TimeRange::new(0, 7_200_000));
        let batches = collect_ok(src.samples(hint)).await;

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

    // ---------- Cardinality estimate ------------------------------------

    #[tokio::test]
    async fn should_return_monotone_upper_bound_for_estimate_cardinality() {
        // given: three distinct series under one metric name
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        for env in ["a", "b", "c"] {
            b.add_sample(
                h0(),
                labels("m", &[("env", env)]),
                MetricType::Gauge,
                Sample::new(1_000, 1.0),
            );
        }
        let src = source(b.build());

        // when: ask for an estimate over the same selector that resolve
        // will use
        let tr = TimeRange::new(0, 3_600_000);
        let est = src
            .estimate_cardinality(&sel_metric("m"), tr)
            .await
            .expect("estimate_cardinality should succeed");
        let resolved = collect_ok(src.resolve(&sel_metric("m"), tr)).await;
        let final_count: u64 = resolved.iter().map(|c| c.series.len() as u64).sum();

        // then: upper bound is ≥ the final resolved cardinality (the
        // monotonicity RFC §"Execution Model" relies on for the gate)
        assert!(
            est.series_upper_bound >= final_count,
            "upper bound {} < resolved {}",
            est.series_upper_bound,
            final_count
        );
        // No regex/negative/empty-string matchers → plain equality →
        // not flagged approx, bounds collapse.
        assert!(!est.approx);
        assert_eq!(est.series_lower_bound, final_count);
    }

    #[tokio::test]
    async fn should_return_u64_max_upper_bound_when_selector_has_no_positive_terms() {
        // given: a selector with no metric name and only a regex OR
        // matcher (no positive-equality term for the adapter to
        // intersect on).
        let selector = VectorSelector {
            name: None,
            matchers: Matchers {
                matchers: vec![re_matcher("foo", ".*")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[("foo", "bar")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: ask for the estimate
        let est = src
            .estimate_cardinality(&selector, TimeRange::new(0, 3_600_000))
            .await
            .expect("estimate_cardinality should succeed");

        // then: sentinel: upper = u64::MAX, lower = 0, approx
        assert_eq!(est.series_upper_bound, u64::MAX);
        assert_eq!(est.series_lower_bound, 0);
        assert!(est.approx);
    }

    #[tokio::test]
    async fn should_flag_approx_when_regex_or_negative_matchers_present() {
        // given: a selector with a metric name AND a regex matcher
        let selector = sel_with("m", vec![re_matcher("env", "prod|staging")]);
        let mut b = MockMultiBucketQueryReaderBuilder::new();
        b.add_sample(
            h0(),
            labels("m", &[("env", "prod")]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
        let src = source(b.build());

        // when: estimate
        let est = src
            .estimate_cardinality(&selector, TimeRange::new(0, 3_600_000))
            .await
            .expect("estimate_cardinality should succeed");

        // then: positive-equality path yields a finite upper bound, but
        // the non-equality matcher forces `approx: true` and
        // `series_lower_bound = 0` (since the regex / negative / empty
        // filter may shrink the set further than positive-only counting
        // suggests).
        assert!(est.approx);
        assert_eq!(est.series_lower_bound, 0);
        assert!(est.series_upper_bound < u64::MAX);
    }

    // ---------- Selector-matcher parity --------------------------------
    //
    // Per §5 Decisions Log 2.2 we validate `selector_util` behaviourally
    // against hand-built fixtures (not by calling into `promql::selector`,
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
