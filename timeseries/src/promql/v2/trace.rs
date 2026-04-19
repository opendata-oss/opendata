//! Per-query tracing for the v2 PromQL engine.
//!
//! Two sinks, one source:
//! - `tracing` spans emitted via `#[instrument]` / `debug_span!` in the phase
//!   functions (always on when `RUST_LOG` allows).
//! - A [`TraceCollector`] that captures per-phase wall-clock durations and
//!   per-operator cumulative stats, serialized inline with the query response
//!   when the caller sets `?trace=true` or config enables it globally.
//!
//! Per-operator timing uses a decorator — [`TracingOperator`] wraps every
//! `Box<dyn Operator>` produced by [`super::plan::build_physical_plan`] so no
//! operator implementation needs to change.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use serde::Serialize;

use super::batch::StepBatch;
use super::memory::QueryError;
use super::operator::{Operator, OperatorSchema};

tokio::task_local! {
    /// Optional collector available to any code running inside
    /// [`with_trace`]. Storage-layer helpers use this to record I/O
    /// sub-phase timings without threading the collector through every
    /// function signature.
    static TRACE_COLLECTOR: Arc<TraceCollector>;
}

thread_local! {
    /// The node id of the currently-polling [`TracingOperator`], or `None`
    /// if we're outside any operator (e.g. during the planner phases or in
    /// top-level reshape code). Set on entry to `TracingOperator::next`
    /// and restored on exit — so nested child polls see the child's id,
    /// and non-operator call sites see `None`.
    ///
    /// Operator polling is synchronous (the `next()` trait returns a Poll,
    /// it doesn't `.await`), so a plain thread-local with a `Cell` is
    /// sufficient — no task-local needed.
    static CURRENT_NODE_ID: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Toggle bundle describing what tracing should do for a given query.
#[derive(Debug, Clone, Copy, Default)]
pub struct TraceConfig {
    /// Record phase / operator data into a [`TraceCollector`] so it can be
    /// returned to the caller inline. Always emit tracing spans regardless.
    pub collect: bool,
}

impl TraceConfig {
    pub fn enabled(&self) -> bool {
        self.collect
    }
}

/// Fixed set of phase labels so callers don't stringly-type them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// Time spent opening bucket readers / materialising the
    /// `QueryReader` backing the query. Runs *before* `parse` because
    /// the engine needs the reader in hand to construct the source
    /// adapter — previously invisible in the trace.
    ReaderSetup,
    Parse,
    Lower,
    Optimize,
    BuildPhysical,
    Execute,
    Reshape,
    /// Time spent turning the collected batches / `QueryValue` into the
    /// HTTP wire JSON. Covers `serde_json::to_value` + wrapper
    /// construction in the HTTP handler.
    Serialize,
}

/// Storage-layer I/O categories. Each variant maps to one specific call
/// site in `minitsdb::MiniQueryReader`; splitting at this granularity
/// lets the reader distinguish e.g. inverted-index traffic from forward-
/// index traffic when tuning the caches or the object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoKind {
    /// `snapshot.get(series_sample_key)` — fetch one series's encoded
    /// sample block for a bucket. Called once per `(bucket, series_id)`
    /// pair touched by the query; this is the dominant storage call in
    /// sample-heavy queries.
    SamplesFetch,
    /// Time spent decoding a sample block's bytes into `(timestamp,
    /// value)` pairs via `TimeSeriesIterator`. Pure CPU.
    Deserialize,
    /// `forward_index(series_ids)` / `all_forward_index()` — load the
    /// forward index (series-id → labels) for a bucket. Consulted during
    /// selector resolution in `build_physical` and, via
    /// `V2IndexCache`, reused during sample fetching in `execute`.
    ForwardIndexFetch,
    /// `inverted_index(terms)` / `all_inverted_index()` — load the
    /// inverted index (label → series-ids) for a bucket. Always part of
    /// selector resolution in `build_physical`.
    InvertedIndexFetch,
    /// `label_values(label_name)` — enumerate distinct values of a
    /// label within a bucket. Used by the `/api/v1/label/{name}/values`
    /// endpoint and by regex candidate search on the PromQL path.
    LabelValuesFetch,
    /// Catch-all for storage-layer calls we haven't categorised yet.
    Other,
}

impl IoKind {
    fn as_str(self) -> &'static str {
        match self {
            IoKind::SamplesFetch => "samples_fetch",
            IoKind::Deserialize => "deserialize",
            IoKind::ForwardIndexFetch => "forward_index_fetch",
            IoKind::InvertedIndexFetch => "inverted_index_fetch",
            IoKind::LabelValuesFetch => "label_values_fetch",
            IoKind::Other => "other",
        }
    }
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::ReaderSetup => "reader_setup",
            Phase::Parse => "parse",
            Phase::Lower => "lower",
            Phase::Optimize => "optimize",
            Phase::BuildPhysical => "build_physical",
            Phase::Execute => "execute",
            Phase::Reshape => "reshape",
            Phase::Serialize => "serialize",
        }
    }

    fn all() -> [Phase; 8] {
        [
            Phase::ReaderSetup,
            Phase::Parse,
            Phase::Lower,
            Phase::Optimize,
            Phase::BuildPhysical,
            Phase::Execute,
            Phase::Reshape,
            Phase::Serialize,
        ]
    }
}

#[derive(Debug)]
struct Inner {
    /// Indexed by `Phase as usize`.
    phase_ns: [u64; 8],
    operators: Vec<OperatorStatsInner>,
    /// Per-kind accumulators. Tracks both cumulative time (sum across
    /// parallel calls) and wall time (earliest start → latest end), so
    /// the reader can tell "55 s of storage fetches" ran as one serial
    /// wait or in ~8-way parallel over a 7 s wall slice.
    io: BTreeMap<IoKind, IoAccum>,
    /// Sub-phase timings recorded while no operator was on the stack
    /// (e.g. during a planner phase or inside the reshape path). Keyed by
    /// `'static` label; rolled up under the top-level `subphases` field in
    /// the serialized trace.
    orphan_subphases: BTreeMap<&'static str, (u64, u64)>,
    /// Query start reference — every `Instant` recorded against IO kinds
    /// is converted to an offset from this baseline so wall spans are
    /// comparable across parallel tasks.
    query_start: Instant,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            phase_ns: [0; 8],
            operators: Vec::new(),
            io: BTreeMap::new(),
            orphan_subphases: BTreeMap::new(),
            query_start: Instant::now(),
        }
    }
}

/// Per-kind IO accumulator — tracks cumulative time (summed across
/// parallel calls), wall span (min-start → max-end), peak concurrency
/// (max simultaneous in-flight calls), and cumulative payload bytes.
#[derive(Debug, Clone, Copy)]
struct IoAccum {
    cumulative_ns: u64,
    call_count: u64,
    /// Offset (ns since query start) of the earliest call's start.
    first_start_ns: u64,
    /// Offset (ns since query start) of the latest call's end. Wall
    /// span = `last_end_ns - first_start_ns`.
    last_end_ns: u64,
    /// Number of calls currently active. Incremented by
    /// [`TraceCollector::io_started`] and decremented by
    /// [`TraceCollector::io_finished`]; `peak_in_flight` is updated on
    /// each increment.
    in_flight: u64,
    peak_in_flight: u64,
    /// Sum of payload bytes returned by calls of this kind. Additive.
    /// Callers report via [`record_bytes`] when they have the size
    /// handy (e.g. `record.value.len()` after a SlateDB `get`).
    bytes: u64,
}

impl Default for IoAccum {
    fn default() -> Self {
        Self {
            cumulative_ns: 0,
            call_count: 0,
            first_start_ns: u64::MAX,
            last_end_ns: 0,
            in_flight: 0,
            peak_in_flight: 0,
            bytes: 0,
        }
    }
}

/// Internal accumulator — keeps ns precision for repeated additions.
/// Converted to the public, ms-valued [`OperatorStats`] at snapshot time.
#[derive(Debug, Clone)]
struct OperatorStatsInner {
    node_id: usize,
    op_name: &'static str,
    total_ns: u64,
    call_count: u64,
    batch_count: u64,
    cell_count: u64,
    /// Operator-scoped sub-phase timings keyed by a `'static` label the
    /// operator picks (e.g. `"build_batch"`, `"hydrate"`). Populated via
    /// [`record_subphase`] / [`Scope`] while this operator is the current
    /// one on the thread-local stack.
    subphases: BTreeMap<&'static str, (u64, u64)>, // (elapsed_ns, call_count)
    /// Operator-scoped free-form counters keyed by a `'static` label the
    /// operator picks (e.g. `"series_selected"`, `"unique_fingerprints"`).
    /// Additive — successive calls with the same label accumulate.
    counters: BTreeMap<&'static str, u64>,
}

/// Per-query tracing collector. Thread-safe — operators may run on spawned
/// tasks (e.g. [`super::operators::ConcurrentOp`]).
#[derive(Debug, Default)]
pub struct TraceCollector {
    inner: Mutex<Inner>,
}

impl TraceCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reserve a new operator slot and return its node id. Called once per
    /// operator at plan time.
    pub fn register_operator(&self, op_name: &'static str) -> usize {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let node_id = inner.operators.len();
        inner.operators.push(OperatorStatsInner {
            node_id,
            op_name,
            total_ns: 0,
            call_count: 0,
            batch_count: 0,
            cell_count: 0,
            subphases: BTreeMap::new(),
            counters: BTreeMap::new(),
        });
        node_id
    }

    /// Accumulate one `next()` call's timing + output into the slot.
    fn record_call(&self, node_id: usize, elapsed_ns: u64, batch: Option<&StepBatch>) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let slot = &mut inner.operators[node_id];
        slot.total_ns = slot.total_ns.saturating_add(elapsed_ns);
        slot.call_count += 1;
        if let Some(b) = batch {
            slot.batch_count += 1;
            slot.cell_count = slot.cell_count.saturating_add(b.values.len() as u64);
        }
    }

    /// Record the wall-clock duration of a phase. Additive — calling twice
    /// accumulates.
    pub fn record_phase(&self, phase: Phase, elapsed_ns: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        inner.phase_ns[phase as usize] = inner.phase_ns[phase as usize].saturating_add(elapsed_ns);
    }

    /// Mark an I/O call of `kind` as starting. Increments `in_flight`
    /// and widens `peak_in_flight` if the new value sets a record.
    pub fn io_started(&self, kind: IoKind) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let slot = inner.io.entry(kind).or_default();
        slot.in_flight += 1;
        if slot.in_flight > slot.peak_in_flight {
            slot.peak_in_flight = slot.in_flight;
        }
    }

    /// Add `n` payload bytes to the running total for `kind`. Callers
    /// invoke this after a fetch when they have the returned bytes in
    /// hand (e.g. `record.value.len()`).
    pub fn record_bytes(&self, kind: IoKind, bytes: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let slot = inner.io.entry(kind).or_default();
        slot.bytes = slot.bytes.saturating_add(bytes);
    }

    /// Mark an I/O call of `kind` as finished, recording its wall
    /// `[start, start + elapsed_ns)` window. Decrements `in_flight` and
    /// widens `first_start_ns` / `last_end_ns` to bound observed calls.
    pub fn io_finished(&self, kind: IoKind, start: Instant, elapsed_ns: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let start_ns = start
            .saturating_duration_since(inner.query_start)
            .as_nanos() as u64;
        let end_ns = start_ns.saturating_add(elapsed_ns);
        let slot = inner.io.entry(kind).or_default();
        slot.cumulative_ns = slot.cumulative_ns.saturating_add(elapsed_ns);
        slot.call_count += 1;
        slot.in_flight = slot.in_flight.saturating_sub(1);
        if start_ns < slot.first_start_ns {
            slot.first_start_ns = start_ns;
        }
        if end_ns > slot.last_end_ns {
            slot.last_end_ns = end_ns;
        }
    }

    /// Record a free-form sub-phase duration, attributing it to whatever
    /// operator is currently on the thread-local stack (via
    /// [`TracingOperator::next`]). When no operator is on the stack the
    /// sample lands in an "orphan" bucket surfaced at the top level of
    /// the trace — useful for attributing time inside planner phases.
    pub fn record_subphase(&self, node_id: Option<usize>, label: &'static str, elapsed_ns: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        match node_id {
            Some(id) if id < inner.operators.len() => {
                let entry = inner.operators[id].subphases.entry(label).or_default();
                entry.0 = entry.0.saturating_add(elapsed_ns);
                entry.1 += 1;
            }
            _ => {
                let entry = inner.orphan_subphases.entry(label).or_default();
                entry.0 = entry.0.saturating_add(elapsed_ns);
                entry.1 += 1;
            }
        }
    }

    /// Add `value` to the counter named `label`, attributed to the
    /// operator currently on the thread-local stack. No-op when no
    /// operator is on the stack (counters are operator-scoped).
    pub fn record_counter(&self, node_id: Option<usize>, label: &'static str, value: u64) {
        let Some(id) = node_id else { return };
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        if id < inner.operators.len() {
            let entry = inner.operators[id].counters.entry(label).or_default();
            *entry = entry.saturating_add(value);
        }
    }

    /// Snapshot the collector into a serializable trace.
    pub fn finish(&self) -> QueryTrace {
        let inner = self.inner.lock().expect("trace collector poisoned");
        let phases: Vec<PhaseStats> = Phase::all()
            .into_iter()
            .map(|p| PhaseStats {
                name: p.as_str(),
                elapsed_ms: ns_to_ms(inner.phase_ns[p as usize]),
            })
            .collect();
        let total_ns: u64 = inner.phase_ns.iter().copied().sum();
        let operators: Vec<OperatorStats> = inner
            .operators
            .iter()
            .map(|raw| OperatorStats {
                node_id: raw.node_id,
                op_name: raw.op_name,
                total_ms: ns_to_ms(raw.total_ns),
                call_count: raw.call_count,
                batch_count: raw.batch_count,
                cell_count: raw.cell_count,
                subphases: subphase_vec(&raw.subphases),
                counters: counter_vec(&raw.counters),
            })
            .collect();
        let io: Vec<IoStats> = inner
            .io
            .iter()
            .map(|(kind, accum)| {
                let wall_ns = accum.last_end_ns.saturating_sub(accum.first_start_ns);
                IoStats {
                    kind: kind.as_str(),
                    cumulative_ms: ns_to_ms(accum.cumulative_ns),
                    wall_ms: ns_to_ms(wall_ns),
                    call_count: accum.call_count,
                    peak_in_flight: accum.peak_in_flight,
                    bytes: accum.bytes,
                }
            })
            .collect();
        let orphan_subphases = subphase_vec(&inner.orphan_subphases);
        QueryTrace {
            total_ms: ns_to_ms(total_ns),
            phases,
            operators,
            io,
            subphases: orphan_subphases,
        }
    }
}

/// Snapshot helper: convert an internal counter map into a sorted list
/// of [`CounterStats`] (alphabetical by label, stable across runs).
fn counter_vec(map: &BTreeMap<&'static str, u64>) -> Vec<CounterStats> {
    map.iter()
        .map(|(label, value)| CounterStats {
            label,
            value: *value,
        })
        .collect()
}

/// Snapshot helper: convert an internal subphase map into a slowest-first
/// list of [`SubphaseStats`].
fn subphase_vec(map: &BTreeMap<&'static str, (u64, u64)>) -> Vec<SubphaseStats> {
    let mut out: Vec<SubphaseStats> = map
        .iter()
        .map(|(label, (ns, calls))| SubphaseStats {
            label,
            elapsed_ms: ns_to_ms(*ns),
            call_count: *calls,
        })
        .collect();
    out.sort_by(|a, b| b.elapsed_ms.partial_cmp(&a.elapsed_ms).unwrap());
    out
}

/// Convert nanoseconds to milliseconds as `f64` with 3 decimal places
/// so sub-ms timings (e.g. the `lower` phase at 3391 ns) serialize as
/// `0.003` rather than rounding to zero.
#[inline]
fn ns_to_ms(ns: u64) -> f64 {
    ((ns as f64) / 1_000.0).round() / 1_000.0
}

/// Single operator's accumulated stats. Durations are in **milliseconds**
/// (three-decimal precision — `0.003` for a 3 μs operator).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorStats {
    /// Pre-order index within the physical-plan tree.
    pub node_id: usize,
    /// Operator type, e.g. "VectorSelector", "Rollup".
    pub op_name: &'static str,
    /// Cumulative wall-clock time spent inside `next()` in ms.
    pub total_ms: f64,
    /// Number of `next()` calls (including the terminating `None`).
    pub call_count: u64,
    /// Number of non-`None` batches yielded.
    pub batch_count: u64,
    /// Sum of `batch.values.len()` over every yielded batch. Equals
    /// `series_count × step_count` summed across batches.
    pub cell_count: u64,
    /// Operator-internal sub-phase timings — finer-grained than
    /// [`Self::total_ms`]. Slowest-first. Populated by the operator's own
    /// [`Scope::enter`] / [`record_subphase_sync`] calls while it is the
    /// one being polled; other operators' sub-phases land in their own
    /// slot.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub subphases: Vec<SubphaseStats>,
    /// Operator-chosen numeric counters (e.g. `series_selected`,
    /// `unique_fingerprints` on a `VectorSelector`). Unlike sub-phases
    /// these are dimensionless — the operator decides what each label
    /// means. Sorted alphabetically by label.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub counters: Vec<CounterStats>,
}

/// One operator-scoped counter. Additive across calls.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CounterStats {
    pub label: &'static str,
    pub value: u64,
}

/// One phase's wall-clock total in milliseconds.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseStats {
    pub name: &'static str,
    pub elapsed_ms: f64,
}

/// One I/O kind's stats. Reports both **cumulative** time (sum across
/// parallel calls — bounded only by the total CPU/wait budget) and
/// **wall** time (earliest call's start → latest call's end — bounded
/// by the enclosing phase). `cumulativeMs / wallMs` gives average
/// concurrency; `peakInFlight` gives the instantaneous max. If peak
/// matches the configured readahead and avg is much lower, traffic is
/// bursty; if peak *and* avg are both below the readahead cap, something
/// upstream (connection pool, object store) is serializing you.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IoStats {
    /// Machine-friendly kind label — `"samples_fetch"`, etc.
    pub kind: &'static str,
    /// Sum of every call's elapsed wall time. Can exceed the query's
    /// total wall time when calls run in parallel.
    pub cumulative_ms: f64,
    /// Wall-clock window from the earliest call's start to the latest
    /// call's end. Always ≤ the enclosing phase's `elapsedMs`. Divide
    /// `cumulativeMs / wallMs` to infer average parallelism over the
    /// span.
    pub wall_ms: f64,
    pub call_count: u64,
    /// Maximum number of calls of this kind that were simultaneously
    /// in-flight at any instant. Compare against the readahead ceilings
    /// (`METADATA_STAGE_READAHEAD`, `SAMPLE_STAGE_READAHEAD`) to see
    /// whether the planner is saturating them.
    pub peak_in_flight: u64,
    /// Cumulative payload bytes returned by all calls of this kind.
    /// Populated by call sites that invoke [`record_bytes`] after a
    /// fetch (zero when no site reports — indicates we haven't wired
    /// bytes tracking for that kind yet).
    pub bytes: u64,
}

/// One named sub-phase's cumulative stats. Label is operator-chosen,
/// typically `"{OpName}.{sub}"` (e.g. `"VectorSelector.build_batch"`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubphaseStats {
    pub label: &'static str,
    pub elapsed_ms: f64,
    pub call_count: u64,
}

/// Serializable snapshot of the collector. All durations in milliseconds.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTrace {
    pub total_ms: f64,
    pub phases: Vec<PhaseStats>,
    pub operators: Vec<OperatorStats>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub io: Vec<IoStats>,
    /// Sub-phases recorded while no operator was on the thread-local
    /// stack — e.g. inside a planner phase or the reshape path. Rare in
    /// practice; present for completeness so no timing gets dropped.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub subphases: Vec<SubphaseStats>,
}

/// Decorator that times every `next()` on its inner operator.
///
/// Registered once per operator in [`super::plan::build_physical_plan`] when
/// a collector is in scope; otherwise the inner op is returned bare.
pub struct TracingOperator {
    inner: Box<dyn Operator + Send>,
    node_id: usize,
    collector: Arc<TraceCollector>,
}

impl TracingOperator {
    pub fn new(
        inner: Box<dyn Operator + Send>,
        op_name: &'static str,
        collector: Arc<TraceCollector>,
    ) -> Self {
        let node_id = collector.register_operator(op_name);
        Self {
            inner,
            node_id,
            collector,
        }
    }
}

impl Operator for TracingOperator {
    fn schema(&self) -> &OperatorSchema {
        self.inner.schema()
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        // Mark this operator as "currently polling" so [`Scope`] / free
        // sub-phase calls in the downstream call stack attribute to our
        // slot. Save the previous id and restore on return — this gives
        // correct nesting when a parent polls a child (the child's `next`
        // pushes its own id, pops on return, parent resumes).
        let prev = CURRENT_NODE_ID.with(|c| c.replace(Some(self.node_id)));
        let start = Instant::now();
        let poll = self.inner.next(cx);
        CURRENT_NODE_ID.with(|c| c.set(prev));
        // Only record completed polls — `Pending` means "wake me later", not
        // "spent time computing". Recording it would inflate totals by every
        // back-pressure wait.
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed = start.elapsed().as_nanos() as u64;
                self.collector
                    .record_call(self.node_id, elapsed, Some(batch));
            }
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                let elapsed = start.elapsed().as_nanos() as u64;
                self.collector.record_call(self.node_id, elapsed, None);
            }
            Poll::Pending => {}
        }
        poll
    }
}

/// If a collector is supplied, wrap `op` in a [`TracingOperator`]; otherwise
/// return the op as-is.
pub fn maybe_trace(
    op: Box<dyn Operator + Send>,
    op_name: &'static str,
    collector: Option<&Arc<TraceCollector>>,
) -> Box<dyn Operator + Send> {
    match collector {
        Some(c) => Box::new(TracingOperator::new(op, op_name, c.clone())),
        None => op,
    }
}

/// Run `fut` with `collector` installed as the task-local [`TRACE_COLLECTOR`].
/// Any call to [`record_io`] inside `fut` (including in spawned tasks that
/// re-scope the task-local) will be attributed to this collector.
pub async fn with_trace<F, T>(collector: Arc<TraceCollector>, fut: F) -> T
where
    F: Future<Output = T>,
{
    TRACE_COLLECTOR.scope(collector, fut).await
}

/// Return a clone of the current task-local collector, if any. Spawned
/// tasks that need to forward timings can use this to re-scope themselves
/// into the same collector.
pub fn current_collector() -> Option<Arc<TraceCollector>> {
    TRACE_COLLECTOR.try_with(|c| c.clone()).ok()
}

/// Mark an I/O call of `kind` as starting on the task-local collector.
/// Pair with [`io_finished`] (or use [`IoGuard`] / the `record_sync` /
/// `record_async` wrappers which bracket automatically).
fn io_started(kind: IoKind) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.io_started(kind));
}

/// Record an I/O call of `kind` as finished, with wall window
/// `[start, start + elapsed_ns)`. No-op outside a [`with_trace`] scope.
pub fn io_finished(kind: IoKind, start: Instant, elapsed_ns: u64) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.io_finished(kind, start, elapsed_ns));
}

/// Attribute `n` payload bytes to the running total for `kind`. Call
/// this after a fetch once you have the returned bytes handy (e.g.
/// `record.value.len()` after a SlateDB `get`). No-op outside a
/// [`with_trace`] scope — safe to call unconditionally.
pub fn record_bytes(kind: IoKind, bytes: u64) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_bytes(kind, bytes));
}

/// RAII guard that keeps the `in_flight` counter for `kind` incremented
/// from `enter` until drop, and records the final cumulative+wall
/// sample on drop. Used by [`record_sync`] / [`record_async`]; rarely
/// needed directly.
struct IoGuard {
    kind: IoKind,
    start: Instant,
}

impl IoGuard {
    fn enter(kind: IoKind) -> Self {
        io_started(kind);
        Self {
            kind,
            start: Instant::now(),
        }
    }
}

impl Drop for IoGuard {
    fn drop(&mut self) {
        io_finished(
            self.kind,
            self.start,
            self.start.elapsed().as_nanos() as u64,
        );
    }
}

/// Convenience wrapper: time a synchronous block and record it against
/// `kind`, tracking in-flight concurrency for the duration. Returns the
/// block's value.
pub fn record_sync<T>(kind: IoKind, f: impl FnOnce() -> T) -> T {
    let _g = IoGuard::enter(kind);
    f()
}

/// Convenience wrapper: time an async block and record it against `kind`,
/// tracking in-flight concurrency for the duration. Includes both work
/// time and any await-polling latency — the point is exactly this, since
/// async storage fetches are dominated by I/O wait.
pub async fn record_async<F, T>(kind: IoKind, fut: F) -> T
where
    F: Future<Output = T>,
{
    let _g = IoGuard::enter(kind);
    fut.await
}

/// Attribute `elapsed_ns` to the sub-phase named `label`, scoped to the
/// operator currently being polled (via the thread-local set by
/// [`TracingOperator::next`]). If no operator is on the stack, the sample
/// is recorded against the top-level orphan bucket. No-op outside a
/// [`with_trace`] scope.
pub fn record_subphase(label: &'static str, elapsed_ns: u64) {
    let node_id = CURRENT_NODE_ID.with(|c| c.get());
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_subphase(node_id, label, elapsed_ns));
}

/// Attribute `value` to the counter named `label`, scoped to the
/// operator currently being polled (via the thread-local set by
/// [`TracingOperator::next`]). No-op outside a [`with_trace`] scope or
/// when no operator is on the stack. Additive — call multiple times to
/// accumulate.
pub fn record_counter(label: &'static str, value: u64) {
    let node_id = CURRENT_NODE_ID.with(|c| c.get());
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_counter(node_id, label, value));
}

/// Time a synchronous block and record its elapsed wall time against the
/// sub-phase named `label`. Returns the block's value.
pub fn record_subphase_sync<T>(label: &'static str, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    record_subphase(label, start.elapsed().as_nanos() as u64);
    out
}

/// RAII guard for sub-phase timing. `drop` records elapsed wall time
/// against `label`. Use when a block has early returns / `?`s that make a
/// wrapping closure awkward.
///
/// ```ignore
/// let _g = trace::Scope::enter("VectorSelector.build_batch");
/// // ... work ...
/// // Dropped at scope end; elapsed recorded.
/// ```
#[must_use = "dropping the guard is what records the elapsed time; assign it to a `_g` binding"]
pub struct Scope {
    label: &'static str,
    start: Instant,
}

impl Scope {
    /// Start a sub-phase timer. Elapsed wall time is recorded on drop.
    pub fn enter(label: &'static str) -> Self {
        Self {
            label,
            start: Instant::now(),
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        record_subphase(self.label, self.start.elapsed().as_nanos() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
    use super::super::operator::StepGrid;
    use crate::model::Labels;

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    struct FakeOp {
        schema: OperatorSchema,
        to_yield: Vec<StepBatch>,
    }

    impl Operator for FakeOp {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            Poll::Ready(self.to_yield.pop().map(Ok))
        }
    }

    fn single_series_schema() -> Arc<SeriesSchema> {
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::empty()]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        Arc::new(SeriesSchema::new(labels, fps))
    }

    fn fake_batch(steps: usize) -> StepBatch {
        let timestamps: Arc<[i64]> = Arc::from(vec![0i64; steps]);
        StepBatch::new(
            timestamps,
            0..steps,
            SchemaRef::Static(single_series_schema()),
            0..1,
            vec![0.0; steps],
            BitSet::with_len(steps),
        )
    }

    #[test]
    fn should_record_phase_timings_in_order() {
        // given
        let c = TraceCollector::new();
        // when — inputs in ns; snapshot returns ms (f64)
        c.record_phase(Phase::Lower, 100_000);
        c.record_phase(Phase::Execute, 500_000);
        c.record_phase(Phase::Lower, 50_000); // accumulates
        let t = c.finish();
        // then
        assert!((t.total_ms - 0.650).abs() < 1e-9, "total_ms={}", t.total_ms);
        let lower = t.phases.iter().find(|p| p.name == "lower").unwrap();
        assert!((lower.elapsed_ms - 0.150).abs() < 1e-9);
        let exec = t.phases.iter().find(|p| p.name == "execute").unwrap();
        assert!((exec.elapsed_ms - 0.500).abs() < 1e-9);
    }

    #[test]
    fn should_accumulate_operator_stats_across_next_calls() {
        // given
        let c = TraceCollector::new();
        let grid = StepGrid {
            start_ms: 0,
            end_ms: 0,
            step_ms: 1,
            step_count: 1,
        };
        let inner = Box::new(FakeOp {
            schema: OperatorSchema::new(SchemaRef::empty_static(), grid),
            to_yield: vec![fake_batch(2), fake_batch(3)],
        });
        let mut wrapped = TracingOperator::new(inner, "Fake", c.clone());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        for _ in 0..3 {
            let _ = wrapped.next(&mut cx);
        }

        // then
        let t = c.finish();
        assert_eq!(t.operators.len(), 1);
        let s = &t.operators[0];
        assert_eq!(s.op_name, "Fake");
        assert_eq!(s.call_count, 3);
        assert_eq!(s.batch_count, 2);
        assert_eq!(s.cell_count, 5);
    }

    /// Operator emits a sub-phase from inside `next()` via [`Scope`]. The
    /// sample must land under the wrapped operator's slot, not in the
    /// top-level orphan bucket.
    #[test]
    fn should_attribute_subphase_to_current_operator() {
        // given — a FakeOp that records a sub-phase inside its next()
        struct SubphaseOp {
            schema: OperatorSchema,
        }
        impl Operator for SubphaseOp {
            fn schema(&self) -> &OperatorSchema {
                &self.schema
            }
            fn next(
                &mut self,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<StepBatch, QueryError>>> {
                let _g = Scope::enter("work");
                std::thread::sleep(std::time::Duration::from_micros(100));
                drop(_g);
                Poll::Ready(None)
            }
        }
        let grid = StepGrid {
            start_ms: 0,
            end_ms: 0,
            step_ms: 1,
            step_count: 1,
        };
        let c = TraceCollector::new();
        let inner = Box::new(SubphaseOp {
            schema: OperatorSchema::new(SchemaRef::empty_static(), grid),
        });
        let mut wrapped = TracingOperator::new(inner, "Fake", c.clone());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when — must be inside with_trace so the task-local is set
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(with_trace(c.clone(), async move {
            let _ = wrapped.next(&mut cx);
        }));

        // then
        let t = c.finish();
        assert_eq!(t.operators.len(), 1);
        let op = &t.operators[0];
        assert_eq!(op.op_name, "Fake");
        assert_eq!(op.subphases.len(), 1);
        assert_eq!(op.subphases[0].label, "work");
        assert_eq!(op.subphases[0].call_count, 1);
        assert!(op.subphases[0].elapsed_ms > 0.0);
        // Nothing should land in the top-level orphan bucket.
        assert!(
            t.subphases.is_empty(),
            "unexpected orphan: {:?}",
            t.subphases
        );
    }

    /// Sub-phases recorded outside any `TracingOperator::next` fall into
    /// the top-level orphan bucket rather than being silently dropped.
    #[test]
    fn should_record_orphan_subphase_when_no_operator_on_stack() {
        // given
        let c = TraceCollector::new();

        // when
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(with_trace(c.clone(), async {
            record_subphase("loose", 12_345);
        }));

        // then
        let t = c.finish();
        assert_eq!(t.subphases.len(), 1);
        assert_eq!(t.subphases[0].label, "loose");
        assert_eq!(t.subphases[0].call_count, 1);
    }

    /// Operator records a counter from inside `next()`; it must land in
    /// that operator's `counters` slot and be additive across calls.
    #[test]
    fn should_attribute_counter_to_current_operator_and_sum() {
        // given
        struct CounterOp {
            schema: OperatorSchema,
            remaining: usize,
        }
        impl Operator for CounterOp {
            fn schema(&self) -> &OperatorSchema {
                &self.schema
            }
            fn next(
                &mut self,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<StepBatch, QueryError>>> {
                if self.remaining == 0 {
                    return Poll::Ready(None);
                }
                record_counter("rows", 10);
                self.remaining -= 1;
                Poll::Ready(None)
            }
        }
        let grid = StepGrid {
            start_ms: 0,
            end_ms: 0,
            step_ms: 1,
            step_count: 1,
        };
        let c = TraceCollector::new();
        let inner = Box::new(CounterOp {
            schema: OperatorSchema::new(SchemaRef::empty_static(), grid),
            remaining: 3,
        });
        let mut wrapped = TracingOperator::new(inner, "Fake", c.clone());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(with_trace(c.clone(), async move {
            for _ in 0..3 {
                let _ = wrapped.next(&mut cx);
            }
        }));

        // then
        let t = c.finish();
        let op = &t.operators[0];
        assert_eq!(op.counters.len(), 1);
        assert_eq!(op.counters[0].label, "rows");
        assert_eq!(op.counters[0].value, 30);
    }

    /// Counters recorded outside a `TracingOperator::next` scope have no
    /// operator to attribute to and must be silently dropped (they aren't
    /// timings, so they can't share the orphan sub-phase bucket).
    #[test]
    fn should_drop_counter_when_no_operator_on_stack() {
        // given
        let c = TraceCollector::new();

        // when
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(with_trace(c.clone(), async {
            record_counter("loose", 42);
        }));

        // then
        let t = c.finish();
        assert!(t.subphases.is_empty());
        assert!(t.operators.is_empty());
    }

    #[test]
    fn should_leave_op_unwrapped_when_no_collector() {
        // given
        let grid = StepGrid {
            start_ms: 0,
            end_ms: 0,
            step_ms: 1,
            step_count: 1,
        };
        let op: Box<dyn Operator + Send> = Box::new(FakeOp {
            schema: OperatorSchema::new(SchemaRef::empty_static(), grid),
            to_yield: vec![],
        });
        // when
        let result = maybe_trace(op, "Fake", None);
        // then: does not panic, returns operator (inner ptr preserved is not
        // observable — just verify it still polls).
        let _ = result.schema();
    }
}
