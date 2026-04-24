//! Per-query tracing — the "where did this query spend its time" view
//! callers want when a PromQL request is slow.
//!
//! Two sinks are written in parallel:
//! - `tracing` spans, always on when `RUST_LOG` allows.
//! - A [`TraceCollector`] that accumulates a structured trace which is
//!   returned inline on the query response when `?trace=true` is set.
//!
//! Per-operator timing goes through [`TracingOperator`], a transparent
//! wrapper the physical planner inserts around each concrete operator in
//! [`super::plan::build_physical_plan`]. Operators themselves don't know
//! they're being traced.

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
    /// Installed by [`with_trace`] so I/O call sites can record without
    /// threading the collector through every signature.
    static TRACE_COLLECTOR: Arc<TraceCollector>;
}

thread_local! {
    /// Node id of the currently-polling [`TracingOperator`]. Set on entry to
    /// `next`, restored on exit; nested polls see the child's id, non-operator
    /// call sites see `None`. Plain thread-local is enough because operator
    /// polling is synchronous.
    static CURRENT_NODE_ID: Cell<Option<usize>> = const { Cell::new(None) };
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TraceConfig {
    /// When `true`, also populate a [`TraceCollector`] for inline return.
    pub collect: bool,
}

impl TraceConfig {
    pub fn enabled(&self) -> bool {
        self.collect
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// Opening bucket readers, before `parse`. Must run first because the
    /// source adapter needs the reader.
    ReaderSetup,
    Parse,
    Lower,
    Optimize,
    BuildPhysical,
    Execute,
    Reshape,
    /// Final JSON conversion in the HTTP handler.
    Serialize,
}

/// Storage-layer I/O categories. Timing is recorded from
/// `minitsdb::MiniQueryReader` via `io_trace_*`; per-call payload bytes
/// are recorded from the inverted / forward index readers in
/// `storage::mod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoKind {
    /// Per-`(bucket, series_id)` sample-block fetch — dominant in sample-heavy queries.
    SamplesFetch,
    /// Sample-block decode (pure CPU).
    Deserialize,
    /// Forward index (series-id → labels).
    ForwardIndexFetch,
    /// Inverted index (label → series-ids).
    InvertedIndexFetch,
    /// `label_values` for the labels endpoint / regex candidate search.
    LabelValuesFetch,
}

impl IoKind {
    fn as_str(self) -> &'static str {
        match self {
            IoKind::SamplesFetch => "samples_fetch",
            IoKind::Deserialize => "deserialize",
            IoKind::ForwardIndexFetch => "forward_index_fetch",
            IoKind::InvertedIndexFetch => "inverted_index_fetch",
            IoKind::LabelValuesFetch => "label_values_fetch",
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
    io: BTreeMap<IoKind, IoAccum>,
    /// Subphase samples taken with no operator on the stack (planner /
    /// reshape phases). Surface under the top-level `subphases` field.
    orphan_subphases: BTreeMap<&'static str, (u64, u64)>,
    /// All IO `Instant`s are normalised to offsets from here.
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

/// Per-kind IO accumulator: cumulative time, wall span, peak concurrency,
/// payload bytes.
#[derive(Debug, Clone, Copy)]
struct IoAccum {
    cumulative_ns: u64,
    call_count: u64,
    /// ns since query start.
    first_start_ns: u64,
    /// ns since query start. Wall span = `last_end_ns - first_start_ns`.
    last_end_ns: u64,
    in_flight: u64,
    peak_in_flight: u64,
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

/// Nanosecond-precision accumulator; converted to ms-valued [`OperatorStats`]
/// at snapshot time.
#[derive(Debug, Clone)]
struct OperatorStatsInner {
    node_id: usize,
    op_name: &'static str,
    total_ns: u64,
    call_count: u64,
    batch_count: u64,
    cell_count: u64,
    /// `(elapsed_ns, call_count)` keyed by operator-chosen `'static` label.
    subphases: BTreeMap<&'static str, (u64, u64)>,
    /// Operator-chosen additive counters.
    counters: BTreeMap<&'static str, u64>,
}

/// Per-query collector. Thread-safe since operators may run on spawned tasks
/// (e.g. [`super::operators::concurrent::ConcurrentOp`]).
#[derive(Debug, Default)]
pub struct TraceCollector {
    inner: Mutex<Inner>,
}

impl TraceCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reserves a slot and returns its node id. Called once per operator.
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

    /// Additive.
    pub fn record_phase(&self, phase: Phase, elapsed_ns: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        inner.phase_ns[phase as usize] = inner.phase_ns[phase as usize].saturating_add(elapsed_ns);
    }

    pub fn io_started(&self, kind: IoKind) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let slot = inner.io.entry(kind).or_default();
        slot.in_flight += 1;
        if slot.in_flight > slot.peak_in_flight {
            slot.peak_in_flight = slot.in_flight;
        }
    }

    pub fn record_bytes(&self, kind: IoKind, bytes: u64) {
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        let slot = inner.io.entry(kind).or_default();
        slot.bytes = slot.bytes.saturating_add(bytes);
    }

    /// Records the wall `[start, start + elapsed_ns)` window and decrements
    /// `in_flight`.
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

    /// Attributes to the operator on the thread-local stack; with no operator
    /// on the stack, lands in the top-level "orphan" bucket (useful for
    /// planner phases).
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

    /// Counters are operator-scoped — no-op when no operator is on the stack.
    pub fn record_counter(&self, node_id: Option<usize>, label: &'static str, value: u64) {
        let Some(id) = node_id else { return };
        let mut inner = self.inner.lock().expect("trace collector poisoned");
        if id < inner.operators.len() {
            let entry = inner.operators[id].counters.entry(label).or_default();
            *entry = entry.saturating_add(value);
        }
    }

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

fn counter_vec(map: &BTreeMap<&'static str, u64>) -> Vec<CounterStats> {
    map.iter()
        .map(|(label, value)| CounterStats {
            label,
            value: *value,
        })
        .collect()
}

/// Slowest-first.
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

/// ns → ms as `f64` rounded to 3 decimals so sub-ms timings don't round to zero.
#[inline]
fn ns_to_ms(ns: u64) -> f64 {
    ((ns as f64) / 1_000.0).round() / 1_000.0
}

/// One operator's accumulated stats. Durations in ms (three-decimal precision).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorStats {
    /// Pre-order index within the physical-plan tree.
    pub node_id: usize,
    pub op_name: &'static str,
    pub total_ms: f64,
    pub call_count: u64,
    pub batch_count: u64,
    /// Sum of `batch.values.len()` over yielded batches.
    pub cell_count: u64,
    /// Sub-phases attributed to this operator, slowest-first.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub subphases: Vec<SubphaseStats>,
    /// Operator-chosen dimensionless counters, alphabetical by label.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub counters: Vec<CounterStats>,
}

/// Operator-scoped additive counter.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CounterStats {
    pub label: &'static str,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseStats {
    pub name: &'static str,
    pub elapsed_ms: f64,
}

/// One I/O kind's stats. `cumulativeMs / wallMs` gives average concurrency;
/// `peakInFlight` gives instantaneous max. Peak matching the readahead ceiling
/// with low average means bursty; both low means something upstream is
/// serializing.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IoStats {
    pub kind: &'static str,
    /// Sum across parallel calls; may exceed total wall time.
    pub cumulative_ms: f64,
    /// Earliest start → latest end; always ≤ enclosing phase.
    pub wall_ms: f64,
    pub call_count: u64,
    pub peak_in_flight: u64,
    /// Zero if no call site has wired [`record_bytes`] for this kind.
    pub bytes: u64,
}

/// One sub-phase's stats. Label is operator-chosen, typically `"{OpName}.{sub}"`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubphaseStats {
    pub label: &'static str,
    pub elapsed_ms: f64,
    pub call_count: u64,
}

/// Serializable collector snapshot. All durations in milliseconds.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTrace {
    pub total_ms: f64,
    pub phases: Vec<PhaseStats>,
    pub operators: Vec<OperatorStats>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub io: Vec<IoStats>,
    /// Sub-phases recorded outside any operator (planner / reshape phases).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub subphases: Vec<SubphaseStats>,
}

/// Times every `next()` on its inner operator. Inserted once per operator in
/// [`super::plan::build_physical_plan`] when a collector is in scope.
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

/// Wraps in [`TracingOperator`] iff a collector is supplied.
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

/// Installs `collector` as the task-local [`TRACE_COLLECTOR`] for `fut`.
pub async fn with_trace<F, T>(collector: Arc<TraceCollector>, fut: F) -> T
where
    F: Future<Output = T>,
{
    TRACE_COLLECTOR.scope(collector, fut).await
}

/// Used by spawned tasks that need to re-scope themselves into the same collector.
pub fn current_collector() -> Option<Arc<TraceCollector>> {
    TRACE_COLLECTOR.try_with(|c| c.clone()).ok()
}

/// Pair with [`io_finished`], or use [`record_sync`] / [`record_async`].
fn io_started(kind: IoKind) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.io_started(kind));
}

/// No-op outside a [`with_trace`] scope.
pub fn io_finished(kind: IoKind, start: Instant, elapsed_ns: u64) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.io_finished(kind, start, elapsed_ns));
}

/// No-op outside a [`with_trace`] scope — safe to call unconditionally.
pub fn record_bytes(kind: IoKind, bytes: u64) {
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_bytes(kind, bytes));
}

/// RAII guard: holds `in_flight` for `kind` from `enter` until drop, then
/// records the cumulative+wall sample. Used by [`record_sync`] / [`record_async`].
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

/// Time a sync block against `kind`, counting in-flight for its duration.
pub fn record_sync<T>(kind: IoKind, f: impl FnOnce() -> T) -> T {
    let _g = IoGuard::enter(kind);
    f()
}

/// Time an async block against `kind`. Includes await-polling latency — the
/// point, since storage fetches are I/O-dominated.
pub async fn record_async<F, T>(kind: IoKind, fut: F) -> T
where
    F: Future<Output = T>,
{
    let _g = IoGuard::enter(kind);
    fut.await
}

/// Attributes to the operator currently on the stack, or the orphan bucket.
/// No-op outside [`with_trace`].
pub fn record_subphase(label: &'static str, elapsed_ns: u64) {
    let node_id = CURRENT_NODE_ID.with(|c| c.get());
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_subphase(node_id, label, elapsed_ns));
}

/// Operator-scoped and additive. No-op when no operator is on the stack.
pub fn record_counter(label: &'static str, value: u64) {
    let node_id = CURRENT_NODE_ID.with(|c| c.get());
    let _ = TRACE_COLLECTOR.try_with(|c| c.record_counter(node_id, label, value));
}

pub fn record_subphase_sync<T>(label: &'static str, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    record_subphase(label, start.elapsed().as_nanos() as u64);
    out
}

/// RAII sub-phase timer. Records on drop — use when early returns make a
/// wrapping closure awkward. Example: `let _g = Scope::enter("...");`
#[must_use = "dropping the guard is what records the elapsed time; assign it to a `_g` binding"]
pub struct Scope {
    label: &'static str,
    start: Instant,
}

impl Scope {
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
