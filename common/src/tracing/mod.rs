//! Tracing framework for OpenData databases.
//!
//! Provides a standard way to initialize tracing with both normal log output
//! and Chrome-format trace file output for performance analysis.
//!
//! # Two independent axes
//!
//! - **`RUST_LOG`** controls the fmt (console) layer only. It does not gate
//!   trace capture.
//! - **`OPENDATA_TRACE`** controls what lands in the Chrome trace file. It is
//!   parsed as an [`EnvFilter`] directive string (same syntax as `RUST_LOG`)
//!   and applied as a per-layer filter on the Chrome layer. Default: `debug`
//!   — because the trace macros emit spans at `debug` level so they don't
//!   pollute ordinary `RUST_LOG=info` output.
//!
//! # Discrimination: the `_otrace` marker field
//!
//! Ordinary `tracing::info!` / `info_span!` calls do **not** land in the
//! Chrome trace file, even if `OPENDATA_TRACE` would accept their target and
//! level. The Chrome filter additionally requires a well-known marker field
//! ([`TRACE_MARKER_FIELD`]) to be declared on the span or event's metadata.
//! That marker is injected automatically by the [`trace_instrument`] proc
//! macro and the [`trace_span!`] declarative macro — use those to instrument
//! code you want to appear in traces.
//!
//! # Per-root dynamic filter
//!
//! For per-operation tracing you can open a root span whose
//! [`LEVEL_FILTER_FIELD`] field holds an `EnvFilter` directive string. That
//! filter is parsed, stored in the root span's extensions, and replaces the
//! process-wide `OPENDATA_TRACE` decision for every descendant span.
//! Useful for e.g. opting a single request into `debug`-level query tracing
//! while the rest of the process stays at `info`. See
//! [`request_trace_span_with_filter`].
//!
//! # Extensibility
//!
//! [`init`] takes optional extra [`BoxedLayer`]s — attach file loggers,
//! OTEL exporters, metrics bridges, etc. alongside the console and Chrome
//! layers without forking this module.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::layer::{Context, Filter, Layer, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

pub use opendata_macros::trace_instrument;

/// Environment variable that controls which spans land in the Chrome trace
/// file. Parsed as an [`EnvFilter`] directive string (same syntax as
/// `RUST_LOG`). When unset, defaults to [`DEFAULT_TRACE_FILTER`].
pub const TRACE_ENV_VAR: &str = "OPENDATA_TRACE";

/// Default directive when `OPENDATA_TRACE` is unset. Trace spans are emitted
/// at `debug` level (so they don't pollute ordinary info-level console logs),
/// so the default trace filter must admit debug to capture them.
pub const DEFAULT_TRACE_FILTER: &str = "debug";

/// Name of the synthetic root span created by [`request_trace_span`]. The
/// Chrome layer filter looks for an ancestor span with this name to decide
/// whether a given span should be emitted when per-request mode is active.
pub const TRACE_ROOT_SPAN_NAME: &str = "opendata_trace_root";

/// Name of the marker field that identifies a span or event as participating
/// in OpenData query tracing. Injected automatically by [`trace_instrument`]
/// and [`trace_span!`]. Kept in sync with the same constant in
/// `opendata-macros`.
pub const TRACE_MARKER_FIELD: &str = "_otrace";

/// Name of the optional field on a root span whose string value is parsed
/// as an [`EnvFilter`] directive and applied to all descendant spans. See
/// [`request_trace_span_with_filter`].
pub const LEVEL_FILTER_FIELD: &str = "level_filter";

/// Name of the `trace_id` field automatically declared on every span
/// produced by [`trace_instrument`] / [`trace_span!`] / [`request_trace_span`].
///
/// The [`RequestFilterLayer`] populates this field at span creation time,
/// inheriting from the nearest ancestor's stored trace id or generating a
/// fresh one. Every span captured in the Chrome trace file therefore
/// carries a `trace_id` you can group by in Perfetto.
pub const TRACE_ID_FIELD: &str = "trace_id";

/// Controls how the Chrome trace layer emits events. See
/// [tracing-chrome docs](https://docs.rs/tracing-chrome) for the full
/// picture; the short story:
///
/// - [`TraceStyle::Async`] (default) uses Chrome's async begin/end events
///   (`ph: "b"`/`"e"`). All spans that share a root-ancestor get the same
///   `id`, which keeps their logical grouping intact even across task
///   boundaries — but Perfetto renders each unique span name as its own
///   row inside the async track, so sequential nested spans do *not*
///   visually stack as a flamegraph.
/// - [`TraceStyle::Threaded`] uses Chrome duration events
///   (`ph: "B"`/`"E"`) scoped by thread id. Spans that run on the same
///   thread stack as a flamegraph in Perfetto, which is what you usually
///   want when profiling a sequential pipeline. Work dispatched to
///   tokio worker threads naturally appears on separate rows — which is
///   the accurate picture of parallelism but loses the parent-arrow to
///   the span that spawned it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TraceStyle {
    /// Emit Chrome async events (`ph: "b"`/`"e"`). Default.
    #[default]
    Async,
    /// Emit Chrome duration events (`ph: "B"`/`"E"`). Best for flamegraph-
    /// style visualization of a single-thread pipeline.
    Threaded,
}

impl From<TraceStyle> for tracing_chrome::TraceStyle {
    fn from(s: TraceStyle) -> Self {
        match s {
            TraceStyle::Async => tracing_chrome::TraceStyle::Async,
            TraceStyle::Threaded => tracing_chrome::TraceStyle::Threaded,
        }
    }
}

/// A boxed, type-erased [`Layer`] compatible with the [`Registry`] used by
/// [`init`]. Accepted by [`init`] via the `extra_layers` argument so callers
/// can attach their own layers (file loggers, OTEL exporters, etc.)
/// alongside the built-in console and Chrome layers.
pub type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

/// Create a DEBUG-level span carrying the trace marker field. Convenience
/// wrapper around [`tracing::debug_span!`] that injects
/// [`TRACE_MARKER_FIELD`] so the Chrome trace layer picks it up.
///
/// Trace spans are `debug` (not `info`) on purpose: at the usual
/// `RUST_LOG=info` setting they stay out of the console log and only land
/// in the Chrome trace file.
///
/// Accepts the same `"name", key = value, ...` syntax as
/// [`tracing::debug_span!`]. For more exotic forms (e.g. `parent: ...`,
/// `target: ...`), use `tracing::debug_span!` directly and add
/// `_otrace = tracing::field::Empty` manually.
#[macro_export]
macro_rules! __opendata_trace_span {
    ($name:expr $(,)?) => {
        ::tracing::debug_span!(
            $name,
            _otrace = ::tracing::field::Empty,
            trace_id = ::tracing::field::Empty,
        )
    };
    ($name:expr, $($rest:tt)+) => {
        ::tracing::debug_span!(
            $name,
            _otrace = ::tracing::field::Empty,
            trace_id = ::tracing::field::Empty,
            $($rest)+
        )
    };
}

#[doc(inline)]
pub use crate::__opendata_trace_span as trace_span;

/// Configuration for trace capture.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TracingConfig {
    /// Directory where Chrome trace JSON files will be written. `None`
    /// disables the Chrome layer; only console logging is initialized.
    #[serde(default)]
    pub output_dir: Option<PathBuf>,

    /// If `true`, every span that `OPENDATA_TRACE` permits is captured. If
    /// `false`, spans are only captured when they are descendants of a
    /// [`request_trace_span`] — the per-request opt-in mode.
    #[serde(default)]
    pub always_on: bool,

    /// How the Chrome trace layer emits events. See [`TraceStyle`].
    #[serde(default)]
    pub trace_style: TraceStyle,
}

/// Guard returned by [`init`] that must be held for the lifetime of the
/// process. Dropping it flushes and closes the Chrome trace file.
pub struct TracingGuard {
    _chrome_guard: Option<FlushGuard>,
    output_path: Option<PathBuf>,
}

impl TracingGuard {
    /// Path of the Chrome trace file, if one is being written.
    pub fn output_path(&self) -> Option<&Path> {
        self.output_path.as_deref()
    }
}

/// Initialize tracing for an OpenData binary with the default set of layers
/// (fmt console + optional Chrome trace). Callers that want to attach
/// additional layers should use [`init_with_layers`] instead.
pub fn init(config: TracingConfig) -> TracingGuard {
    init_with_layers(config, Vec::new())
}

/// Like [`init`], but also attaches `extra_layers` to the registry. Each
/// extra layer is a fully-configured, already-filtered [`BoxedLayer`] —
/// attach any per-layer filter the caller needs with
/// [`Layer::with_filter`] / [`Layer::boxed`] before passing it in.
///
/// Example: also write a file log filtered with its own env filter.
///
/// ```ignore
/// use common::tracing::{BoxedLayer, TracingConfig, init_with_layers};
/// use tracing_subscriber::{EnvFilter, Layer, fmt};
///
/// let file = std::fs::File::create("/var/log/opendata-debug.log").unwrap();
/// let extra: BoxedLayer = fmt::layer()
///     .with_writer(std::sync::Arc::new(file))
///     .with_filter(EnvFilter::new("opendata_vector=debug"))
///     .boxed();
///
/// let _guard = init_with_layers(config, vec![extra]);
/// ```
///
/// # Panics
///
/// Panics if tracing has already been initialized in this process, or if the
/// output directory cannot be created.
pub fn init_with_layers(config: TracingConfig, extra_layers: Vec<BoxedLayer>) -> TracingGuard {
    static INIT: AtomicBool = AtomicBool::new(false);
    if INIT.swap(true, Ordering::SeqCst) {
        panic!("common::tracing::init called more than once");
    }

    // `RUST_LOG` must control ONLY the fmt layer. If attached globally via
    // `registry.with(env_filter)`, it would also gate the Chrome layer, making
    // trace capture dependent on log level — exactly what this framework is
    // designed to avoid. Using a per-layer filter keeps the two axes independent.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .with_filter(env_filter);

    let mut guard = TracingGuard {
        _chrome_guard: None,
        output_path: None,
    };

    // Extra layers are `Box<dyn Layer<Registry>>`, so they only satisfy
    // `Layer` against the bare `Registry` type. Attach them FIRST — while the
    // subscriber is still a plain `Registry` — so subsequent `.with()` calls
    // wrap them rather than trying to attach them on top of an already-
    // stacked `Layered<...>` type they couldn't type-check against.
    let registry = tracing_subscriber::registry()
        .with(extra_layers)
        .with(fmt_layer);

    if let Some(dir) = config.output_dir {
        std::fs::create_dir_all(&dir).expect("failed to create trace output directory");
        let filename = format!(
            "opendata-trace-{pid}-{ts}.json",
            pid = std::process::id(),
            ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
        );
        let path = dir.join(filename);
        let (chrome_layer, flush_guard) = ChromeLayerBuilder::new()
            .file(path.clone())
            .include_args(true)
            .trace_style(config.trace_style.into())
            .build();
        let chrome_layer = chrome_layer.with_filter(build_trace_filter(config.always_on));
        // Order matters: Chrome must be added before RequestFilterLayer so
        // Chrome's on_new_span runs first and registers its per-span args
        // block. RequestFilterLayer.on_new_span then dispatches a record
        // for the `trace_id` field, and Chrome's on_record updates the
        // stored args so the id appears in the output JSON.
        registry.with(chrome_layer).with(RequestFilterLayer).init();
        guard._chrome_guard = Some(flush_guard);
        guard.output_path = Some(path);
    } else {
        registry.init();
    }

    guard
}

/// Build the composite filter used by the Chrome layer:
///
/// 1. Root marker spans ([`TRACE_ROOT_SPAN_NAME`]) always pass.
/// 2. Non-root spans/events must carry the [`TRACE_MARKER_FIELD`] marker,
///    so ordinary `info!`/`info_span!` callsites never land in the file.
/// 3. If an ancestor root span carries a stored [`LEVEL_FILTER_FIELD`]
///    filter, it replaces `OPENDATA_TRACE` for descendants.
/// 4. Otherwise `OPENDATA_TRACE` (as an [`EnvFilter`]) must admit the span.
/// 5. If `always_on` is false, additionally require an ancestor named
///    [`TRACE_ROOT_SPAN_NAME`].
fn build_trace_filter<S>(always_on: bool) -> ChromeTraceFilter<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let env_filter = EnvFilter::try_from_env(TRACE_ENV_VAR)
        .unwrap_or_else(|_| EnvFilter::new(DEFAULT_TRACE_FILTER));
    ChromeTraceFilter {
        env: env_filter,
        ancestor_required: !always_on,
        _phantom: std::marker::PhantomData,
    }
}

/// Filter implementation for the Chrome layer. See [`build_trace_filter`].
struct ChromeTraceFilter<S> {
    env: EnvFilter,
    /// If true, non-root spans require an ancestor with
    /// [`TRACE_ROOT_SPAN_NAME`] to be captured. This is the per-request gate.
    ancestor_required: bool,
    _phantom: std::marker::PhantomData<fn(S)>,
}

impl<S> Filter<S> for ChromeTraceFilter<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        if meta.name() == TRACE_ROOT_SPAN_NAME {
            return true;
        }
        if !has_trace_marker(meta) {
            return false;
        }

        // If any ancestor root carries a stored EnvFilter, it replaces
        // OPENDATA_TRACE for descendants. Otherwise fall back to the
        // process-wide filter.
        let passes_level = match find_ancestor_level_filter(cx) {
            Some(filter) => <EnvFilter as Filter<S>>::enabled(&filter, meta, cx),
            None => <EnvFilter as Filter<S>>::enabled(&self.env, meta, cx),
        };
        if !passes_level {
            return false;
        }

        if !self.ancestor_required {
            return true;
        }
        cx.lookup_current()
            .map(|span| {
                span.scope()
                    .from_root()
                    .any(|s| s.name() == TRACE_ROOT_SPAN_NAME)
            })
            .unwrap_or(false)
    }
}

/// True if the callsite declared the trace marker field in its metadata.
/// `FieldSet::field(name)` returns `Some` only when the field is statically
/// declared on this span/event's metadata, so ordinary `info!` calls miss.
fn has_trace_marker(meta: &Metadata<'_>) -> bool {
    meta.fields().field(TRACE_MARKER_FIELD).is_some()
}

/// Walk the current span's scope looking for an ancestor with a stored
/// [`StoredLevelFilter`] extension. Innermost match wins.
///
/// Returns a clone of the ancestor's filter because `enabled` calls may
/// outlive the borrow of the extensions map.
fn find_ancestor_level_filter<S>(cx: &Context<'_, S>) -> Option<EnvFilter>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let current = cx.lookup_current()?;
    for span in current.scope() {
        if let Some(stored) = span.extensions().get::<StoredLevelFilter>() {
            return Some(stored.0.clone());
        }
    }
    None
}

/// Layer that enriches every trace span with a [`TRACE_ID_FIELD`] value and
/// parses any [`LEVEL_FILTER_FIELD`] on root spans.
///
/// Trace-id rules:
/// - If the span is a trace span ([`TRACE_MARKER_FIELD`] is declared in its
///   metadata) and `trace_id` already has a value in its attributes (e.g.
///   [`request_trace_span`] set one explicitly), keep that value.
/// - Else if an ancestor span has a stored trace id, inherit it.
/// - Else generate a fresh [`ulid::Ulid`] and attach it to this span. That
///   span becomes the implicit root of a new trace.
///
/// The trace id is stored both in the span's extensions (for inheritance)
/// and recorded as a field value via the tracing dispatcher (so
/// tracing-chrome picks it up in the span's `args` block).
pub(crate) struct RequestFilterLayer;

struct StoredLevelFilter(EnvFilter);

#[derive(Clone)]
struct StoredTraceId(String);

impl<S> Layer<S> for RequestFilterLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, cx: Context<'_, S>) {
        let meta = attrs.metadata();

        // Only trace-marked spans participate in the trace-id chain.
        if meta.fields().field(TRACE_MARKER_FIELD).is_some() {
            resolve_and_store_trace_id(attrs, id, &cx);
        }

        // Root spans may carry a level_filter directive.
        if meta.name() == TRACE_ROOT_SPAN_NAME {
            let mut visitor = LevelFilterVisitor(None);
            attrs.record(&mut visitor);
            store_filter_on_span(visitor.0, id, &cx);
        }
    }

    fn on_record(&self, id: &Id, record: &Record<'_>, cx: Context<'_, S>) {
        let Some(span) = cx.span(id) else {
            return;
        };
        if span.metadata().name() != TRACE_ROOT_SPAN_NAME {
            return;
        }
        let mut visitor = LevelFilterVisitor(None);
        record.record(&mut visitor);
        store_filter_on_span(visitor.0, id, &cx);
    }
}

/// Resolve the `trace_id` for a newly-created trace span and ensure the
/// field value is both stored in extensions (for descendants) and recorded
/// on the span (for tracing-chrome args output).
fn resolve_and_store_trace_id<S>(attrs: &Attributes<'_>, id: &Id, cx: &Context<'_, S>)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    // Case 1: the span's attrs already supplied a trace_id (e.g. request_trace_span).
    let mut visitor = TraceIdVisitor(None);
    attrs.record(&mut visitor);
    if let Some(existing) = visitor.0 {
        store_trace_id(id, cx, existing);
        return;
    }

    // Case 2: inherit from the nearest ancestor that has a stored trace_id.
    if let Some(inherited) = find_ancestor_trace_id(cx, id) {
        store_trace_id(id, cx, inherited.clone());
        record_trace_id_on_span(id, attrs.metadata(), &inherited);
        return;
    }

    // Case 3: this span is an implicit trace root — mint a fresh id.
    let fresh = ulid::Ulid::new().to_string();
    store_trace_id(id, cx, fresh.clone());
    record_trace_id_on_span(id, attrs.metadata(), &fresh);
}

fn store_trace_id<S>(id: &Id, cx: &Context<'_, S>, trace_id: String)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    if let Some(span) = cx.span(id) {
        span.extensions_mut().insert(StoredTraceId(trace_id));
    }
}

/// Walk the span's ancestors (not including the span itself) for the
/// nearest `StoredTraceId`. Returns a clone of the string.
fn find_ancestor_trace_id<S>(cx: &Context<'_, S>, id: &Id) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let span = cx.span(id)?;
    for ancestor in span.scope().skip(1) {
        if let Some(stored) = ancestor.extensions().get::<StoredTraceId>() {
            return Some(stored.0.clone());
        }
    }
    None
}

/// Record the `trace_id` field value on a span via the current dispatcher.
/// This fires every layer's `on_record` hook, including tracing-chrome's,
/// which updates its stored args so the id appears in the Chrome JSON.
///
/// We wrap the value in `tracing::field::display` so tracing-chrome's
/// `record_debug`-only visitor formats it as a clean string (the wrapper's
/// Debug impl forwards to Display). Passing `&str` directly would produce
/// `"\"id\""` — the string rendered via Debug of &str, which includes
/// surrounding quotes.
fn record_trace_id_on_span(id: &Id, meta: &Metadata<'_>, trace_id: &str) {
    let Some(field) = meta.fields().field(TRACE_ID_FIELD) else {
        return;
    };
    let display = tracing::field::display(trace_id);
    let value: &dyn tracing::Value = &display;
    let values = [(&field, Some(value))];
    let value_set = meta.fields().value_set(&values);
    let record = Record::new(&value_set);
    tracing::dispatcher::get_default(|d| d.record(id, &record));
}

fn store_filter_on_span<S>(filter_str: Option<String>, id: &Id, cx: &Context<'_, S>)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let Some(filter_str) = filter_str else {
        return;
    };
    let filter = match EnvFilter::try_new(&filter_str) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(
                error = %e,
                filter = %filter_str,
                "invalid level_filter on opendata_trace_root; ignoring"
            );
            return;
        }
    };
    let Some(span) = cx.span(id) else {
        return;
    };
    span.extensions_mut().insert(StoredLevelFilter(filter));
}

/// Field visitor that captures the string value of [`LEVEL_FILTER_FIELD`] if
/// any. We accept both `record_str` and `record_debug` because `EnvFilter`
/// strings may be passed via `field = %value` / `field = value`.
struct LevelFilterVisitor(Option<String>);

impl Visit for LevelFilterVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == LEVEL_FILTER_FIELD {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == LEVEL_FILTER_FIELD && self.0.is_none() {
            self.0 = Some(trim_quotes(format!("{:?}", value)));
        }
    }
}

/// Like [`LevelFilterVisitor`] but scoped to [`TRACE_ID_FIELD`]. Captures the
/// string value if the span's attrs declared it.
struct TraceIdVisitor(Option<String>);

impl Visit for TraceIdVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == TRACE_ID_FIELD {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == TRACE_ID_FIELD && self.0.is_none() {
            self.0 = Some(trim_quotes(format!("{:?}", value)));
        }
    }
}

/// Strip surrounding double-quotes from a Debug-formatted string so
/// `field = value` and `field = %value` normalize the same way.
fn trim_quotes(mut s: String) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s = s[1..s.len() - 1].to_string();
    }
    s
}

/// Create a root span that marks the start of a per-request trace. All
/// descendant spans permitted by `OPENDATA_TRACE` will be captured in the
/// Chrome trace output, regardless of [`TracingConfig::always_on`].
///
/// The returned span is not entered. Callers typically use `.in_scope(...)`
/// or `.instrument(span)` on a future.
pub fn request_trace_span(trace_id: &str) -> tracing::Span {
    tracing::debug_span!(
        TRACE_ROOT_SPAN_NAME,
        trace_id = %trace_id,
        _otrace = tracing::field::Empty,
    )
}

/// Like [`request_trace_span`] but additionally sets the
/// [`LEVEL_FILTER_FIELD`] to `filter`. The filter is parsed as an
/// [`EnvFilter`] directive string (same syntax as `RUST_LOG`). When present,
/// it replaces the process-wide `OPENDATA_TRACE` filter for every descendant
/// span — useful for opting a single operation into `debug` capture while
/// the rest of the process stays at `info`.
///
/// A malformed filter string emits a `warn!` and is ignored (the span still
/// acts as a request marker, just without a custom filter).
pub fn request_trace_span_with_filter(trace_id: &str, filter: &str) -> tracing::Span {
    tracing::debug_span!(
        TRACE_ROOT_SPAN_NAME,
        trace_id = %trace_id,
        level_filter = filter,
        _otrace = tracing::field::Empty,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_expose_output_path_on_guard() {
        // given
        let guard = TracingGuard {
            _chrome_guard: None,
            output_path: Some(PathBuf::from("/tmp/x.json")),
        };

        // when
        let path = guard.output_path();

        // then
        assert_eq!(path, Some(Path::new("/tmp/x.json")));
    }

    #[test]
    fn should_return_none_output_path_when_chrome_disabled() {
        // given
        let guard = TracingGuard {
            _chrome_guard: None,
            output_path: None,
        };

        // when / then
        assert!(guard.output_path().is_none());
    }
}
