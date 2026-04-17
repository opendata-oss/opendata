# Query Tracing

The `common::tracing` module provides a shared framework for capturing
performance traces of query operations across all OpenData databases. Traces
are written in Chrome Trace Format and viewable in
[Perfetto UI](https://ui.perfetto.dev) (preferred) or `chrome://tracing`.

## Two streams, asymmetric membership

OpenData has two observability streams:

| Stream | What's in it | Destination | Gate |
|---|---|---|---|
| **Logs** | Every `tracing` callsite at or above the configured level | stderr via fmt layer | `RUST_LOG` |
| **Traces** | Only callsites emitted via `trace_span!` / `#[trace_instrument]` / `request_trace_span` | Chrome JSON file | `OPENDATA_TRACE` + ancestor root |

Trace callsites are emitted at **`debug`** level (not `info`). That keeps
them out of the default `RUST_LOG=info` console stream — ordinary
operators reading logs don't see `traced_op` scroll past — but they still
flow into the Chrome file because `OPENDATA_TRACE` defaults to `debug`.

Consequences:

- `RUST_LOG=info` → trace spans are **absent** from logs (desired default).
- `RUST_LOG=debug` → trace spans **appear** in logs alongside ordinary
  debug output.
- Chrome trace file (when enabled) always captures the trace-marked
  callsites that `OPENDATA_TRACE` admits.
- Plain `info!()` / `info_span!()` are *never* in the Chrome file no
  matter what `OPENDATA_TRACE` is set to.

### How the discrimination works (Chrome side only)

The Chrome layer's filter requires every span or event to declare a
well-known marker field (`_otrace`) on its metadata before it's captured.
The trace macros inject that field automatically; ordinary `tracing` macros
don't. The fmt layer does NOT check this marker — it's the Chrome filter's
job to decide who gets into the trace file, and the trace file's job alone.

## Every captured span carries a `trace_id`

The framework auto-attaches a `trace_id` field to every span it captures:

- If the span is a [`request_trace_span`] (or a span otherwise explicitly
  declaring `trace_id = %...`), the caller-supplied id is used.
- Else if any ancestor span already has a stored trace id, this span
  inherits it.
- Else a fresh [ULID](https://github.com/ulid/spec) is minted; this span
  becomes the implicit root of a new trace.

The value appears in the Chrome trace JSON under each span's `args`, so you
can filter/search by `trace_id` in Perfetto to isolate a specific logical
operation. Root-span begin events include the id directly; descendant
spans record it after creation (via the tracing dispatcher), so it shows
up on their end events in async mode and on both begin and end in
threaded mode.

## Trace style (async vs. threaded)

`TracingConfig::trace_style` picks how the Chrome layer emits events. See
the `TraceStyle` docs for detail; the short version:

| Style | Phase codes | Rendering in Perfetto |
|---|---|---|
| `Async` (default) | `ph: "b"`/`"e"` + `id` | One row per unique span name under a shared async track; visually flat even when spans nest in time. |
| `Threaded` | `ph: "B"`/`"E"` (no id) | Flamegraph stacking of nested spans on each thread; ideal for profiling a sequential pipeline. Spawned work appears on its own thread row. |

The default stays `Async` because it preserves logical span grouping
across threads via the shared `id`. Flip to `Threaded` when you want the
flamegraph view of a single-task pipeline.

## Environment variables

| Env var | Controls | Default |
|---|---|---|
| `RUST_LOG` | fmt (console) layer | `info` |
| `OPENDATA_TRACE` | Chrome trace layer | `debug` |

`OPENDATA_TRACE` uses the same `EnvFilter` directive syntax as `RUST_LOG`.
Because trace spans are debug-level, directives need to admit `debug`:

```sh
OPENDATA_TRACE=debug                                  # capture every trace span (default)
OPENDATA_TRACE=vector::query_engine=debug             # just that module
OPENDATA_TRACE=debug,vector::write=off                # everything except vector::write
OPENDATA_TRACE=off                                    # disable capture entirely
OPENDATA_TRACE=off,vector::query_engine=debug         # opt in to exactly one module
```

## Activation modes

### Process-wide

Set `output_dir` and `always_on = true` on the [`TracingConfig`] passed to
`common::tracing::init`. Every trace-marked span permitted by
`OPENDATA_TRACE` is captured for the life of the process. A single JSON file
is produced per process: `opendata-trace-<pid>-<epoch_secs>.json`.

### Per-request (HTTP)

Set `output_dir` but leave `always_on = false`. Send the header
`X-Opendata-Trace: true` (or `?trace=1` as a query parameter) with the
request. The server wraps the handler in a `request_trace_span` which opts
that request into tracing. The response includes an `X-Opendata-Trace-Id`
header with the assigned ID. Search for the ID inside the trace JSON (or
filter in Perfetto) to isolate the request's spans. `OPENDATA_TRACE` still
scopes what's captured inside the traced request.

### Per-request with a custom level filter

Sometimes you want a single request captured at `debug` while the rest of
the process stays at `info`. Send a directive string via
`X-Opendata-Trace-Filter` (or `?trace_filter=...`):

```sh
curl -H 'X-Opendata-Trace-Filter: vector::query_engine=debug' \
     -X POST 'https://.../api/v1/vector/search' ...
```

The directive is parsed as an `EnvFilter` and *replaces* `OPENDATA_TRACE`
for the lifetime of that request's root span. The header implicitly opts
into tracing, so `X-Opendata-Trace: true` is redundant when the filter
header is present. A malformed directive is logged at `warn!` and ignored
(the request is still traced with `OPENDATA_TRACE`).

Programmatically, call `request_trace_span_with_filter(trace_id, filter)`
to open such a root span in non-HTTP contexts.

## How to view a trace

1. Locate the JSON file in `output_dir`.
2. Open <https://ui.perfetto.dev>.
3. Click "Open trace file" and select the JSON. Perfetto loads it locally —
   no account, no upload.
4. Use the search bar to find a span by name or trace_id, or drag-select a
   region to see per-span aggregate statistics.
5. For aggregate analysis (averages / percentiles across many requests),
   open the "Query (SQL)" panel and query the `slice` table.

## How to instrument code

### Functions — `#[trace_instrument]`

Use the proc-macro attribute from `opendata_macros`. It takes the same
arguments as `#[tracing::instrument]` and additionally injects the marker
field so the Chrome layer picks up the span. Use `skip_all` and list useful
fields explicitly — cloning whole payloads into the trace file is expensive.

```rust
use opendata_macros::trace_instrument;

#[trace_instrument(skip_all, fields(limit = query.limit))]
pub async fn search(query: &Query) -> Result<Vec<Hit>> {
    // ...
}
```

### Inline spans — `trace_span!`

For spans created outside a function boundary (around a block of work):

```rust
use common::tracing::trace_span;

let span = trace_span!("load_posting_list", centroid_id = 42);
let posting = span.in_scope(|| load(42))?;
```

`trace_span!` accepts the same `"name", key = value, ...` syntax as
`tracing::info_span!`. For the more exotic `parent: ...` / `target: ...`
forms, use `tracing::info_span!` directly and add `_otrace =
tracing::field::Empty` manually.

### Propagating spans across `tokio::spawn`

`tokio::spawn` creates a new task, and that task does **not** inherit the
caller's current span by default. If you spawn a future whose work should
appear under the current span in the trace, attach the span explicitly:

```rust
use tracing::Instrument;

let span = tracing::Span::current();
tokio::spawn(async move {
    // work
}.instrument(span));
```

### Choosing what to instrument

- **Do instrument**: high-level phases (search, load-and-score, filter apply),
  IO-bound operations (storage reads, network calls), per-batch loops.
- **Don't instrument**: tight CPU loops, trivial accessors, very hot paths
  where the span overhead would distort the measurement.

Rule of thumb: if you'd want to see it as a bar on a timeline, instrument it.

## Behavior details

- The Chrome layer always captures the synthetic root span created by
  `request_trace_span` (recognized by its fixed name `opendata_trace_root`)
  — regardless of `OPENDATA_TRACE`, so we always have a record that a
  request was traced.
- Non-root spans must (a) carry the `_otrace` marker field, (b) satisfy
  `OPENDATA_TRACE`, and (c) when `always_on = false`, have an
  `opendata_trace_root` ancestor.
- `init` must be called exactly once per process. The returned `TracingGuard`
  must be held for the life of the process — dropping it flushes and closes
  the JSON file.

### Implementation: per-layer filters, not global

The fmt layer and the Chrome layer each get their own filter attached via
`Layer::with_filter`. If `EnvFilter` were instead attached to the registry
as a *global* layer (`registry.with(env_filter)`), its decision would
propagate to every downstream layer, re-coupling the two axes. Using
per-layer filters is what lets `RUST_LOG` and `OPENDATA_TRACE` be truly
independent.

## Attaching additional layers

`init_with_layers(config, Vec<BoxedLayer>)` accepts extra layers that are
attached to the same registry as the built-in fmt and Chrome layers.
Typical uses: a JSON file logger for structured events, an OTEL exporter,
a metrics bridge. Construct your layer, attach whatever per-layer filter
it needs, then `.boxed()` and pass it in:

```rust
use common::tracing::{BoxedLayer, TracingConfig, init_with_layers};
use tracing_subscriber::{EnvFilter, Layer, fmt};

let file = std::fs::File::create("/var/log/opendata-debug.log").unwrap();
let file_layer: BoxedLayer = fmt::layer()
    .with_writer(std::sync::Arc::new(file))
    .with_filter(EnvFilter::new("opendata_vector=debug"))
    .boxed();

let _guard = init_with_layers(config, vec![file_layer]);
```

Extras are attached to the registry *before* the built-in layers so their
boxed trait objects (`Box<dyn Layer<Registry>>`) type-check. This does not
affect their visibility into events — every layer in the stack sees every
span/event it's interested in.
