---
name: vector-bench
description: Benchmark a vector dataset against OpenData Vector. Handles three modes — built-in dataset (sift, cohere, deep, …), bench-format custom (fvecs/bvecs/parquet you already have), or raw embeddings (npy / single parquet) that need to be converted first. Generates the config, generates ground truth if missing, runs the bench, and writes a summary.
---

# vector-bench

You are running the OpenData Vector benchmark for the user. Be procedural, write
artifacts to disk so the user can re-run without you, and only ask the user when
there's genuine ambiguity.

## Output layout

Every invocation writes to `bench-runs/<run-name>/` at the repo root:

```
bench-runs/<run-name>/
├── bench.toml              # the config you wrote (always present)
├── convert.py              # the conversion script (Mode C only; copied from templates/)
├── data/                   # converted files (Mode C only)
│   ├── base.parquet
│   └── query.parquet
└── summary.md              # final results — always write this, even on failure
```

Pick `<run-name>` from the user's prompt (e.g. `my-cohere-subset`), or default
to a short slug of the input filename.

## Decide the mode

Inspect the user's prompt and any path they gave you:

| Trigger | Mode |
|---|---|
| Built-in name (`sift1m`, `sift100k`, `cohere1m`, `cohere_wiki_10m`, `deep10m`, `deep1b`, `wikipedia_bge_m3_en`, `sift10m`, `sift50m`, `sift100m`, `sift1b`) | **A: built-in** |
| Path to an existing dataset with `base.*` + `query.*` + ground-truth files already in `fvecs`/`bvecs`/`parquet` | **B: bench-format** |
| Path to a single `.npy`, or a single `.parquet` with embeddings but no separate query file | **C: convert** |

Use `Bash` (`ls`, `file`) and `Read` to confirm. If still ambiguous, ask one
question; don't guess.

## Mode A: built-in

1. If the dataset is `sift100k` and `vector/tests/data/sift100k/base.fvecs`
   doesn't exist, run `tar xzf vector/tests/data/sift100k/sift100k.tgz -C
   vector/tests/data/sift100k/`. The bundled tarball is the source of truth.
2. For any other built-in, check whether the expected files are present under
   `vector/bench/data/` (or `$DATA_ROOT`). If missing, do **not** silently
   download — point the user at the section in
   `vector/bench/README.md#datasets` and ask if they want you to run the
   download snippet.
3. **Prompt the user about storage** — see [Storage and disk-space
   planning](#storage-and-disk-space-planning). This is not optional and
   not conditional on dataset size; always ask before writing the config.
4. Write a minimal config (see [Schema](#schema-benchtoml) — built-in form).
5. Skip to [Run](#run).

## Mode B: bench-format custom

1. Inspect the files the user pointed at. For `.parquet`, use the `pyarrow`
   inspection snippet under [Inspecting parquet](#inspecting-parquet) to
   identify the embedding column, candidate id column, and candidate metadata
   columns. For `.fvecs`/`.bvecs`, parse the dim prefix to read dimensions
   from the first record.
2. Confirm distance metric. If the user didn't say, ask: `l2` / `cosine` /
   `dot_product`.
3. **Prompt the user about storage** — see [Storage and disk-space
   planning](#storage-and-disk-space-planning). This is not optional and
   not conditional on dataset size; always ask before writing the config.
4. Write the full config (see [Schema](#schema-benchtoml) — custom form).
5. If `ground_truth_file` doesn't exist, run [gen_groundtruth](#generating-ground-truth) before the main bench.
6. [Run](#run).

## Mode C: convert

1. **`.npy` input**: copy `vector/bench/claude/skills/vector-bench/templates/convert_npy.py`
   to `bench-runs/<run-name>/convert.py`, then run:
   ```bash
   python3 bench-runs/<run-name>/convert.py \
     --input <user-input.npy> \
     --output bench-runs/<run-name>/data/base.parquet \
     --vector-column emb
   ```
   You now have a base file. There's no separate query file — split off the
   last 1000 rows as queries using `parquet_split_queries.py`:
   ```bash
   cp vector/bench/claude/skills/vector-bench/templates/parquet_split_queries.py \
      bench-runs/<run-name>/split_queries.py
   python3 bench-runs/<run-name>/split_queries.py \
     --input bench-runs/<run-name>/data/base.parquet \
     --base bench-runs/<run-name>/data/base.parquet \
     --query bench-runs/<run-name>/data/query.parquet \
     --num-queries 1000
   ```
   (Yes, `--base` reads and writes the same file — the script reads the whole
   table before writing.)

2. **Single `.parquet`** (no separate query file): just use
   `parquet_split_queries.py` directly, with the user's parquet as `--input`.
   Skip the npy conversion.

3. Confirm distance metric (ask if unclear).

4. **Prompt the user about storage** — see [Storage and disk-space
   planning](#storage-and-disk-space-planning). This is not optional and
   not conditional on dataset size; always ask before writing the config.

5. Write the config in **custom parquet form** pointing at the new
   `data/base.parquet` / `data/query.parquet` and
   `data/gt.parquet`. **Do not** create `gt.parquet` yet — the next step
   generates it.

6. Run [gen_groundtruth](#generating-ground-truth) to create `data/gt.parquet`.

7. [Run](#run).

> **Note:** describe the split heuristic ("last 1000 rows are queries") in the
> summary.md so the user can see what you did.

## Schema: bench.toml

### Built-in form (Mode A)

```toml
[data.storage]
type = "SlateDb"
path = "<run-name>"

[data.storage.object_store]
type = "Local"
path = "/tmp/<run-name>"

[[params.recall]]
dataset = "<built-in-name>"
# All other parameters fall back to the built-in's defaults.
```

### Custom form (Modes B and C)

```toml
[data.storage]
type = "SlateDb"
path = "<run-name>"

[data.storage.object_store]
type = "Local"
path = "/tmp/<run-name>"

[[params.recall]]
dataset = "<run-name>"                  # any unique string that isn't a built-in
dimensions = "<u16>"
distance_metric = "<l2|cosine|dot_product>"
format = "<fvecs|bvecs|parquet>"
base_file = "<absolute path>"
query_file = "<absolute path>"
ground_truth_file = "<absolute path>"

# Parquet-only (omit entirely if format is fvecs/bvecs):
vector_column = "<embedding column name>"
ground_truth_column = "<neighbour-list column name in GT file>"
# id_column = "<base id column>"        # only if GT references ids, not row indices

# Filter-only (omit if you're not benchmarking filtered queries):
# metadata_columns = "category,brand"   # comma-sep base columns ingested as attrs
# indexed_columns = "category,brand"    # subset indexed for filtering (omit to index all)
# filter_spec = "/abs/path/to/filter.json"

# Tuning (sensible defaults — uncomment + edit only if the user asks):
nprobe = "100"
num_queries = "1000"
phases = "INGEST,COLD,WARM"
```

### Defaults to pick when the user doesn't specify

- **`nprobe`** = `100`.
- **`num_queries`** = `min(1000, queries_available)`.
- **`split_threshold` / `merge_threshold`** — omit (the dataset's built-in default
  applies). For custom datasets without a built-in default, the bench uses
  `1500` / `500`, which is right for ≥1M-vector datasets. For smaller
  datasets (<1M), explicitly set `split_threshold = "150"` and
  `merge_threshold = "50"`.
- **`block_cache_bytes`** — omit. The bench picks per-phase defaults (25% of
  system memory for ingest, ~67% for warm/cold).
- **Phases** — `INGEST,COLD,WARM` (the default).
- **Storage** — see [Storage and disk-space planning](#storage-and-disk-space-planning).
  Always prompt the user; don't silently default to local for anything
  non-trivial.

## Storage and disk-space planning

> **Always prompt the user about the storage backend before writing
> `bench.toml`.** This is non-negotiable and not conditional on dataset
> size. The wording of the prompt changes based on the estimated footprint
> (see below) but the prompt itself always happens. If you find yourself
> writing `bench.toml` without having asked, you've skipped a required
> step.

The bench's on-disk footprint is bigger than the raw embedding bytes, and
large datasets routinely fill local disk if you leave the default in
place. Equally important: a local-storage run doesn't reflect S3 latency
or cost, which is how production SlateDB usually runs — so even when local
*fits*, the user often wants S3 instead.

### Estimate the footprint

Rough rule: SlateDB on local disk needs about **2–3× the raw embedding
bytes** (`num_vectors × dim × 4`), plus the converted Parquets and ground
truth for Mode C. A few orders of magnitude for perspective:

- sift100k (100K × 128 × 4 ≈ 50 MB): ~150 MB total — local is fine.
- cohere1m (1M × 768 × 4 ≈ 3 GB): ~6–9 GB total — local OK on most disks.
- cohere_wiki_10m (10M × 1024 × 4 ≈ 41 GB): ~80–120 GB — **configure S3**.
- sift1b (1B × 128 × 4 ≈ 512 GB): ~1 TB+ — **must configure S3**.

For Mode C add the size of the converted Parquets (roughly the input file
size) and the generated ground-truth file (negligible — `num_queries × 40`
bytes for top-10 int32 IDs).

### Prompt the user about the storage backend

**Always** prompt the user before writing `bench.toml`. The default in the
config schema is `[data.storage.object_store] type = "Local"`, which writes
to local disk and **won't reflect S3 latency or cost** — the bench is most
useful when configured the way you'd run SlateDB in production, which is
usually backed by object storage.

For estimated footprint ≤ 10 GB, a short prompt is enough:

> Estimated bench footprint is ~**N GB**. I'll put SlateDB on local disk at
> `/tmp/<run-name>/` — okay, or do you want a different local path or S3?

For footprint > 10 GB, prompt with the S3 recommendation explicit:

> Estimated bench footprint is ~**N GB**, more than you usually want on
> local disk. The default storage backend is `Local`, which writes to local
> disk and won't reflect S3 latency or cost. I'd recommend configuring S3 —
> tell me a bucket and region and I'll wire it up. Otherwise tell me which
> local path to use.

If the user picks S3, write:

```toml
[data.storage.object_store]
type = "Aws"
region = "<user>"
bucket = "<user>"
```

…and remind them their AWS credentials need to be discoverable by the AWS
SDK (env vars, `~/.aws/credentials`, or EC2 instance metadata). The bench
will fail fast if credentials are missing.

### Check free disk space before any large write

Whenever storage involves **local disk** — i.e. the storage backend is
`Local`, or for any Mode C path that writes converted Parquets / ground
truth into `bench-runs/<run-name>/data/` — check free space at the target
path before running anything large:

```bash
df -P "<path>" | awk 'NR==2 {print $4 * 1024}'   # bytes free
```

If `free < estimated_footprint × 1.2` (20% headroom), **stop and ask** for
an alternative; don't pick one yourself:

> Need ~**N GB** at `<path>` but only **M GB** is free. Where should the
> data go?
>   1. a different local path with more room (give me the path);
>   2. switch storage to S3 (give me a bucket + region);
>   3. shrink the workload — for a custom dataset, set `max_vectors = "N"`
>      to ingest only a prefix; for a built-in, pick the next smaller
>      variant (e.g. `sift10m` instead of `sift100m`).

Update `bench.toml` with the user's pick and re-check before proceeding.
Never silently write to a path the user didn't choose, even if you can
find one with enough room — surprising the user with 20 GB on the wrong
partition is worse than failing.

### S3-backed runs still need local-disk checks

For S3-backed runs, free-space checks don't apply to the SlateDB data, but
they **do** still apply to:

- the converted Parquets in `bench-runs/<run-name>/data/` (Mode C only —
  read from local disk, not S3);
- the generated `ground_truth_file` (likewise local in Mode C unless
  pointed elsewhere);
- the foyer block cache disk tier (only if `block_cache_disk_bytes` is
  set in the config — uses `block_cache_disk_path`).

## Generating ground truth

Always run from the repo root:

```bash
cargo run -p vector-bench --release --bin gen_groundtruth -- \
  --config bench-runs/<run-name>/bench.toml
```

The tool reads the same TOML the bench reads. It writes the dataset's
`ground_truth_file` in the matching format (`ivecs` for fvecs/bvecs, Parquet
for parquet). Ground truth depth is 10 by default, which is what `recall@10`
needs.

If `filter_spec` is set, the tool generates **filtered** ground truth
automatically — exact top-k among the rows each query's filter matches.
Before running, verify that every column the filter references is listed in
`metadata_columns` (otherwise the filter matches nothing and recall will look
broken).

If `ground_truth_file` already exists, **ask the user** before regenerating;
don't silently overwrite.

## Run

```bash
cargo run -p vector-bench --release -- \
  --config bench-runs/<run-name>/bench.toml
```

Stream the output. Watch for these signals:

- `phase: INGEST` → ingest is running.
- `Ingested N vectors in T s` → ingest completed.
- `recall@10 = ...` → warm phase finished.
- `cold reader p50 = ...` → cold phase finished.
- Anything `panicked at` → halt, capture the error in `summary.md`.

When the run completes (or fails), write `summary.md`.

## Writing summary.md

Always write it, even on failure. Sections to include, in order:

1. **Dataset** — 1–2 sentences describing what data was benchmarked, the row
   count, dim, and metric.
2. **Configuration** — link to `bench.toml`, and a small table of the key
   knobs (dataset, dimensions, metric, format, nprobe, split_threshold,
   num_queries, phases, filter_spec if any).
3. **Results** — a headline metrics table:
   - recall@10
   - warm QPS, warm p50/p90/p99
   - cold p50/p90/p99
   - ingest throughput, num vectors, num centroids
4. **Commentary** — 1 short paragraph per metric explaining what *this* number
   means in *this* setup. Don't editorialise — say things like "warm phase is
   rate-limited by `query_qps_limit = 32`, so this is per-query latency at
   that load, not max throughput."
5. **What this did NOT measure** — the standard caveats list (no metadata
   filters except when `filter_spec` was used, no deletes/updates, no mixed
   read/write, no multi-tenant).
6. **How to reproduce** — the exact `cargo run` invocation pointing at this
   `bench.toml`. If Mode C, also point at `convert.py`.

If the run failed, replace the Results section with the error captured from
stderr and a one-line guess at the cause if you can identify one (common: GT
file missing, vector_column wrong, dimensions mismatch).

## Inspecting parquet

When you need to look at an unfamiliar parquet, prefer a short pyarrow probe
over reading the whole file:

```python
import pyarrow.parquet as pq
md = pq.read_metadata("<path.parquet>")
schema = md.schema.to_arrow_schema()
print("rows:", md.num_rows)
for f in schema:
    print(f"  {f.name}: {f.type}")
# peek at one row to confirm shape
tbl = pq.read_table("<path.parquet>", columns=[<vec_col>]).slice(0, 1)
print(tbl.to_pylist())
```

Run this via `Bash` with `python3 -c "..."` so output ends up in the
transcript. Identify:

- **Embedding column**: a `FixedSizeList<float32>`, `list<float32>`, or
  `list<float64>` — if there's only one, use it. If multiple, ask.
- **ID column**: an `int32`/`int64` named like `id` / `doc_id` — used only
  for GT remap; usually unnecessary if you're regenerating GT.
- **Metadata columns**: strings/ints/bools/floats other than the above —
  candidates to ingest if the user wants filtered queries.

## When to ask the user

**Always ask** (every run, no exceptions):

- **Storage backend** — local vs. S3, and the path/bucket details. See
  [Storage and disk-space planning](#storage-and-disk-space-planning).
  Yes, even for tiny datasets. The schema's `Local` default is a *schema*
  default, not a skill default — the skill always confirms.

Ask only when ambiguity is genuine:

- Multiple float-list columns → which is the embedding?
- No metric implied → `l2` (default), `cosine`, or `dot_product`?
- Single-file split heuristic → "I'll use the last 1000 rows as queries — okay?"
- Pre-existing `ground_truth_file` → "Overwrite it?"

Don't ask:

- Dataset name (default to a slug of the input filename or what the user
  called it).
- Run-name / output path (always `bench-runs/<name>/`).
- Block cache config (defaults are fine).
- Phases (default `INGEST,COLD,WARM`).
- nprobe / split_threshold (use defaults; user can edit the TOML and re-run
  if they want to tune).

## Defensive checks

Before running, verify:

- `cargo` is on PATH (`Bash: cargo --version`).
- The chosen `base_file` / `query_file` exist and are readable.
- For parquet: the columns named in `vector_column` /
  `ground_truth_column` / `id_column` / `metadata_columns` exist in the
  schema.
- For filtered queries: every `@col` referenced in the filter spec is in
  `metadata_columns`.
- For Mode A `sift100k`: `vector/tests/data/sift100k/base.fvecs` exists
  (extract the tarball if not).
- The output dir `bench-runs/<run-name>/` exists (`mkdir -p`).
- **The user has explicitly chosen a storage backend.** Don't proceed with
  the schema default (`Local`) silently — see [Storage and disk-space
  planning](#storage-and-disk-space-planning).
- **Free disk space ≥ estimated footprint × 1.2** at every local path the
  run will write to (storage path if `Local`; `bench-runs/<run-name>/data/`
  for Mode C; `block_cache_disk_path` if hybrid cache is configured).

If any check fails, stop and write `summary.md` describing the problem
instead of running a bench that will silently produce garbage.
