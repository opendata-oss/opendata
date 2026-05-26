# Synthetic embeddings — a fixture for trying the bench / skill

A small ready-to-use Parquet (`embeddings.parquet`, ~1.9 MB) that exists
mostly to exercise the `/vector-bench` skill's "convert + benchmark"
path without you having to find or generate your own embeddings first.

## What's in it

| Column | Type | Description |
|---|---|---|
| `embedding` | `FixedSizeList<Float32, 64>` | 5,000 Gaussian random 64-dim vectors (deterministic, seeded RNG=42). |
| `category` | `String` | one of `clothing`, `electronics`, `books`, `home`, `garden`. |
| `score` | `Float64` | uniform random in `[0, 1)`. |

The vectors are pure Gaussian noise — there's no real similarity structure
to recover, so the *absolute* recall numbers from a run aren't meaningful
("80% recall on random data" doesn't tell you anything). What this fixture
**does** test is that the whole pipeline — column inference, query split,
ground-truth generation, ingest, query — works end-to-end against a file
that looks like real user data, not a doctored bench-format input.

It deliberately exercises three things the skill has to handle:

1. **A single file with no separate query set** — the skill should detect
   this and use `parquet_split_queries.py` to peel off trailing rows as
   queries.
2. **A non-default embedding column name** (`embedding`, not `emb`) — the
   skill should inspect the schema and configure `vector_column = "embedding"`
   instead of assuming a default.
3. **Metadata columns sitting alongside the embedding** (`category`,
   `score`) — the skill can ignore them on a basic run, or ingest them and
   configure a `filter_spec` if you ask.

## Try it

With the repo cloned, the bench built (`cargo build -p vector-bench
--release` or just let the first invocation build), and Claude Code open in
the repo:

```
/vector-bench vector/bench/examples/synthetic-embeddings/embeddings.parquet
```

What you should see the skill do:

1. Inspect the schema and notice there's exactly one float-list column
   (`embedding`).
2. Ask which distance metric to use (pick `l2` — Gaussian-noise data has no
   reason to favour cosine or dot product).
3. Confirm splitting trailing rows off as queries (the skill should propose
   ~1,000 trailing rows by default; for this fixture you can also say "500
   queries is fine").
4. Copy `parquet_split_queries.py` into `bench-runs/<name>/`, run it to
   produce `base.parquet` and `query.parquet`.
5. Write `bench-runs/<name>/bench.toml` with `vector_column = "embedding"`,
   `format = "parquet"`, sensible defaults for `nprobe` / `split_threshold`.
6. Run `gen_groundtruth` to produce `data/gt.parquet`.
7. Run the bench. Recall@10 should land at or near **1.0** — the dataset is
   small and `nprobe` is generous enough that the ANN search basically
   matches brute force.
8. Write `bench-runs/<name>/summary.md`.

For a filter-aware run, ask the skill something like:

```
/vector-bench vector/bench/examples/synthetic-embeddings/embeddings.parquet
filter queries to the same category
```

The skill should add `metadata_columns = "category"`, drop a
`filter_spec` like `{"eq": {"field": "category", "value": "@category"}}`
next to the bench config, and regenerate ground truth as **filtered** top-k
(rows in the same category as each query). Recall@10 should still land near
1.0 against the filtered GT — that's the proof the filter is actually
applied during search.

## Regenerating

The fixture is committed, but the script is too. To regenerate at a
different size or dimension:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pyarrow numpy
python3 vector/bench/examples/synthetic-embeddings/generate.py --rows 20000 --dim 128
```

The script is seeded (`--seed 42` by default) so re-running with the
defaults produces bit-identical output — `git diff` stays clean.
