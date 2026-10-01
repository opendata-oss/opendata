# vector-bench skill — manual test scenarios

After editing `SKILL.md` or the templates, walk these scenarios. Each takes
30–60 s. The mechanical layer (templates, conversion correctness, end-to-end
bench) is covered by `vector/bench/skill-tests/run.sh`; this checklist exists
for the *decision-flow* parts that only a real Claude session exercises.

For each scenario, paste the prompt into Claude Code with this skill loaded,
then verify the invariants. The skill should produce a `bench-runs/<name>/`
directory in every case.

## 1. Mode A — built-in dataset

**Prompt:** `/vector-bench sift100k`

**Invariants:**

- `bench-runs/<name>/bench.toml` exists, has `dataset = "sift100k"`, and no
  custom-dataset fields (no `dimensions`, `base_file`, etc.).
- `vector/tests/data/sift100k/base.fvecs` exists after the run (the skill
  should have extracted the tarball if it wasn't already).
- `bench-runs/<name>/summary.md` exists with a non-empty Results section.
- `recall@10` ≥ 0.70 (the sift100k.toml default config gives ~0.84).

## 2. Mode B — bench-format parquet, with ground truth

**Setup:** stage a small bench-format parquet at e.g. `/tmp/bm/` with
`base.parquet`, `query.parquet`, `gt.parquet` (steal from
`run.sh`'s generated fixtures or roll your own).

**Prompt:** `/vector-bench /tmp/bm`

**Invariants:**

- The skill detects Mode B (no conversion happens — no `convert.py` written).
- `bench-runs/<name>/bench.toml` has `format = "parquet"` and points at the
  user's files with `base_file` / `query_file` / `ground_truth_file` (no
  files copied — references kept as paths).
- The pre-existing `gt.parquet` is **not** overwritten or touched (compare
  mtime before/after).
- `summary.md` exists with a recall number; for a well-formed fixture it
  should be near 1.0.

## 3. Mode B — bench-format parquet, missing ground truth

**Setup:** same as (2) but delete `gt.parquet` first.

**Prompt:** `/vector-bench /tmp/bm`

**Invariants:**

- The skill invokes `gen_groundtruth` (visible in transcript).
- A new `gt.parquet` appears at the original path.
- The bench then runs against the freshly-generated GT.
- Recall is reasonable (near 1.0 for the small fixture).

## 4. Mode C — single .npy

**Setup:** generate a tiny `.npy`:

```python
import numpy as np
arr = np.random.RandomState(0).randn(2000, 64).astype(np.float32)
np.save("/tmp/embs.npy", arr)
```

**Prompt:** `/vector-bench /tmp/embs.npy`

**Invariants:**

- `bench-runs/<name>/convert.py` exists (copied from templates).
- `bench-runs/<name>/data/base.parquet` and `data/query.parquet` exist.
- `bench-runs/<name>/data/gt.parquet` exists (generated, not user-supplied).
- The bench runs end-to-end without errors.
- `summary.md` explicitly mentions that the last N rows were split off as
  queries.

## 5. Mode C — parquet with a non-default embedding column name

**Setup:** generate a tiny parquet where the embedding column is named
something other than `emb` (e.g. `vec` or `representation`).

**Prompt:** `/vector-bench /tmp/odd-column.parquet`

**Invariants:**

- The skill correctly identifies the embedding column either by inspecting
  the schema (single float-list column) or by asking the user (multiple
  candidates).
- The generated `bench.toml` has `vector_column = "<actual name>"`, not the
  template's default `emb`.

## 6. Filter sanity — metadata + per-query filter

**Setup:** stage a parquet with a `category` int column and a separate query
parquet with a matching `category` column. (Re-use the fixture pattern from
the filtered-GT verification in the prior work.) Provide a `filter.json`
that does `category = @category`.

**Prompt:** `/vector-bench /tmp/filtered-data filter by category from filter.json`

**Invariants:**

- `bench.toml` has `metadata_columns = "category"` and
  `filter_spec = "<path>"`.
- The skill invokes `gen_groundtruth` (filtered GT is required).
- The bench's recall@10 vs the generated GT is ≥ 0.90 — confirms the filter
  is actually applied during search and matches the GT semantics.
- `summary.md` notes that filtered queries were used and names the filter
  spec file.

## 7. Storage + disk-space prompts

The skill should always prompt about storage before writing `bench.toml`,
and should refuse to run when disk is tight. Two sub-scenarios:

### 7a. Large dataset — should recommend S3

**Prompt:** `/vector-bench cohere_wiki_10m`  *(or any other dataset whose
estimated footprint is > 10 GB)*

**Invariants:**

- The skill prompts you about storage **before** writing `bench.toml`,
  and the prompt explicitly says the default `Local` won't reflect S3
  latency/cost and recommends S3 for large datasets.
- If you answer "use S3 bucket `my-bench` in `us-west-2`", the generated
  `bench.toml` has `[data.storage.object_store] type = "Aws"` with the
  given `region` and `bucket`.
- The skill reminds you that AWS credentials need to be discoverable.

### 7b. Insufficient disk space — should ask for an alternative

**Setup:** make `/tmp` small enough that the bench's footprint won't fit
(or invoke against a dataset large enough that local won't fit on your
real disk).

**Prompt:** `/vector-bench <large dataset>`  *(answer "local" to the
storage prompt, intentionally choosing a path that won't fit)*

**Invariants:**

- The skill checks `df` at the target path before running anything large.
- It refuses to proceed and asks for an alternative (different local path,
  S3, or shrink the workload via `max_vectors` / a smaller built-in).
- It does **not** silently fall back to a different path it picked itself.
- Picking option 3 ("shrink the workload") updates `bench.toml` with
  `max_vectors` and re-runs the check.

---

## Failure modes worth checking deliberately

Every few edits, also verify these don't silently regress:

- **Missing data**: prompt `/vector-bench sift1m` without staging the SIFT1M
  files. The skill should ask if it can run the download snippet, not
  silently `cargo run` and fail.
- **Bad column**: a parquet whose only float-list column is `Float64`. The
  bench should still accept it (the parquet reader handles `Float64` →
  cast). The skill should not refuse.
- **`gt.parquet` exists from a stale filter**: with a `filter_spec` set,
  the skill should *ask* before reusing pre-existing GT, since stale
  filtered GT silently gives wrong recall.

These are decision-flow failures the LLM-driven skill can regress on after a
SKILL.md edit; the mechanical tests in `run.sh` won't catch them.
