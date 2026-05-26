# Vector Recall Benchmark

This directory contains the benchmark for [OpenData Vector](../). The benchmark measures 
**recall@k**, **warm and cold query latency**, and **ingestion throughput** against either one 
of the built-in datasets (SIFT, Cohere, Deep, BIGANN) or your own data.

## Quickstart

```bash
git clone --depth 1 https://github.com/responsivedev/opendata
cd opendata
tar xzf vector/tests/data/sift100k/sift100k.tgz -C vector/tests/data/sift100k/
cargo run -p vector-bench --release -- --config vector/bench/sift100k.toml
```

This runs the bench against a bundled 100K-vector SIFT subset (this small subset is included in 
the repo, so you don't need to download it). This should take ~5 minutes the first time — 
most of which is `cargo build`. The output should look roughly like:

```
Running benchmark: recall
  Loaded 1000 queries, 1000 ground truth entries

=== phase: INGEST ===
  Ingest throughput table: (no samples — ingest finished in under 60s)
  Ingested 100000 vectors in 5.4s (18455 vec/s)
  Num centroids: 991

=== phase: WARM ===
start warmup
warm p90 = 5.75
end warmup
  warm query phase: concurrency = 8, qps_limit = 32
  recall@10 = 0.9962, QPS = 32.0, p50 = 4.80 ms, p90 = 7.95 ms, p99 = 12.74 ms

=== phase: COLD ===
start cold reader phase
  running 1000 queries in groups of 10 (reader re-opened between groups to clear cache)
  cold reader p50 = 3.00 ms, p90 = 6.40 ms, p99 = 9.51 ms (1000 queries)
end cold reader phase
  [benchmark=recall, commit=fc4d99807415c537e5c5add258f70e5e5af1f7e8, branch=vector-benchmark-custom-datasets, dataset=sift100k]
    num_vectors          100.00K
    ingest_secs          5.42
    ingest_vec_per_sec   18.45K
    recall_at_k          1.00
    k                    10
    qps                  31.96
    num_queries          1.00K
    num_centroids        991
    p50_latency_us       4.80K
    p90_latency_us       7.95K
    p99_latency_us       12.74K
    cold_p50_latency_us  3.00K
    cold_p90_latency_us  6.40K
    cold_p99_latency_us  9.51K
```

## Measurements 

- **recall@10** — fraction of true top-10 neighbours returned (uses precomputed exact
  ground truth. You can generate this locally if you don't have one — see
  [Generating ground truth](#generating-ground-truth)).
- **warm latency** — p50/p90/p99 of queries when everything is in local cache. 
- **cold latency** — p50/p90/p99 of queries when index/data must be read from object store. 
- **ingest throughput** — vectors ingested/sec
- **qps** — queries/second 

## Running

You can run the benchmark using the following command:
```bash
# Run with your own config:
cargo run -p vector-bench --release -- --config bench.toml
```

The rest of the README mostly focuses on how to configure the bench.toml to run specific 
datasets, or to restrict the benchmark to specific phases.

## Phases

The benchmark runs as an ordered sequence of phases:

- **`INGEST`** — opens a `VectorDb` and writes the dataset's base vectors.
- **`COLD`** — repeatedly opens a `VectorDbReader` with an empty block cache and measures
  cold-start query latency. Runs 1,000 queries in groups of 10; the reader is re-opened between
  groups so the first query in each group pays the full cold-cache cost.
- **`WARM`** — opens a `VectorDb`, runs a warmup pass over the queries, then runs the
  rate-limited concurrent query workload that produces the headline recall@k and latency metrics.

Phases are configured per-dataset and default to `INGEST,COLD,WARM`. Override the list in
the bench config via the `phases` parameter (comma-separated, in execution order):

```toml
# Query-only re-run against an already-ingested database
[[params.recall]]
dataset = "sift1m"
phases = "COLD,WARM"
```

Skipping `INGEST` requires persistent storage (SlateDB) and `--no-cleanup` on the initial ingest
run so the database survives across invocations:

```bash
# First run: ingest and keep the data
cargo run -p vector-bench --release -- --config bench.toml --no-cleanup

# Subsequent runs: query only (set phases = "COLD,WARM" in bench.toml)
cargo run -p vector-bench --release -- --config bench.toml --no-cleanup
```

## Built-In Datasets

As mentioned above, you can either run vector against your own dataset or one of the built-in 
datasets:

| Dataset         | Number of Vectors | Dimensions | Description |
|-----------------|-------------------| --- | --- | 
| SIFT100K        | 100K              | 128 | The first 100K vectors of SIFT1M |
| SIFT1M          | 1M                | 128 | http://corpus-texmex.irisa.fr/ |
| SIFT10M         | 10M               | 128 | The first 10M vectors of SIFT1B (http://corpus-texmex.irisa.fr/) |
| SIFT100M        | 100M              | 128 | The first 100M vectors of SIFT1B (http://corpus-texmex.irisa.fr/) |
| SIFT1B          | 1B                | 128 | http://corpus-texmex.irisa.fr/ |
| COHERE1M        | 1M                | 768 | 1 million 768-dim text embeddings |
| COHERE_WIKI_10M | 10M               | 1024 | 10 million 1024-dim text embeddings |

The following instructions detail how to download each dataset and configure the benchmark to 
evaluate Vector against it.

### Conventions

All dataset setup snippets below resolve their output location from a shell variable named
`DATA_ROOT`, which defaults to `vector/bench/data` (the path the benchmark uses when no
`data_dir` is set in the bench config). To stage data somewhere else — say, a large external
disk — export `DATA_ROOT` before running the snippet and point the bench at it from your config:

```bash
export DATA_ROOT=/mnt/nvme/vector-bench-data
```

```toml
[[params.recall]]
dataset = "sift1m"
data_dir = "/mnt/nvme/vector-bench-data"
```

The bench resolves each dataset's files relative to `data_dir/<dataset-subdir>/` (e.g.
`data_dir/sift/sift_base.fvecs`, `data_dir/bigann/bigann_base.bvecs`). Snippets always pass
`-L` to `curl` and use absolute output paths so they work from any working directory.

<details>

<summary>SIFT1M</summary>

### SIFT1M

1M vectors, 128 dimensions, L2 distance. From the [ANN Benchmarks SIFT1M dataset](http://corpus-texmex.irisa.fr/).

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/sift"
curl -L ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz \
  | tar xz -C "$DATA_ROOT/sift" --strip-components=1
# Expected files: $DATA_ROOT/sift/{sift_base.fvecs,sift_query.fvecs,sift_groundtruth.ivecs}
```

</details>

<details>

<summary>SIFT10M, SIFT50M, SIFT100M, and SIFT1B</summary>

### SIFT10M, SIFT50M, SIFT100M, and SIFT1B

These benchmark entries use the BIGANN SIFT1B dataset:

- `sift10m`
- `sift50m`
- `sift100m`
- `sift1b`

All four use the same base and query files:

- `bigann/bigann_base.bvecs`
- `bigann/bigann_query.bvecs`

They differ only in:

- how many base vectors the benchmark ingests
- which ground-truth file is used

The benchmark reads `bvecs` directly. The download flow below fetches `u8bin` (from the FB mirror) and converts
it to `bvecs` in one step.

Expected layout:

```text
$DATA_ROOT/bigann/
├── bigann_base.bvecs
├── bigann_query.bvecs
├── bigann_groundtruth_10M.ivecs
├── bigann_groundtruth_50M.ivecs
├── bigann_groundtruth_100M.ivecs
└── bigann_groundtruth_1B.ivecs
```

The original IRISA FTP mirror (`ftp.irisa.fr`) is frequently unreachable. The instructions below use the Facebook
`big-ann-benchmarks` mirror over HTTPS instead. That mirror publishes vectors as `u8bin` and ground truth as the
big-ann `.bin` format, so a short conversion step turns them into the `bvecs` / `ivecs` layout the benchmark reads.

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/bigann"

# Base vectors (~128 GB uncompressed; the mirror serves the file uncompressed).
curl -L -o "$DATA_ROOT/bigann/bigann_base.u8bin" \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin

# Query vectors (~1.3 MB).
curl -L -o "$DATA_ROOT/bigann/bigann_query.u8bin" \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin

# Ground truth (8 MB each). Note: the mirror does NOT publish a 50M ground truth file;
# `sift50m` would require computing ground truth locally.
curl -L -o "$DATA_ROOT/bigann/bigann-10M.bin" \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_10M/bigann-10M
curl -L -o "$DATA_ROOT/bigann/bigann-100M.bin" \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_100M/bigann-100M
curl -L -o "$DATA_ROOT/bigann/bigann-1B.bin" \
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_1B/bigann-1B

DATA_ROOT="$DATA_ROOT" python3 - <<'PY'
import os
from pathlib import Path
import numpy as np

root = Path(os.environ["DATA_ROOT"]) / "bigann"

def u8bin_to_bvecs(src: Path, dst: Path) -> None:
    # u8bin: u32 n, u32 d, then n*d uint8 values.
    # bvecs: for each vector, u32 d then d uint8 values.
    with src.open("rb") as f:
        n = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        d = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
    data = np.memmap(src, dtype=np.uint8, mode="r", offset=8, shape=(n, d))
    out = np.memmap(
        dst,
        dtype=np.dtype([("dim", "<i4"), ("vec", "u1", (d,))]),
        mode="w+",
        shape=(n,),
    )
    chunk = 1_000_000
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        out["dim"][start:end] = d
        out["vec"][start:end] = data[start:end]
    out.flush()

def ann_gt_to_ivecs(src: Path, dst: Path) -> None:
    # big-ann ground truth: u32 n, u32 k, n*k int32 ids, n*k float32 dists.
    # ivecs: for each row, u32 k then k int32 ids (distances dropped).
    with src.open("rb") as f:
        n = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        k = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        ids = np.fromfile(f, dtype=np.int32, count=n * k).reshape(n, k)
    out = np.memmap(
        dst,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<i4", (k,))]),
        mode="w+",
        shape=(n,),
    )
    out["dim"][:] = k
    out["vec"][:] = ids
    out.flush()

u8bin_to_bvecs(root / "bigann_base.u8bin", root / "bigann_base.bvecs")
u8bin_to_bvecs(root / "bigann_query.u8bin", root / "bigann_query.bvecs")
ann_gt_to_ivecs(root / "bigann-10M.bin", root / "bigann_groundtruth_10M.ivecs")
ann_gt_to_ivecs(root / "bigann-100M.bin", root / "bigann_groundtruth_100M.ivecs")
ann_gt_to_ivecs(root / "bigann-1B.bin", root / "bigann_groundtruth_1B.ivecs")
PY

ls -lh "$DATA_ROOT/bigann"
```

Then choose whichever dataset you want in your benchmark config.

For `sift10m`:

```toml
[[params.recall]]
dataset = "sift10m"
```

For `sift50m`:

```toml
[[params.recall]]
dataset = "sift50m"
```

For `sift100m`:

```toml
[[params.recall]]
dataset = "sift100m"
```

For `sift1b`:

```toml
[[params.recall]]
dataset = "sift1b"
```

Notes:

- `sift10m` ingests the first 10,000,000 vectors from `bigann_base.bvecs`.
- `sift50m` ingests the first 50,000,000 vectors from `bigann_base.bvecs`.
- `sift100m` ingests the first 100,000,000 vectors from `bigann_base.bvecs`.
- `sift1b` ingests the full `bigann_base.bvecs`.
- BIGANN is large. The base file is ~128 GB on the FB mirror. After conversion to `bvecs`, `bigann_base.bvecs`
  is ~129 GB (one extra dim prefix per vector).
- `sift50m` ground truth is **not** published on the FB mirror; only 10M / 100M / 1B are. To run `sift50m`
  you'd need to compute ground truth locally over the first 50M base vectors.

</details>

<details>

<summary>Cohere1M</summary>

### Cohere1M

1M vectors, 768 dimensions, cosine distance. Cohere Wikipedia embeddings, distributed as a
pre-converted parquet snapshot by [VectorDBBench](https://github.com/zilliztech/VectorDBBench)
at `s3://assets.zilliz.com/benchmark/cohere_medium_1m/`. The snapshot ships its own ground
truth, so no brute-force step is needed locally.

Expected layout:

```text
$DATA_ROOT/cohere/
├── cohere_base.fvecs        (1M x 768 float32, ~3 GB)
├── cohere_query.fvecs       (~1K x 768 float32, ~3 MB)
└── cohere_groundtruth.ivecs (~1K x 100 int32)
```

#### Copy-paste setup

The bucket is public — `aws s3 cp --no-sign-request` works without AWS credentials. The
parquet files store neighbour pointers as the parquet's `id` column, but the bench keys
vectors by their row index in `cohere_base.fvecs`, so the conversion below remaps each
neighbour `id` to a row index before writing ground truth.

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/cohere/.parquet"

python3 -m venv .venv-cohere
source .venv-cohere/bin/activate
pip install -U pip
pip install pyarrow numpy

# Download the three parquet shards (~3 GB total). Requires the AWS CLI.
for f in train.parquet test.parquet neighbors.parquet; do
  dest="$DATA_ROOT/cohere/.parquet/$f"
  if [ -f "$dest" ]; then
    echo "  $f already present, skipping"
    continue
  fi
  aws s3 cp \
    "s3://assets.zilliz.com/benchmark/cohere_medium_1m/$f" \
    "$dest" \
    --region us-west-2 --no-sign-request
done

DATA_ROOT="$DATA_ROOT" python3 - <<'PY'
import os
import struct
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq

OUT = Path(os.environ["DATA_ROOT"]) / "cohere"
SRC = OUT / ".parquet"


def write_fvecs(path, rows):
    with open(path, "wb") as f:
        for vec in rows:
            f.write(struct.pack("<i", len(vec)))
            f.write(np.asarray(vec, dtype=np.float32).tobytes())


def write_ivecs(path, rows):
    with open(path, "wb") as f:
        for vec in rows:
            f.write(struct.pack("<i", len(vec)))
            f.write(np.asarray(vec, dtype=np.int32).tobytes())


# Base vectors. Remember the parquet `id` -> row-index mapping so we can
# rewrite ground-truth neighbours below.
train = pq.read_table(SRC / "train.parquet")
ids = train.column("id").to_pylist()
id_to_idx = {v: i for i, v in enumerate(ids)}
embs = train.column("emb")
print(f"Converting {len(embs)} base vectors -> cohere_base.fvecs")
write_fvecs(
    OUT / "cohere_base.fvecs",
    (embs[i].as_py() for i in range(len(embs))),
)

# Query vectors.
test = pq.read_table(SRC / "test.parquet")
q_embs = test.column("emb")
print(f"Converting {len(q_embs)} query vectors -> cohere_query.fvecs")
write_fvecs(
    OUT / "cohere_query.fvecs",
    (q_embs[i].as_py() for i in range(len(q_embs))),
)

# Ground truth — parquet stores neighbour `id` values; remap to row indices.
neighbors = pq.read_table(SRC / "neighbors.parquet").column("neighbors_id")
K = 100
print(f"Converting {len(neighbors)} ground-truth rows (k={K}) -> cohere_groundtruth.ivecs")
write_ivecs(
    OUT / "cohere_groundtruth.ivecs",
    (
        [id_to_idx[nid] for nid in neighbors[i].as_py()[:K]]
        for i in range(len(neighbors))
    ),
)
PY

ls -lh "$DATA_ROOT/cohere"
```

The `.parquet/` cache directory can be deleted once the conversion finishes; the bench only
reads the three top-level `.fvecs` / `.ivecs` files.

If you've already run `vector/tests/data/cohere/download_and_convert.py` for the integration
tests, you can skip the steps above and just copy the resulting `cohere_*.{fvecs,ivecs}` into
`$DATA_ROOT/cohere/`.

#### Run the benchmark

```toml
[[params.recall]]
dataset = "cohere1m"
```

</details>

<details>

<summary>Cohere Wikipedia 10M</summary>

### Cohere Wikipedia 10M

10M vectors, 1024 dimensions, cosine distance. Uses Cohere's `embed-multilingual-v3` embeddings from
the [Cohere Wikipedia dataset](https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3).
This dataset mirrors the [turbopuffer vector-10m-hot benchmark](https://github.com/turbopuffer/tpuf-benchmark/blob/main/benchmarks/website/vector-10m-hot.toml).

Expected layout:

```text
$DATA_ROOT/cohere-wiki/
├── base.fvecs         (10M x 1024 float32, ~41 GB)
├── query.fvecs        (1K x 1024 float32, ~4 MB)
└── groundtruth.ivecs  (1K x 100 int32)
```

#### Step 1: Download and convert

The download script streams the English split from HuggingFace, writes the first 10M embeddings
as base vectors and the next 1K as query vectors.

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/cohere-wiki"

python3 -m venv .venv-cohere-wiki
source .venv-cohere-wiki/bin/activate
pip install -U pip
pip install datasets numpy

python3 vector/bench/data/cohere-wiki/download.py --output-dir "$DATA_ROOT/cohere-wiki"
```

This takes a while (the base file is ~41 GB). Progress is printed every 1M vectors.

#### Step 2: Generate ground truth

Run the `gen_groundtruth` tool. It reads the **same bench config** as the runner, and for
every `[[params.recall]]` dataset computes exact top-k neighbours by brute force (streaming
the base in chunks across all cores) and writes the dataset's `ground_truth_file`. So add
the dataset to your `bench.toml` first (Step 3), then:

```bash
cargo run -p vector-bench --release --bin gen_groundtruth -- --config bench.toml
```

The output format matches the dataset's `format` (`ivecs` for fvecs/bvecs, Parquet for
parquet). For datasets with a `filter_spec`, the generated ground truth is **filtered** —
exact top-k among the rows each query's filter matches. (See
[Generating ground truth](#generating-ground-truth).)

#### Step 3: Run the benchmark

```bash
cargo run -p vector-bench --release -- --config bench.toml
```

with:

```toml
[[params.recall]]
dataset = "cohere_wiki_10m"
```

</details>

### Custom datasets

The benchmark also accepts datasets defined entirely in the TOML config. The datasets can be 
formatted in `fvecs`, `bvecs`, or `parquet` format.

To define one, set `dataset` in `[[params.recall]]` to any name that **isn't** one of the
built-in names (`sift1m`, `sift100k`, `cohere1m`, `cohere_wiki_10m`, `sift10m`, `sift50m`, 
`sift100m`, `sift1b`). When the bench sees a name it doesn't recognise it treats the entry as a 
user-defined dataset and requires you to supply the data-file fields explicitly:

| Required for a custom dataset | Type   | Notes |
|-------------------------------|--------|-------|
| `dataset`                     | string | Any unique name not matching a built-in. |
| `dimensions`                  | u16    | Vector dimensionality. |
| `distance_metric`             | string | `l2`, `cosine`, or `dot_product`. |
| `base_file`                   | string | Path to base vectors (relative to `data_dir` if set, absolute otherwise). |
| `query_file`                  | string | Path to query vectors. |
| `ground_truth_file`           | string | Path to ground-truth neighbours. |
| `format`                      | string | `fvecs`, `bvecs`, or `parquet`. |

Every other parameter (split/merge thresholds, `nprobe`, block-cache settings, etc.) falls
back to a sensible default if omitted — same defaults the built-in datasets get.

#### Parquet datasets

`format = "parquet"` lets you point the bench straight at Parquet files. It requires three more 
configs:

| Field                 | Required?            | Notes |
|-----------------------|----------------------|-------|
| `vector_column`       | yes (parquet)        | Name of the embedding column in the base and query files. Type must be `FixedSizeList<Float32>`, `List<Float32>`, or `List<Float64>` (cast to f32). |
| `ground_truth_column` | yes (parquet)        | Name of the neighbour-list column in the ground-truth file. Type `List<Int32>` or `List<Int64>`. |
| `id_column`           | no                   | Name of an ID column in the base file. When set, ground-truth neighbour values are treated as dataset IDs and remapped to the row index each vector is ingested under. When **unset**, neighbour values are assumed to already be 0-based row indices. |

The base, query, and ground-truth Parquet files all use the same `vector_column` /
`ground_truth_column` names. If your ground truth references your own document IDs rather
than row positions, set `id_column` to the base file's ID column (`Int32`/`Int64`) and the
bench builds the `id → row-index` map automatically. 

#### Filtered queries (parquet only)

To benchmark metadata-filtered search, ingest metadata columns and attach a filter to every
query. You'll need to set three more configs:

| Field              | Notes |
|--------------------|-------|
| `metadata_columns` | Comma-separated base-file columns to ingest as attributes. Types are inferred from the schema: `Utf8`/`LargeUtf8` → String, integer types → Int64, float types → Float64, `Boolean` → Bool. |
| `indexed_columns`  | Comma-separated subset of `metadata_columns` to index for filtering. **Empty = index all** metadata columns. |
| `filter_spec`      | Path to a JSON/YAML filter applied to every query (see below). |

The `filter_spec` file uses the same shape as the HTTP API's filter JSON
(`eq` / `neq` / `in` / `and` / `or`). Filter values follow the **`@column` convention**:

- a string `"@col"` binds to that query row's `col` value (read from the query Parquet);
- a string without a leading `@` is a literal string;
- a number is a literal `Int64` (if integral) or `Float64`; a bool is a literal `Bool`;
- the same rules apply to each entry of an `in` filter's `values`.

So every query runs the same filter *shape*, with the literal values varying per query row.
Example `filter.json` — "same category as the query, and one of an allowed brand set":

```json
{ "and": [
    { "eq": { "field": "category", "value": "@category" } },
    { "in": { "field": "brand", "values": ["@brand", "house-brand"] } }
] }
```

> **Ground truth must be filtered too.** recall@k is only meaningful if each query's
> ground truth is the true top-k *among rows that pass that query's filter*. Computing
> ground truth over the unfiltered corpus and then filtering queries produces misleadingly
> low recall. The bench does not yet generate filtered ground truth for you — supply a
> `ground_truth_file` computed with the same filter applied.

#### Example

```toml
[data.storage]
type = "SlateDb"
path = "vector-bench-custom"
[data.storage.object_store]
type = "Local"
path = "/tmp/vector-bench-data"

[[params.recall]]
dataset = "my-cohere-subset"
dimensions = "768"
distance_metric = "cosine"
base_file = "/mnt/data/my-cohere/base.fvecs"
query_file = "/mnt/data/my-cohere/queries.fvecs"
ground_truth_file = "/mnt/data/my-cohere/gt.ivecs"
format = "fvecs"
# Optional tuning — anything you omit falls back to the bench's defaults:
nprobe = "100"
split_threshold = "1500"
num_queries = "1000"
phases = "INGEST,COLD,WARM"
```

And the same dataset as Parquet, where the embedding lives in an `emb` column, the
ground-truth neighbours in a `neighbors` column, and the ground truth references your own
`doc_id`s rather than row positions:

```toml
[[params.recall]]
dataset = "my-cohere-parquet"
dimensions = "768"
distance_metric = "cosine"
format = "parquet"
base_file = "/mnt/data/my-cohere/base.parquet"
query_file = "/mnt/data/my-cohere/queries.parquet"
ground_truth_file = "/mnt/data/my-cohere/gt.parquet"
vector_column = "emb"
ground_truth_column = "neighbors"
id_column = "doc_id"        # omit if neighbours are already 0-based row indices
nprobe = "100"
num_queries = "1000"
phases = "INGEST,COLD,WARM"
```

And the same dataset with metadata-filtered queries — ingest `category`/`brand`, index both,
and filter each query by its own `category`/`brand` columns (`gt.parquet` must be the
filtered ground truth):

```toml
[[params.recall]]
dataset = "my-cohere-filtered"
dimensions = "768"
distance_metric = "cosine"
format = "parquet"
base_file = "/mnt/data/my-cohere/base.parquet"
query_file = "/mnt/data/my-cohere/queries.parquet"
ground_truth_file = "/mnt/data/my-cohere/gt-filtered.parquet"
vector_column = "emb"
ground_truth_column = "neighbors"
metadata_columns = "category,brand"
indexed_columns = "category,brand"   # omit to index all metadata_columns
filter_spec = "/mnt/data/my-cohere/filter.json"
nprobe = "200"
num_queries = "1000"
phases = "INGEST,COLD,WARM"
```

## Generating ground truth

The WARM phase measures recall@k against a precomputed `ground_truth_file`. If you don't have
one, the `gen_groundtruth` tool computes it. It reads the **same bench config** as the runner
and, for every `[[params.recall]]` dataset, brute-forces exact top-k neighbours and writes the
dataset's `ground_truth_file`:

```bash
cargo run -p vector-bench --release --bin gen_groundtruth -- --config bench.toml
```

- Output format matches the dataset's `format`: `ivecs` for `fvecs`/`bvecs`, Parquet (with the
  configured `ground_truth_column`) for `parquet`.
- Neighbours are written as **0-based row indices** (ingest order), so leave `id_column` unset
  for generated parquet ground truth.
- Default depth is top-10, which supports the bench's `recall@10`.
- For a dataset with a `filter_spec`, the ground truth is **filtered**: exact top-k among the
  base rows each query's filter matches (the filter's referenced columns must be listed in
  `metadata_columns`). This is the correct ground truth for filtered-query benchmarking —
  filtering queries against unfiltered ground truth produces misleadingly low recall.

It does not overwrite intelligently or fingerprint inputs — if you change the filter, queries,
or base data, delete the stale `ground_truth_file` (or point at a new path) and regenerate.

## Configuration

The benchmark is configured via a TOML config file passed with `--config`. The config has three sections:

1. **`[data]`** — Storage backend for the database under test.
3. **`[[params.recall]]`** — (Optional) Per-dataset parameter overrides. Each entry runs one benchmark iteration. When
   present, these replace the default dataset list.

### Parameters

| Parameter           | Type   | Description                                                        |
|---------------------|--------|--------------------------------------------------------------------|
| `dataset`           | string | Dataset name (`sift1m`, `cohere1m`, `cohere_wiki_10m`, `deep10m`, `deep1b`, `wikipedia_bge_m3_en`, `sift10m`, etc., or any unique name to define a [custom dataset](#custom-datasets-inline-in-the-bench-config) inline). |
| `dimensions`        | u16    | Vector dimensions (default: from built-in dataset; **required** for a custom dataset). |
| `distance_metric`   | string | `l2`, `cosine`, or `dot_product` (default: from built-in dataset; **required** for a custom dataset). |
| `base_file`         | string | Path to the base-vectors file, relative to `data_dir`. Default: from built-in dataset; **required** for a custom dataset. |
| `query_file`        | string | Path to the query-vectors file, relative to `data_dir`. Default: from built-in dataset; **required** for a custom dataset. |
| `ground_truth_file` | string | Path to the ground-truth file, relative to `data_dir`. Default: from built-in dataset; **required** for a custom dataset. |
| `format`            | string | `fvecs`, `bvecs`, or `parquet`. Default: from built-in dataset; **required** for a custom dataset. |
| `vector_column`     | string | Embedding column name. **Required** when `format = "parquet"`; ignored otherwise. See [Parquet datasets](#parquet-datasets). |
| `ground_truth_column` | string | Neighbour-list column name. **Required** when `format = "parquet"`; ignored otherwise. |
| `id_column`         | string | Base-file ID column for ground-truth remapping. Optional; only used when `format = "parquet"`. |
| `metadata_columns`  | string | Comma-separated base-file columns to ingest as attributes. Only used when `format = "parquet"`. See [Filtered queries](#filtered-queries-parquet-only). |
| `indexed_columns`   | string | Comma-separated subset of `metadata_columns` to index for filtering. Empty = index all. |
| `filter_spec`       | string | Path to a JSON/YAML filter applied to every query (`@column` binds values from the query file). Only used when `format = "parquet"`. |
| `split_threshold`   | usize  | Centroid split threshold (default: from dataset, or 1500 for custom). |
| `merge_threshold`   | usize  | Centroid merge threshold (default: from dataset, or 500 for custom). |
| `nprobe`            | usize  | Number of centroids to probe at query time (default: from dataset) |
| `num_queries`       | usize  | Number of queries to run in the warm phase (default: 100)          |
| `num_cold_queries`  | usize  | Number of queries to run in the cold phase (default: 1000). Queries are cycled when this exceeds the number of loaded warm queries. |
| `query_concurrency` | usize  | Concurrent in-flight queries during the warm query phase (default: 8) |
| `query_qps_limit`   | usize  | Rate cap on warm-phase query submissions, in QPS (default: 32)     |
| `block_cache_bytes` | i64    | In-memory block cache size in bytes. **Unset**: derive from a phase-specific default (25% of system memory for ingest, ~67% for cold/warm). **`-1`**: disable the cache entirely. **`n >= 0`**: use exactly `n` bytes. |
| `block_cache_disk_bytes` | u64 | On-disk block-cache size in bytes. When set, the cache becomes a hybrid memory + disk foyer cache (memory tier sized by `block_cache_bytes`). When unset, the cache is memory-only. Ignored when `block_cache_bytes = -1`. |
| `block_cache_disk_path` | string | Filesystem path for the hybrid cache's disk tier (default: `/mnt/cache/foyer`). Only used when `block_cache_disk_bytes` is set. |
| `data_dir`          | string | Directory containing dataset files (default: `vector/bench/data/`) |
| `vector_config`     | string | Path to a YAML file with vector `Config` overrides                 |
| `phases`            | string | Comma-separated phases to run, in order (default: `INGEST,COLD,WARM`). Allowed values: `INGEST`, `COLD`, `WARM`. See [Phases](#phases). |

### Example: SlateDB with S3

```toml
# bench.toml — run SIFT1M against SlateDB backed by S3

[data.storage]
type = "SlateDb"
path = "vector-bench"

[data.storage.object_store]
type = "Aws"
region = "us-west-2"
bucket = "my-bench-bucket"

[[params.recall]]
dataset = "sift1m"
data_dir = "/mnt/data"
nprobe = "100"
block_cache_bytes = "1073741824"
```

```bash
cargo run -p vector-bench --release -- --config bench.toml
```

### Example: multiple datasets in one run

```toml
[data.storage]
type = "SlateDb"
path = "vector-bench"

[data.storage.object_store]
type = "Aws"
region = "us-west-2"
bucket = "my-bench-bucket"

[[params.recall]]
dataset = "sift1m"
data_dir = "/mnt/data"
nprobe = "100"

[[params.recall]]
dataset = "sift10m"
data_dir = "/mnt/data"
nprobe = "200"
block_cache_bytes = "2147483648"
```

## Driving the bench with the Claude Code skill

The repo ships a [Claude Code](https://docs.claude.com/en/docs/claude-code/overview)
skill — [`/vector-bench`](claude/skills/vector-bench/SKILL.md) — that wraps the whole
flow: it inspects your data, infers the bench config, generates ground truth if
missing, runs the bench, and writes a Markdown summary. It's intended to be an easy way to
benchmark *your own* data; for the built-in datasets the `cargo run` invocations above
are usually faster.

### Install

The skill is committed at `vector/bench/claude/skills/vector-bench/`. 
bench code rather than in a top-level `.claude/`. Claude Code auto-discovers
skills under `.claude/skills/` (project-local), so you can install it with: 

```bash
git clone https://github.com/responsivedev/opendata
cd opendata
mkdir -p .claude/skills
ln -s "$(pwd)/vector/bench/claude/skills/vector-bench" .claude/skills/vector-bench
claude   # /vector-bench is now available
```

```bash
ln -s "$(pwd)/vector/bench/claude/skills/vector-bench" ~/.claude/skills/vector-bench
```

You'll need to have the following installed:

- **`cargo`** — for the bench itself. If you ran the Quickstart, you already have it.
- **`python3` with `pyarrow` + `numpy`** — for the conversion templates
  (`convert_npy.py`, `parquet_split_queries.py`). Install once:
  ```bash
  python3 -m venv .venv && source .venv/bin/activate
  pip install pyarrow numpy
  ```
  (Or use system Python if it already has them.)

### Try it on the bundled synthetic fixture

The bench ships a small synthetic dataset at
[`examples/synthetic-embeddings/`](examples/synthetic-embeddings/README.md) — a single
Parquet of 5,000 × 64-dim Gaussian vectors plus a `category`/`score` pair of metadata
columns. It exercises the most interesting parts of the skill's "convert and benchmark"
path: there's no separate query file (skill has to split), the embedding column is
named `embedding` rather than the default `emb` (skill has to infer from the schema),
and the metadata columns let you optionally try filtered queries.

With the skill installed and Claude Code open in the repo:

```
/vector-bench vector/bench/examples/synthetic-embeddings/embeddings.parquet
```

What the skill should do:

1. Inspect the Parquet, identify `embedding` as the only float-list column, and confirm
   `l2` as the distance metric (the data is Gaussian; no reason to prefer cosine).
2. Copy `parquet_split_queries.py` into a fresh `bench-runs/<name>/`, split off the
   last ~1,000 rows as queries.
3. Write `bench-runs/<name>/bench.toml` with `vector_column = "embedding"`,
   `format = "parquet"`, and sensible defaults.
4. Invoke `gen_groundtruth` to produce `bench-runs/<name>/data/gt.parquet`.
5. Run the bench. Recall@10 should land at or near **1.0** — the dataset is small and
   `nprobe` is generous enough that ANN search matches brute force exactly.
6. Write `bench-runs/<name>/summary.md`.

The absolute recall numbers are not meaningful (Gaussian data has no real similarity
structure to recover) — the point is that the pipeline works end-to-end. For a
filter-aware variant, ask the skill to filter queries by `category`; see
[`examples/synthetic-embeddings/README.md`](examples/synthetic-embeddings/README.md) for
the exact prompt and what to expect.

### Pointing it at your own data

The skill handles three input shapes:

- **Built-in name** (`sift1m`, `cohere1m`, …) — looks at `vector/bench/data/` for the
  files, points you at the dataset's download snippet if they're missing.
- **Existing bench-format files** (you already have `base.*` + `query.*` + ground truth
  in `fvecs`/`bvecs`/`parquet`) — writes a config, generates ground truth if missing,
  runs.
- **Raw embeddings** (`.npy`, or a single `.parquet` with no separate query file) —
  copies a conversion template into the run directory, converts, splits queries off,
  generates ground truth, runs.

If you have a Parquet with a non-standard column layout, point the skill at it and
it'll inspect the schema and ask you only the genuinely ambiguous bits (which of
several float-list columns is the embedding; what distance metric; whether to split
trailing rows off as queries). Everything else uses defaults — you can edit the
`bench-runs/<name>/bench.toml` it writes and rerun directly with `cargo` if you want
to tune nprobe / split thresholds / phases.

The full playbook lives in [`SKILL.md`](claude/skills/vector-bench/SKILL.md) —
you can read it to see exactly what the skill is doing and when it'll prompt you.
