# Vector Recall Benchmark

Measures recall@k, query latency, and ingestion throughput for the vector database. The benchmark ingests a dataset of
base vectors, then runs a set of queries against the database and compares the results to precomputed ground truth
nearest neighbors.

Reported metrics:

- **recall@10** — fraction of true top-10 neighbors returned
- **QPS** — queries per second
- **p50 / p90 / p99 latency** — query latency percentiles (microseconds)
- **ingest throughput** — vectors per second (when ingestion is not skipped)

## Running

To run the benchmarks, first pull the datasets you wish to test. See the [Datasets](#datasets)
section below for details. Then, you can run the benchmarks using:

```bash
# Run with default settings (in-memory storage, sift1M):
cargo run -p vector-bench --release

# Run with a config file:
cargo run -p vector-bench --release -- --config bench.toml

# Skip cleanup to inspect the database after the run:
cargo run -p vector-bench --release -- --no-cleanup
```

### Phases

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

## Datasets

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

The bundled `sift100k` smoke-test dataset is an exception: its files ship in the repo at
`vector/tests/data/sift100k/` and the dataset definition pins absolute paths at compile time,
so it ignores `data_dir` / `DATA_ROOT`. See [`sift100k.toml`](sift100k.toml) for a ready-to-run
config.

### SIFT1M

1M vectors, 128 dimensions, L2 distance. From the [ANN Benchmarks SIFT1M dataset](http://corpus-texmex.irisa.fr/).

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/sift"
curl -L ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz \
  | tar xz -C "$DATA_ROOT/sift" --strip-components=1
# Expected files: $DATA_ROOT/sift/{sift_base.fvecs,sift_query.fvecs,sift_groundtruth.ivecs}
```

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

Run the `gen_groundtruth` tool to compute exact nearest neighbors via brute-force search.
This streams the base vectors in chunks and uses all available cores.

```bash
cargo run -p opendata-vector --release --bin gen_groundtruth -- \
  --base-fvecs "$DATA_ROOT/cohere-wiki/base.fvecs" \
  --query-fvecs "$DATA_ROOT/cohere-wiki/query.fvecs" \
  --output-ivecs "$DATA_ROOT/cohere-wiki/groundtruth.ivecs" \
  --top-k 100 \
  --distance-metric cosine
```

#### Step 3: Run the benchmark

```bash
cargo run -p vector-bench --release -- --config bench.toml
```

with:

```toml
[[params.recall]]
dataset = "cohere_wiki_10m"
```

## Configuration

The benchmark is configured via a TOML config file passed with `--config`. The config has three sections:

1. **`[data]`** — Storage backend for the database under test.
2. **`[reporter]`** — (Optional) Where to persist benchmark metrics.
3. **`[[params.recall]]`** — (Optional) Per-dataset parameter overrides. Each entry runs one benchmark iteration. When
   present, these replace the default dataset list.

### Parameters

| Parameter           | Type   | Description                                                        |
|---------------------|--------|--------------------------------------------------------------------|
| `dataset`           | string | Dataset name (`sift1m`, `cohere1m`, `cohere_wiki_10m`, `deep10m`, `deep1b`, `wikipedia_bge_m3_en`, `sift10m`, etc.) |
| `dimensions`        | u16    | Vector dimensions (default: from dataset)                          |
| `distance_metric`   | string | `l2`, `cosine`, or `dot_product` (default: from dataset)           |
| `split_threshold`   | usize  | Centroid split threshold (default: from dataset)                   |
| `merge_threshold`   | usize  | Centroid merge threshold (default: from dataset)                   |
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
