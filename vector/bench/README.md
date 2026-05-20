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

### DEEP10M and DEEP1B

96 dimensions, L2 distance, `fvecs` format. These benchmark entries expect local DEEP vectors converted into the same
`fvecs` / `ivecs` file layout as the other benchmarks.

Expected layout:

```text
$DATA_ROOT/deep/
├── deep_base.fvecs
├── deep_query.fvecs
├── deep_groundtruth_10M.ivecs
└── deep_groundtruth_1B.ivecs
```

Important:

- `deep10m` and `deep1b` do **not** use the same ground-truth file.
- For `deep10m`, use the Yandex 10M debug subset files together:
  `base.10M.fbin` and `query.public.10K.fbin`, then generate ground truth locally with
  `gen_deep_groundtruth`.
- For `deep1b`, use the Yandex full-dataset files together:
  `base.1B.fbin`, `query.public.10K.fbin`, and `groundtruth.public.10K.ibin`.
- Do **not** mix the Yandex 10M debug subset with `matsui528/deep1b_gt`.
- The benchmark now runs a brute-force sanity check for `deep10m` before ingest. If that check fails, your DEEP files
  do not match each other.

The benchmark expects **all vectors in `fvecs` format** and **ground truth in `ivecs` format**.

#### Copy-paste setup for `deep10m`

This setup intentionally uses:

- `base.10M.fbin` from Yandex for the base vectors
- `query.public.10K.fbin` from Yandex for the queries
- `gen_deep_groundtruth` to generate the ground truth locally

That avoids relying on a questionable prepublished `deep10m` ground-truth pairing.

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/deep"

curl -L -o "$DATA_ROOT/deep/base.10M.fbin" \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin

curl -L -o "$DATA_ROOT/deep/query.public.10K.fbin" \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin

DATA_ROOT="$DATA_ROOT" python3 - <<'PY'
import os
from pathlib import Path
import numpy as np

root = Path(os.environ["DATA_ROOT"]) / "deep"

def fbin_to_fvecs(src: Path, dst: Path) -> None:
    with src.open("rb") as f:
        n = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        d = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
    data = np.memmap(src, dtype=np.float32, mode="r", offset=8, shape=(n, d))
    out = np.memmap(
        dst,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<f4", (d,))]),
        mode="w+",
        shape=(n,),
    )
    chunk = 100_000
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        out["dim"][start:end] = d
        out["vec"][start:end] = data[start:end]
    out.flush()

fbin_to_fvecs(root / "base.10M.fbin", root / "deep_base.fvecs")
fbin_to_fvecs(root / "query.public.10K.fbin", root / "deep_query.fvecs")
PY

cargo run -p opendata-vector --release --bin gen_deep_groundtruth -- \
  --base-fbin "$DATA_ROOT/deep/base.10M.fbin" \
  --query-fbin "$DATA_ROOT/deep/query.public.10K.fbin" \
  --output-ivecs "$DATA_ROOT/deep/deep_groundtruth_10M.ivecs" \
  --top-k 100 \
  --distance-metric l2

ls -lh "$DATA_ROOT/deep"
```

Then run:

```bash
cargo run -p vector-bench --release -- --config bench.toml
```

with:

```toml
[[params.recall]]
dataset = "deep10m"
```

If the files are aligned correctly, the benchmark will print:

```text
deep10m sanity check passed for query 0
```

before ingestion starts.

#### Copy-paste setup for `deep1b`

`base.1B.fbin` alone is around 388 GB — point `DATA_ROOT` at a disk with room before running.

This setup intentionally uses:

- `base.1B.fbin` from Yandex for the base vectors
- `query.public.10K.fbin` from Yandex for the queries
- `groundtruth.public.10K.ibin` from Yandex for the ground truth

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/deep"

curl -L -o "$DATA_ROOT/deep/base.1B.fbin" \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.1B.fbin

curl -L -o "$DATA_ROOT/deep/query.public.10K.fbin" \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin

curl -L -o "$DATA_ROOT/deep/groundtruth.public.10K.ibin" \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/groundtruth.public.10K.ibin

DATA_ROOT="$DATA_ROOT" python3 - <<'PY'
import os
from pathlib import Path
import numpy as np

root = Path(os.environ["DATA_ROOT"]) / "deep"

def fbin_to_fvecs(src: Path, dst: Path) -> None:
    with src.open("rb") as f:
        n = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        d = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
    data = np.memmap(src, dtype=np.float32, mode="r", offset=8, shape=(n, d))
    out = np.memmap(
        dst,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<f4", (d,))]),
        mode="w+",
        shape=(n,),
    )
    chunk = 100_000
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        out["dim"][start:end] = d
        out["vec"][start:end] = data[start:end]
    out.flush()

def ibin_to_ivecs(src: Path, dst: Path) -> None:
    with src.open("rb") as f:
        n = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
        d = int(np.fromfile(f, dtype=np.uint32, count=1)[0])
    data = np.memmap(src, dtype=np.int32, mode="r", offset=8, shape=(n, d))
    out = np.memmap(
        dst,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<i4", (d,))]),
        mode="w+",
        shape=(n,),
    )
    chunk = 100_000
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        out["dim"][start:end] = d
        out["vec"][start:end] = data[start:end]
    out.flush()

fbin_to_fvecs(root / "base.1B.fbin", root / "deep_base.fvecs")
fbin_to_fvecs(root / "query.public.10K.fbin", root / "deep_query.fvecs")
ibin_to_ivecs(root / "groundtruth.public.10K.ibin", root / "deep_groundtruth_1B.ivecs")
PY

ls -lh "$DATA_ROOT/deep"
```

#### Smaller DEEP subset for a quick smoke test

The smallest public DEEP subset with matching published ground truth that I found is `deep1M`, from
`matsui528/deep1b_gt`.

Source: <https://github.com/matsui528/deep1b_gt>

That repo publishes:

- `deep1M_groundtruth.ivecs`
- a `download_deep1b.py` helper
- a `pickup_vecs.py` helper to build `deep1M_base.fvecs` from the first 1M base vectors

There does not appear to be a standard public `deep100K` package with matching ground truth. If you want exactly
100K, the practical approach is to start from `deep1M`, take the first 100K base vectors, and recompute exact ground
truth locally for that 100K subset.

The current benchmark does not have a built-in `deep1m` dataset entry, but `deep1M` is the best public DEEP
smoke-test-sized subset I found.

### Upstash Wikipedia BGE-M3

The benchmark includes an English Wikipedia BGE-M3 dataset entry named `wikipedia_bge_m3_en`.

Expected layout:

```text
$DATA_ROOT/wikipedia-bge-m3/en/
├── base.fvecs
├── query.fvecs
└── groundtruth.ivecs
```

Dataset characteristics:

- 1024 dimensions
- dot-product search
- embeddings generated with `BAAI/bge-m3`

Recommended sourcing flow:

1. Download the English split from `Upstash/wikipedia-2024-06-bge-m3`.
   Source: <https://huggingface.co/datasets/Upstash/wikipedia-2024-06-bge-m3>
2. Extract paragraph embeddings and write them to `base.fvecs`.
3. Prepare a held-out query set in the same 1024-d format and write it to `query.fvecs`.
4. Compute exact top-k neighbors for those queries and write them to `groundtruth.ivecs`.

Notes:

- The benchmark does **not** read Hugging Face parquet directly; convert embeddings to `fvecs` first.
- The benchmark assumes vectors are already in the same embedding space as the base set. If you generate queries with
  `BAAI/bge-m3`, use the same model and normalization settings you used for the base vectors when you computed ground
  truth.

#### Copy-paste setup for `wikipedia_bge_m3_en`

This example builds a runnable benchmark dataset from the first 1,000,000 English embeddings and the next 1,000
embeddings as queries.

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"

python3 -m venv .venv-vector-bench
source .venv-vector-bench/bin/activate
pip install -U pip
pip install datasets numpy faiss-cpu

mkdir -p "$DATA_ROOT/wikipedia-bge-m3/en"

DATA_ROOT="$DATA_ROOT" python3 - <<'PY'
import os
from pathlib import Path
import numpy as np
import faiss
from datasets import load_dataset

OUT = Path(os.environ["DATA_ROOT"]) / "wikipedia-bge-m3" / "en"
BASE_COUNT = 1_000_000
QUERY_COUNT = 1_000
TOPK = 100

def write_fvecs(path: Path, arr: np.ndarray) -> None:
    arr = np.asarray(arr, dtype=np.float32)
    n, d = arr.shape
    out = np.memmap(
        path,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<f4", (d,))]),
        mode="w+",
        shape=(n,),
    )
    out["dim"][:] = d
    out["vec"][:] = arr
    out.flush()

def write_ivecs(path: Path, arr: np.ndarray) -> None:
    arr = np.asarray(arr, dtype=np.int32)
    n, d = arr.shape
    out = np.memmap(
        path,
        dtype=np.dtype([("dim", "<i4"), ("vec", "<i4", (d,))]),
        mode="w+",
        shape=(n,),
    )
    out["dim"][:] = d
    out["vec"][:] = arr
    out.flush()

dataset = load_dataset(
    "Upstash/wikipedia-2024-06-bge-m3",
    "en",
    split="train",
    streaming=True,
)

base = []
queries = []
for row in dataset:
    vec = np.asarray(row["embedding"], dtype=np.float32)
    if len(base) < BASE_COUNT:
        base.append(vec)
    elif len(queries) < QUERY_COUNT:
        queries.append(vec)
    else:
        break

base = np.vstack(base)
queries = np.vstack(queries)

index = faiss.IndexFlatIP(base.shape[1])
index.add(base)
_, gt = index.search(queries, TOPK)

write_fvecs(OUT / "base.fvecs", base)
write_fvecs(OUT / "query.fvecs", queries)
write_ivecs(OUT / "groundtruth.ivecs", gt)
PY
```

Then run:

```bash
cargo run -p vector-bench --release -- --config bench.toml
```

with:

```toml
[[params.recall]]
dataset = "wikipedia_bge_m3_en"
```

### Cohere1M

1M vectors, 768 dimensions, cosine distance. Uses Cohere's `embed-english-v3.0` embeddings from
the [Cohere Wikipedia dataset](https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3).

```bash
DATA_ROOT="${DATA_ROOT:-vector/bench/data}"
mkdir -p "$DATA_ROOT/cohere"
# Download and convert the dataset to fvecs format under "$DATA_ROOT/cohere":
#   cohere_base.fvecs        — 1M base vectors
#   cohere_query.fvecs       — query vectors
#   cohere_groundtruth.ivecs — ground truth nearest neighbors
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
| `num_queries`       | usize  | Number of queries to run (default: 100)                            |
| `query_concurrency` | usize  | Concurrent in-flight queries during the warm query phase (default: 8) |
| `query_qps_limit`   | usize  | Rate cap on warm-phase query submissions, in QPS (default: 32)     |
| `block_cache_bytes` | u64    | Block cache size in bytes (default: none)                          |
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
