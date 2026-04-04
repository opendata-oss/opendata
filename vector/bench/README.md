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

```bash
# Run with default settings (in-memory storage, sift1M):
cargo run -p vector-bench --release

# Run with a config file:
cargo run -p vector-bench --release -- --config bench.toml

# Skip cleanup to inspect the database after the run:
cargo run -p vector-bench --release -- --no-cleanup
```

### Skipping ingestion

Set `VECTOR_BENCH_SKIP_INGEST=1` to skip the ingest phase and query an existing database. This requires persistent
storage (SlateDB) so the database survives across runs, and `--no-cleanup` on the initial ingest run.

```bash
# First run: ingest and keep the data
cargo run -p vector-bench --release -- --config bench.toml --no-cleanup

# Subsequent runs: query only
VECTOR_BENCH_SKIP_INGEST=1 cargo run -p vector-bench --release -- --config bench.toml --no-cleanup
```

## Datasets

By default, the benchmark looks for dataset files under a data directory resolved from the crate's manifest path (
`vector/bench/data/`). This can be overridden per-dataset with the `data_dir` parameter in the config file.

### SIFT1M

1M vectors, 128 dimensions, L2 distance. From the [ANN Benchmarks SIFT1M dataset](http://corpus-texmex.irisa.fr/).

```bash
mkdir -p vector/bench/data/sift
cd vector/bench/data/sift
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
tar xzf sift.tar.gz --strip-components=1
# Expected files: sift_base.fvecs, sift_query.fvecs, sift_groundtruth.ivecs
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

The benchmark reads `bvecs` directly, so unlike DEEP you do **not** need to convert the base/query files.

Expected layout:

```text
vector/bench/data/bigann/
├── bigann_base.bvecs
├── bigann_query.bvecs
├── bigann_groundtruth_10M.ivecs
├── bigann_groundtruth_50M.ivecs
├── bigann_groundtruth_100M.ivecs
└── bigann_groundtruth_1B.ivecs
```

Run these commands from the **workspace root**.

```bash
mkdir -p vector/bench/data/bigann

curl -L -o vector/bench/data/bigann/bigann_base.bvecs.gz \
  ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz

curl -L -o vector/bench/data/bigann/bigann_query.bvecs.gz \
  ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs.gz

curl -L -o vector/bench/data/bigann/bigann_gnd.tar.gz \
  ftp://ftp.irisa.fr/local/texmex/corpus/bigann_gnd.tar.gz

gzip -dc vector/bench/data/bigann/bigann_base.bvecs.gz \
  > vector/bench/data/bigann/bigann_base.bvecs

gzip -dc vector/bench/data/bigann/bigann_query.bvecs.gz \
  > vector/bench/data/bigann/bigann_query.bvecs

mkdir -p /tmp/bigann_gnd_extract
tar xzf vector/bench/data/bigann/bigann_gnd.tar.gz -C /tmp/bigann_gnd_extract

cp /tmp/bigann_gnd_extract/gnd/idx_10M.ivecs \
  vector/bench/data/bigann/bigann_groundtruth_10M.ivecs

cp /tmp/bigann_gnd_extract/gnd/idx_50M.ivecs \
  vector/bench/data/bigann/bigann_groundtruth_50M.ivecs

cp /tmp/bigann_gnd_extract/gnd/idx_100M.ivecs \
  vector/bench/data/bigann/bigann_groundtruth_100M.ivecs

cp /tmp/bigann_gnd_extract/gnd/idx_1000M.ivecs \
  vector/bench/data/bigann/bigann_groundtruth_1B.ivecs

ls -lh vector/bench/data/bigann
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
- BIGANN is large. `bigann_base.bvecs.gz` is about 91 GB compressed on IRISA and expands substantially when decompressed.

### DEEP10M and DEEP1B

96 dimensions, L2 distance, `fvecs` format. These benchmark entries expect local DEEP vectors converted into the same
`fvecs` / `ivecs` file layout as the other benchmarks.

Create this directory layout under `vector/bench/data/deep/`:

```text
vector/bench/data/deep/
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

Run these commands from the **workspace root**. Do not `cd` into `vector/bench/data/deep` first.

This setup intentionally uses:

- `base.10M.fbin` from Yandex for the base vectors
- `query.public.10K.fbin` from Yandex for the queries
- `gen_deep_groundtruth` to generate the ground truth locally

That avoids relying on a questionable prepublished `deep10m` ground-truth pairing.

```bash
mkdir -p vector/bench/data/deep

curl -L -o vector/bench/data/deep/base.10M.fbin \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin

curl -L -o vector/bench/data/deep/query.public.10K.fbin \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin

python3 - <<'PY'
from pathlib import Path
import numpy as np

root = Path("vector/bench/data/deep")

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
  --base-fbin vector/bench/data/deep/base.10M.fbin \
  --query-fbin vector/bench/data/deep/query.public.10K.fbin \
  --output-ivecs vector/bench/data/deep/deep_groundtruth_10M.ivecs \
  --top-k 100 \
  --distance-metric l2

ls -lh vector/bench/data/deep
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

Run these commands from the **workspace root**. `base.1B.fbin` alone is around 388 GB.

This setup intentionally uses:

- `base.1B.fbin` from Yandex for the base vectors
- `query.public.10K.fbin` from Yandex for the queries
- `groundtruth.public.10K.ibin` from Yandex for the ground truth

```bash
mkdir -p vector/bench/data/deep

curl -L -o vector/bench/data/deep/base.1B.fbin \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.1B.fbin

curl -L -o vector/bench/data/deep/query.public.10K.fbin \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin

curl -L -o vector/bench/data/deep/groundtruth.public.10K.ibin \
  https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/groundtruth.public.10K.ibin

python3 - <<'PY'
from pathlib import Path
import numpy as np

root = Path("vector/bench/data/deep")

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

ls -lh vector/bench/data/deep
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

Expected local layout:

```text
vector/bench/data/wikipedia-bge-m3/en/
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
python3 -m venv .venv-vector-bench
source .venv-vector-bench/bin/activate
pip install -U pip
pip install datasets numpy faiss-cpu

mkdir -p vector/bench/data/wikipedia-bge-m3/en

python3 - <<'PY'
from pathlib import Path
import numpy as np
import faiss
from datasets import load_dataset

OUT = Path("vector/bench/data/wikipedia-bge-m3/en")
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
mkdir -p vector/bench/data/cohere
# Download and convert the dataset to fvecs format:
#   cohere_base.fvecs   — 1M base vectors
#   cohere_query.fvecs  — query vectors
#   cohere_groundtruth.ivecs — ground truth nearest neighbors
```

### BigANN / SIFT1B

128 dimensions, L2 distance, bvecs format. From the [BigANN Benchmark](http://big-ann-benchmarks.com/). Several subsets
are available — all share the same base and query files but use different ground truth files and vector counts.

| Dataset  | Vectors | Ground truth file               |
|----------|---------|---------------------------------|
| sift10m  | 10M     | `bigann_groundtruth_10M.ivecs`  |
| sift50m  | 50M     | `bigann_groundtruth_50M.ivecs`  |
| sift100m | 100M    | `bigann_groundtruth_100M.ivecs` |
| sift1b   | 1B      | `bigann_groundtruth_1B.ivecs`   |

```bash
mkdir -p vector/bench/data/bigann
cd vector/bench/data/bigann

# Base vectors (~128 GB for the full 1B set):
wget https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin.gz
# Convert to bvecs format and rename to bigann_base.bvecs

# Query vectors:
wget https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin
# Convert to bvecs format and rename to bigann_query.bvecs

# Ground truth files (generate with brute-force search for each subset size):
#   bigann_groundtruth_10M.ivecs
#   bigann_groundtruth_50M.ivecs
#   bigann_groundtruth_100M.ivecs
#   bigann_groundtruth_1B.ivecs
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
| `dataset`           | string | Dataset name (`sift1m`, `cohere1m`, `deep10m`, `deep1b`, `wikipedia_bge_m3_en`, `sift10m`, etc.) |
| `dimensions`        | u16    | Vector dimensions (default: from dataset)                          |
| `distance_metric`   | string | `l2`, `cosine`, or `dot_product` (default: from dataset)           |
| `split_threshold`   | usize  | Centroid split threshold (default: from dataset)                   |
| `merge_threshold`   | usize  | Centroid merge threshold (default: from dataset)                   |
| `nprobe`            | usize  | Number of centroids to probe at query time (default: from dataset) |
| `num_queries`       | usize  | Number of queries to run (default: 100)                            |
| `block_cache_bytes` | u64    | Block cache size in bytes (default: none)                          |
| `data_dir`          | string | Directory containing dataset files (default: `vector/bench/data/`) |
| `vector_config`     | string | Path to a YAML file with vector `Config` overrides                 |

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
