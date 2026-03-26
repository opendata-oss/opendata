# Vector Benchmarks

Includes two benchmarks for the vector database:

- `recall` measures recall@k, query latency, and ingestion throughput after ingesting a dataset and comparing search
  results to precomputed ground truth nearest neighbors.
- `usearch` measures USEARCH index build throughput, approximate search recall, and query latency/QPS on Cohere
  vectors, using exhaustive search over the same subset as ground truth.

Reported metrics:

- **recall@10** — fraction of true top-10 neighbors returned
- **QPS** — queries per second
- **p50 / p90 / p99 latency** — query latency percentiles (microseconds)
- **ingest throughput** — vectors per second (when ingestion is not skipped)

## Running

```bash
# Run all benchmarks with default settings (in-memory storage, sift1M for recall):
cargo run -p vector-bench --release

# Run only the recall benchmark:
cargo run -p vector-bench --release -- recall

# Run only the USEARCH benchmark:
cargo run -p vector-bench --release -- usearch

# Equivalent explicit form:
cargo run -p vector-bench --release -- --benchmark recall

# Run with a config file:
cargo run -p vector-bench --release -- --config bench.toml

# Run the USEARCH benchmark on a custom Cohere subset:
cargo run -p vector-bench --release -- --config bench.toml usearch

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
4. **`[[params.usearch]]`** — (Optional) USEARCH benchmark parameter overrides. Each entry runs one Cohere subset size.

### Parameters

| Parameter           | Type   | Description                                                        |
|---------------------|--------|--------------------------------------------------------------------|
| `dataset`           | string | Dataset name (`sift1m`, `cohere1m`, `sift10m`, etc.)               |
| `dimensions`        | u16    | Vector dimensions (default: from dataset)                          |
| `distance_metric`   | string | `l2`, `cosine`, or `dot_product` (default: from dataset)           |
| `split_threshold`   | usize  | Centroid split threshold (default: from dataset)                   |
| `merge_threshold`   | usize  | Centroid merge threshold (default: from dataset)                   |
| `nprobe`            | usize  | Number of centroids to probe at query time (default: from dataset) |
| `num_queries`       | usize  | Number of queries to run (default: 100)                            |
| `block_cache_bytes` | u64    | Block cache size in bytes (default: none)                          |
| `data_dir`          | string | Directory containing dataset files (default: `vector/bench/data/`) |
| `vector_config`     | string | Path to a YAML file with vector `Config` overrides                 |

USEARCH-specific params:

- `num_vectors`: number of Cohere base vectors to index.
- `num_queries`: number of Cohere query vectors to evaluate.
- `k`: nearest-neighbor cutoff used for both ANN search and recall.
- `parallel_inserts`: `"true"` or `"false"` to enable or disable USEARCH bulk-build parallel insertion.
- `data_dir`: parent directory containing `cohere/cohere_base.fvecs` and `cohere/cohere_query.fvecs`.
- `dimensions`: optional guard that must match the Cohere file dimensions when provided.

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
