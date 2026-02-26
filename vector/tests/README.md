# Recall Integration Tests

Integration tests that measure ANN search recall against standard SIFT benchmark datasets.

## Tests

### `sift100k_recall`

Runs automatically with `cargo test`. Ingests 100K 128-dimensional vectors and measures
recall\@10 against brute-force ground truth.

**Data location**: `tests/data/sift100k/`

The test data is checked into the repo as `sift100k.tgz`. The test automatically extracts
it on first run. The tarball contains:

| File | Description |
|---|---|
| `base.fvecs` | 100K base vectors (128-d, float32) |
| `query.fvecs` | 1000 query vectors (128-d, float32) |
| `groundtruth.ivecs` | Top-100 nearest neighbors per query (positional IDs) |

#### How the sift100k data is generated

The sift100k dataset is derived from SIFT1M by randomly sampling 100K vectors and recomputing
ground truth via brute-force over the sampled subset. Ground truth IDs are positional
indices into `base.fvecs` (0, 1, 2, ...).

To regenerate (requires the SIFT1M dataset, see below):

```bash
cargo run -p opendata-vector --release --bin gen_sift100k_groundtruth
```

This reads from `tests/data/sift/` and writes to `tests/data/sift100k/`. After regenerating,
update the tarball:

```bash
cd vector/tests/data/sift100k
tar czf sift100k.tgz base.fvecs query.fvecs groundtruth.ivecs
```

### `sift1m_recall`

Marked `#[ignore]` — requires the full SIFT1M dataset (~500 MB) which is not checked into the
repo. Ingests all 1M vectors and measures recall with 10K queries.

```bash
cargo test -p opendata-vector --test sift1m -- sift1m_recall --ignored --nocapture
```

**Data location**: `tests/data/sift/`

| File | Description |
|---|---|
| `sift_base.fvecs` | 1M base vectors (128-d, float32) |
| `sift_query.fvecs` | 10K query vectors (128-d, float32) |
| `sift_groundtruth.ivecs` | Top-100 nearest neighbors per query |

#### How to get the SIFT1M dataset

Download from the [TEXMEX ANN benchmarks](http://corpus-texmex.irisa.fr/):

```bash
cd vector/tests/data
mkdir -p sift && cd sift
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
tar xzf sift.tar.gz --strip-components=1
```

This extracts `sift_base.fvecs` (512 MB), `sift_query.fvecs` (5 MB),
`sift_groundtruth.ivecs` (4 MB), and `sift_learn.fvecs` (64 MB, unused).

## File formats

Both tests use standard ANN benchmark file formats:

- **fvecs**: Each vector is stored as `[dim: u32][values: f32 x dim]` in little-endian.
- **ivecs**: Same layout but with `i32` values instead of `f32`.
