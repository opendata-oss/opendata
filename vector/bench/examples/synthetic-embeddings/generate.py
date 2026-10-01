#!/usr/bin/env python3
"""Generate the synthetic embeddings fixture committed alongside this script.

The output is a single Parquet file with three columns:
  - `embedding`: FixedSizeList<Float32, 64> — Gaussian random vectors
  - `category`: string — one of 5 categories
  - `score`:    float64 — uniform [0, 1)

5,000 rows by default. The output is deterministic (seeded RNG=42), so a
re-run produces byte-identical bits — i.e. you can regenerate and `git diff`
will be clean.

Usage:
  python3 generate.py [--rows 5000] [--dim 64] [--output embeddings.parquet]

Requires: pyarrow, numpy.
"""

import argparse
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

CATEGORIES = ["clothing", "electronics", "books", "home", "garden"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=5000, help="row count (default 5000)")
    ap.add_argument("--dim", type=int, default=64, help="embedding dimension (default 64)")
    ap.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "embeddings.parquet",
        help="output Parquet path",
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    n, dim = args.rows, args.dim

    embeddings = rng.standard_normal((n, dim)).astype(np.float32)
    categories = np.array(CATEGORIES)[rng.integers(0, len(CATEGORIES), n)]
    scores = rng.uniform(0.0, 1.0, n).astype(np.float64)

    table = pa.table(
        {
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(embeddings.reshape(-1)), dim
            ),
            "category": pa.array(categories),
            "score": pa.array(scores),
        }
    )
    pq.write_table(table, args.output)

    size = args.output.stat().st_size
    print(
        f"wrote {n} rows x {dim}-dim embeddings + category + score "
        f"to {args.output} ({size / 1024:.1f} KB)"
    )


if __name__ == "__main__":
    main()
