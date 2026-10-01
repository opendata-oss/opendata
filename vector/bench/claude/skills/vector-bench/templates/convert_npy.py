#!/usr/bin/env python3
"""Convert a 2-D .npy of embeddings to a Parquet file in vector-bench format.

The output Parquet has one column (`--vector-column`, default `emb`) of type
`FixedSizeList<Float32, D>`, where D is inferred from the input array's
second dimension. No other columns are added — if you need metadata, write
your own Parquet directly.

Usage:
  convert_npy.py --input <path.npy> --output <path.parquet> [--vector-column emb]

Requires: pyarrow, numpy.
"""

import argparse
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True, help="path to input .npy")
    ap.add_argument("--output", required=True, help="path to output .parquet")
    ap.add_argument("--vector-column", default="emb", help="embedding column name")
    args = ap.parse_args()

    arr = np.load(args.input, mmap_mode="r")
    if arr.ndim != 2:
        print(
            f"error: expected a 2-D array (N x D), got shape {arr.shape}",
            file=sys.stderr,
        )
        return 2

    n, dim = arr.shape
    # Ensure float32, contiguous; cast cheaply if already.
    arr = np.ascontiguousarray(arr, dtype=np.float32)

    embeddings = pa.FixedSizeListArray.from_arrays(
        pa.array(arr.reshape(-1)),
        dim,
    )
    table = pa.table({args.vector_column: embeddings})
    pq.write_table(table, args.output)

    print(
        f"wrote {n} rows x {dim}-dim FixedSizeList<float32> to {args.output} "
        f"(column '{args.vector_column}')"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
