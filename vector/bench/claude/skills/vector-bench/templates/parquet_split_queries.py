#!/usr/bin/env python3
"""Split a single Parquet file into a base file (first M rows) and a query file
(last N rows).

Use when the user has one Parquet of embeddings but no separate held-out
queries. The last `--num-queries` rows become queries, the rest become base.
All columns are preserved (so metadata columns travel to both files, and the
query file's filter-binding columns are available downstream).

Usage:
  parquet_split_queries.py --input <path.parquet> \\
      --base <out-base.parquet> --query <out-query.parquet> \\
      [--num-queries 1000]

Requires: pyarrow.
"""

import argparse
import sys

import pyarrow.parquet as pq


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--input", required=True, help="input parquet")
    ap.add_argument("--base", required=True, help="output base parquet (first rows)")
    ap.add_argument("--query", required=True, help="output query parquet (last rows)")
    ap.add_argument(
        "--num-queries",
        type=int,
        default=1000,
        help="number of trailing rows to use as queries (default 1000)",
    )
    args = ap.parse_args()

    table = pq.read_table(args.input)
    total = table.num_rows
    nq = args.num_queries
    if nq <= 0 or nq >= total:
        print(
            f"error: --num-queries must be in 1..{total - 1}, got {nq}",
            file=sys.stderr,
        )
        return 2

    base = table.slice(0, total - nq)
    query = table.slice(total - nq, nq)
    pq.write_table(base, args.base)
    pq.write_table(query, args.query)

    print(
        f"split: {base.num_rows} base + {query.num_rows} query rows "
        f"({total} total, schema preserved)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
