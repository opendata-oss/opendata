#!/usr/bin/env python3
"""
Analyze object_store_call events from server-logs.jsonl.

Parses structured JSON logs and summarizes object-store activity,
grouped by request_id or query_hash.

Usage:
    python3 scripts/analyze_object_store_calls.py <server-logs.jsonl> [--query-hash HASH] [--request-id ID]
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


def parse_events(path: str):
    """Yield object_store_call events from a JSONL file."""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            fields = entry.get("fields", {})
            if fields.get("message") != "object_store_call":
                continue
            # Merge span context into fields for grouping.
            for span in entry.get("spans", []):
                if "request_id" in span:
                    fields["request_id"] = span["request_id"]
                if "query_hash" in span:
                    fields["query_hash"] = span["query_hash"]
            yield fields


def percentile(sorted_vals, p):
    """Return the p-th percentile from a sorted list."""
    if not sorted_vals:
        return 0
    idx = int(len(sorted_vals) * p / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def summarize(events: list):
    """Print a summary of object_store_call events."""
    if not events:
        print("No object_store_call events found.")
        return

    # Group by op.
    by_op = defaultdict(list)
    for e in events:
        by_op[e.get("op", "unknown")].append(e)

    print(f"\n{'='*70}")
    print(f"Total object_store_call events: {len(events)}")
    print(f"{'='*70}")

    # Summary by op.
    print(f"\n--- Call count and bytes by op ---")
    print(f"{'op':<25} {'count':>8} {'total_bytes':>14} {'avg_bytes':>12} {'total_ms':>10} {'avg_ms':>8}")
    print("-" * 80)
    for op in sorted(by_op.keys()):
        calls = by_op[op]
        count = len(calls)
        total_bytes = sum(e.get("bytes_returned", 0) for e in calls)
        total_ms = sum(e.get("duration_ms", 0) for e in calls)
        avg_bytes = total_bytes // count if count else 0
        avg_ms = total_ms / count if count else 0
        print(f"{op:<25} {count:>8} {total_bytes:>14} {avg_bytes:>12} {total_ms:>10} {avg_ms:>8.1f}")

    # Duration distribution by op.
    print(f"\n--- Duration distribution by op (ms) ---")
    print(f"{'op':<25} {'min':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}")
    print("-" * 66)
    for op in sorted(by_op.keys()):
        durations = sorted(e.get("duration_ms", 0) for e in by_op[op])
        print(
            f"{op:<25} {durations[0]:>8} {percentile(durations, 50):>8} "
            f"{percentile(durations, 95):>8} {percentile(durations, 99):>8} {durations[-1]:>8}"
        )

    # Top paths by bytes.
    path_bytes = defaultdict(int)
    path_count = defaultdict(int)
    for e in events:
        p = e.get("path", "")
        path_bytes[p] += e.get("bytes_returned", 0)
        path_count[p] += 1

    print(f"\n--- Top 10 paths by total bytes ---")
    print(f"{'path':<60} {'bytes':>14} {'calls':>8}")
    print("-" * 85)
    for p, b in sorted(path_bytes.items(), key=lambda x: -x[1])[:10]:
        print(f"{p:<60} {b:>14} {path_count[p]:>8}")

    # Top paths by call count.
    print(f"\n--- Top 10 paths by call count ---")
    print(f"{'path':<60} {'calls':>8} {'bytes':>14}")
    print("-" * 85)
    for p, c in sorted(path_count.items(), key=lambda x: -x[1])[:10]:
        print(f"{p:<60} {c:>8} {path_bytes[p]:>14}")

    # Largest individual responses.
    print(f"\n--- Top 10 largest individual responses ---")
    print(f"{'op':<25} {'path':<40} {'bytes':>14} {'ms':>8}")
    print("-" * 90)
    by_size = sorted(events, key=lambda e: -e.get("bytes_returned", 0))
    for e in by_size[:10]:
        op = e.get("op", "")
        path = e.get("path", "")
        if len(path) > 38:
            path = "..." + path[-35:]
        print(
            f"{op:<25} {path:<40} {e.get('bytes_returned', 0):>14} "
            f"{e.get('duration_ms', 0):>8}"
        )

    # Error summary.
    errors = [e for e in events if e.get("status") == "err"]
    if errors:
        print(f"\n--- Errors ({len(errors)}) ---")
        for e in errors:
            print(f"  op={e.get('op')} path={e.get('path')} error={e.get('error')}")


def main():
    parser = argparse.ArgumentParser(description="Analyze object_store_call events")
    parser.add_argument("logfile", help="Path to server-logs.jsonl")
    parser.add_argument("--query-hash", help="Filter by query_hash")
    parser.add_argument("--request-id", help="Filter by request_id")
    args = parser.parse_args()

    if not Path(args.logfile).exists():
        print(f"File not found: {args.logfile}", file=sys.stderr)
        sys.exit(1)

    events = list(parse_events(args.logfile))

    if args.query_hash:
        events = [e for e in events if e.get("query_hash") == args.query_hash]
    if args.request_id:
        events = [e for e in events if e.get("request_id") == args.request_id]

    # If no filter, show available query hashes.
    if not args.query_hash and not args.request_id:
        hashes = defaultdict(int)
        for e in events:
            h = e.get("query_hash", "none")
            hashes[h] += 1
        if len(hashes) > 1:
            print("Available query hashes (use --query-hash to filter):")
            for h, c in sorted(hashes.items(), key=lambda x: -x[1]):
                print(f"  {h}: {c} events")

    summarize(events)


if __name__ == "__main__":
    main()
