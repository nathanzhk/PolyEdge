"""Aggregate 1s OHLCV binance data into larger interval buckets."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent


def parse_interval(s: str) -> int:
    """Parse interval string like '3s', '5s', '1m' into milliseconds."""
    m = re.fullmatch(r"(\d+)(ms|s|m)", s)
    if not m:
        print(f"Invalid interval: {s}  (examples: 3s, 5s, 1m)")
        sys.exit(1)
    value, unit = int(m.group(1)), m.group(2)
    if unit == "ms":
        return value
    if unit == "s":
        return value * 1000
    return value * 60_000


def main():
    if len(sys.argv) != 3:
        print(
            """Usage: python interval-binance.py <date> <interval> 
            (e.g. python interval-binance.py 2026-04-07 3s)"""
        )
        sys.exit(1)

    date = sys.argv[1]
    interval_str = sys.argv[2]
    interval_ms = parse_interval(interval_str)

    src_path = DATA_DIR / f"{date}-1s-binance.json"
    if not src_path.exists():
        print(f"Source file not found: {src_path.name}")
        sys.exit(1)

    candles: list[dict] = json.loads(src_path.read_text())

    # Bucket by interval
    buckets: dict[int, list[dict]] = {}
    for c in candles:
        bucket_ts = int(c["timestamp"]) - (int(c["timestamp"]) % interval_ms)
        buckets.setdefault(bucket_ts, []).append(c)

    # Aggregate OHLCV per bucket
    rows: list[dict] = []
    for bucket_ts in sorted(buckets):
        group = buckets[bucket_ts]
        rows.append(
            {
                "timestamp": bucket_ts,
                "open": group[0]["open"],
                "high": max(c["high"] for c in group),
                "low": min(c["low"] for c in group),
                "close": group[-1]["close"],
                "volume": round(sum(c["volume"] for c in group), 8),
            }
        )

    out_path = DATA_DIR / f"{date}-{interval_str}-binance.json"
    out_path.write_text(json.dumps(rows, indent=2))
    print(f"Written {len(rows)} rows to {out_path.name}")


if __name__ == "__main__":
    main()
