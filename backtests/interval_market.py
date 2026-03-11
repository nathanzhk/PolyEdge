"""Aggregate raw orderbook snapshots into fixed-interval buckets using mode."""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent


def parse_interval(s: str) -> int:
    """Parse interval string like '1s', '3s', '500ms' into milliseconds."""
    m = re.fullmatch(r"(\d+)(ms|s|m)", s)
    if not m:
        print(f"Invalid interval: {s}  (examples: 1s, 3s, 500ms)")
        sys.exit(1)
    value, unit = int(m.group(1)), m.group(2)
    if unit == "ms":
        return value
    if unit == "s":
        return value * 1000
    return value * 60_000


def aggregate(date: str, interval_str: str) -> None:
    interval_ms = parse_interval(interval_str)

    raw_dir = DATA_DIR / date
    files = sorted(raw_dir.glob("*.json"))
    if not files:
        print(f"No data files in {raw_dir}")
        sys.exit(1)

    # Load all snapshots
    all_snaps: list[dict] = []
    for f in files:
        slug = f.stem
        for snap in json.loads(f.read_text()):
            snap["slug"] = slug
            all_snaps.append(snap)

    all_snaps.sort(key=lambda s: s["timestamp"])

    # Bucket by (slug, interval)
    buckets: dict[tuple[str, int], list[dict]] = {}
    for snap in all_snaps:
        ts = snap["timestamp"]
        bucket_key = (snap["slug"], int(ts) - (int(ts) % interval_ms))
        buckets.setdefault(bucket_key, []).append(snap)

    # Aggregate using mode (most frequent price combo)
    rows: list[dict] = []
    for (slug, bucket_ts), snaps in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][1])):
        counts = Counter((s["bid_up"], s["ask_up"], s["bid_down"], s["ask_down"]) for s in snaps)
        best = counts.most_common(1)[0][0]
        rows.append(
            {
                "timestamp": bucket_ts,
                "slug": slug,
                "bid_up": best[0],
                "ask_up": best[1],
                "bid_down": best[2],
                "ask_down": best[3],
            }
        )

    out_path = DATA_DIR / f"{date}-{interval_str}-market.json"
    out_path.write_text(json.dumps(rows, indent=2))
    print(f"Written {len(rows)} rows to {out_path.name}")


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python backtests/interval_market.py <date> <interval>  "
            "(e.g. python backtests/interval_market.py 2026-04-07 1s)"
        )
        sys.exit(1)

    aggregate(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
