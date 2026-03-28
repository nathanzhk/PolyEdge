"""Download polybacktest orderbook data for a given date, then aggregate to 1s."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from backtest_rest import get_orderbook_by_slug
from interval_market import aggregate

from utils.env import Env

MARKET = ZoneInfo("America/New_York")


def generate_slugs(backtest_date: str) -> list[dict]:
    """Generate BTC 5-min market slugs for 08:00–18:00 ET."""
    dt = datetime.strptime(backtest_date, "%Y-%m-%d")
    start_dt = datetime(dt.year, dt.month, dt.day, 8, tzinfo=MARKET)
    end_dt = datetime(dt.year, dt.month, dt.day, 18, tzinfo=MARKET)

    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())

    markets = []
    for epoch in range(start_epoch, end_epoch, 300):
        markets.append({"slug": f"btc-updown-5m-{epoch}", "start_ts": epoch, "end_ts": epoch + 300})
    return markets


def _download_one(args: tuple) -> str:
    slug, out_dir = args
    out_file = out_dir / f"{slug}.json"
    if out_file.exists():
        return slug
    snapshots = get_orderbook_by_slug(slug)
    out_file.write_text(json.dumps(snapshots, indent=2))
    return slug


def main():
    if len(sys.argv) != 2:
        print("Usage: python backtests/download_market.py 2026-04-06")
        sys.exit(1)

    Env.load()
    date_str = sys.argv[1]
    markets = generate_slugs(date_str)
    total = len(markets)

    out_dir = Path(__file__).resolve().parents[1] / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_download_one, (m["slug"], out_dir)): m["slug"] for m in markets}
        for future in as_completed(futures):
            done += 1
            slug = futures[future]
            future.result()
            print(f"  [{done}/{total}] {slug}")

    print(f"Done. {total} markets saved to {out_dir}")

    print("Aggregating to 1s...")
    aggregate(date_str, "1s")


if __name__ == "__main__":
    main()
