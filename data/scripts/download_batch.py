"""Batch download and aggregate data, then delete raw orderbooks to save space."""

from __future__ import annotations

import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPTS_DIR.parent
PROJECT_ROOT = DATA_DIR.parent


def dates_in_range(start: str, end: str) -> list[str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out = []
    while s <= e:
        out.append(s.isoformat())
        s += timedelta(days=1)
    return out


def main():
    if len(sys.argv) != 3:
        print("Usage: python backtests/batch_download.py <start-date> <end-date>")
        print("Example: python backtests/batch_download.py 2026-03-09 2026-04-07")
        sys.exit(1)

    start, end = sys.argv[1], sys.argv[2]
    all_dates = dates_in_range(start, end)
    print(f"Processing {len(all_dates)} dates: {all_dates[0]} to {all_dates[-1]}\n")

    for i, d in enumerate(all_dates, 1):
        binance_file = DATA_DIR / f"{d}-1s-binance.json"
        market_file = DATA_DIR / f"{d}-1s-market.json"

        if binance_file.exists() and market_file.exists():
            print(f"[{i}/{len(all_dates)}] {d} — already exists, skipping")
            continue

        print(f"[{i}/{len(all_dates)}] {d} — downloading...")

        env = {**__import__("os").environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}

        # Download binance
        if not binance_file.exists():
            r = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "download_binance.py"), d],
                capture_output=True,
                text=True,
                env=env,
            )
            if r.returncode != 0:
                print(f"  WARN: binance download failed: {r.stderr.strip()}")
                continue
            print("  binance OK")

        # Download market (also auto-aggregates to 1s)
        if not market_file.exists():
            r = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "download_market.py"), d],
                capture_output=True,
                text=True,
                env=env,
            )
            if r.returncode != 0:
                print(f"  WARN: market download failed: {r.stderr.strip()}")
                continue
            print("  market OK")

        # Delete raw orderbook directory
        raw_dir = DATA_DIR / d
        if raw_dir.exists():
            shutil.rmtree(raw_dir)
            print(f"  deleted raw data ({d}/)")

    print("\nDone.")


if __name__ == "__main__":
    main()
