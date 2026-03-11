"""Download Binance BTC/USDT 1s klines for a given date (08:00–18:00 ET)."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from binance_rest import fetch_ohlcv

MARKET = ZoneInfo("America/New_York")


def main():
    if len(sys.argv) != 2:
        print("Usage: python backtests/download_binance.py 2026-04-07")
        sys.exit(1)

    date_str = sys.argv[1]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    start_ms = int(datetime(dt.year, dt.month, dt.day, 8, tzinfo=MARKET).timestamp() * 1000)
    end_ms = int(datetime(dt.year, dt.month, dt.day, 18, tzinfo=MARKET).timestamp() * 1000)

    print(f"Fetching BTC/USDT 1s klines {date_str} 08:00–18:00 ET …")
    rows = fetch_ohlcv(start_time=start_ms, end_time=end_ms)

    out_path = Path(__file__).resolve().parent / f"{date_str}-1s-binance.json"
    out_path.write_text(json.dumps(rows, indent=2))
    print(f"Done. {len(rows)} rows saved to {out_path}")


if __name__ == "__main__":
    main()
