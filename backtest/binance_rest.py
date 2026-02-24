"""Binance API utilities — REST and WebSocket for BTC/USDT klines."""

from __future__ import annotations

import requests

_BASE_URL = "https://api.binance.com/api/v3"


def fetch_ohlcv(
    symbol: str = "BTCUSDT",
    interval: str = "1s",
    start_time: int = 0,
    end_time: int = 0,
) -> list[dict]:
    """Fetch klines from Binance REST API, paginating automatically."""
    rows: list[dict] = []
    limit: int = 1000
    cursor = start_time
    while cursor < end_time:
        response = requests.get(
            f"{_BASE_URL}/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_time,
                "limit": limit,
            },
            timeout=5,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        for raw in batch:
            rows.append(_parse_kline_rest(raw))
        if len(batch) < limit:
            break
        cursor = batch[-1][0] + 1
    return rows


def _parse_kline_rest(raw: list) -> dict:
    """Parse a single REST kline array into a dict."""
    return {
        "timestamp": raw[0],
        "open": float(raw[1]),
        "high": float(raw[2]),
        "low": float(raw[3]),
        "close": float(raw[4]),
        "volume": float(raw[5]),
    }
