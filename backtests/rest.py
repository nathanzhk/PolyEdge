"""polybacktest.com API client."""

from __future__ import annotations

import time

import requests

from infra import Env, iso_to_ms

_remaining = -1  # X-RateLimit-Remaining from last response


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {Env.POLY_BACKTEST_KEY}"}


def _get(url: str, params: dict | None = None) -> dict | None:
    global _remaining
    while True:
        if _remaining == 0:
            time.sleep(1)
        response = requests.get(url, headers=_auth(), params=params, timeout=5)
        _remaining = int(response.headers["X-RateLimit-Remaining"])
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429:
            wait = float(response.headers.get("Retry-After", 0)) + 1
            time.sleep(wait)
            continue
        return None


def get_market_by_slug(slug: str, coin: str = "btc") -> dict | None:
    """Look up a market by slug.

    Returns the market object with market_id, condition_id, winner,
    start_time, end_time, etc.  Returns None on failure.
    """
    market = _get(
        f"{Env.POLY_BACKTEST_BASE_URL}/markets/by-slug/{slug}",
        params={"coin": coin},
    )
    if not market:
        return None
    market["start_time"] = iso_to_ms(market["start_time"])
    market["end_time"] = iso_to_ms(market["end_time"])
    return market


def get_orderbook_by_slug(slug: str, coin: str = "btc") -> list[dict]:
    """Look up a market by slug, then fetch all its orderbook snapshots.

    Returns (market, snapshots) or None if the market lookup fails.
    """
    market = get_market_by_slug(slug, coin)
    if not market:
        return []
    return _get_orderbook_by_market(market["market_id"], coin)


def _get_orderbook_by_market(market_id: int | str, coin: str) -> list[dict]:
    """Fetch all orderbook snapshots for a market (handles pagination).

    Returns the full list of snapshot dicts, or [] on failure.
    """
    all_snaps: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        page_data = _get(
            f"{Env.POLY_BACKTEST_BASE_URL}/markets/{market_id}/snapshots",
            params={
                "coin": coin,
                "include_orderbook": "true",
                "offset": offset,
                "limit": page_size,
            },
        )
        if not page_data or "snapshots" not in page_data:
            break
        page_snaps = page_data["snapshots"]
        all_snaps.extend(page_snaps)
        if len(page_snaps) < page_size:
            break
        offset += page_size
    for snap in all_snaps:
        snap["timestamp"] = iso_to_ms(snap["time"])
        up_book = snap.get("orderbook_up", {}) or {}
        down_book = snap.get("orderbook_down", {}) or {}
        snap["bid_up"] = _best_price(up_book.get("bids", []), "bid")
        snap["ask_up"] = _best_price(up_book.get("asks", []), "ask")
        snap["bid_down"] = _best_price(down_book.get("bids", []), "bid")
        snap["ask_down"] = _best_price(down_book.get("asks", []), "ask")
    return all_snaps


def _best_price(levels: list[dict], side: str = "bid") -> float:
    """Return the best (top-of-book) price from a sorted level list.

    Sorts by price, then returns highest for bids, lowest for asks.
    Handles both old (low→high) and new (high→low) API orderings.
    """
    if not levels:
        return 0.0
    sorted_levels = sorted(levels, key=lambda level: float(level.get("price") or 0))
    if side == "bid":
        return float(sorted_levels[-1].get("price") or 0)
    return float(sorted_levels[0].get("price") or 0)
