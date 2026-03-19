from __future__ import annotations

from dataclasses import dataclass

from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class CurrentPositionEvent:
    market: Market
    shares: float
    price: float
    token: Token
    opening_shares: float
    holding_shares: float
    closing_shares: float
    holding_avg_price: float | None = None
    holding_cost: float = 0.0
    holding_open_ts_ms: int | None = None
