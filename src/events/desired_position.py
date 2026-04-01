from __future__ import annotations

from dataclasses import dataclass

from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class DesiredPositionEvent:
    market: Market
    token: Token
    shares: float
    best_bid: float
    best_ask: float
    force: bool = False
