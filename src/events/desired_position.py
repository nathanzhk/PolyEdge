from __future__ import annotations

from dataclasses import dataclass

from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class DesiredPositionEvent:
    market: Market
    token: Token | None
    shares: float
    price: float
    not_urgent: bool
