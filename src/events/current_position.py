from __future__ import annotations

from dataclasses import dataclass

from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class CurrentPositionEvent:
    token: Token
    market: Market
    opening_shares: float
    holding_shares: float
    closing_shares: float
    holding_cost: float
    holding_avg_price: float
    realized_pnl: float = 0.0

    @property
    def is_active(self) -> bool:
        return self.opening_shares > 0 or self.holding_shares > 0 or self.closing_shares > 0
