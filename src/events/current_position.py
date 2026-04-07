from __future__ import annotations

from dataclasses import dataclass

from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class CurrentPositionEvent:
    token: Token
    market: Market
    opening_shares: float
    open_settling_shares: float
    closing_shares: float
    close_settling_shares: float
    holding_cost: float
    holding_shares: float
    holding_avg_price: float
    realized_pnl: float = 0.0

    @property
    def effective_shares(self) -> float:
        """Position size after all pending and settling orders finish."""
        return round(
            self.opening_shares
            + self.open_settling_shares
            + self.holding_shares
            - self.closing_shares
            - self.close_settling_shares,
            6,
        )

    @property
    def sellable_shares(self) -> float:
        """Held shares that are not already committed to pending or settling sells."""
        return max(
            round(
                self.holding_shares - self.closing_shares - self.close_settling_shares,
                6,
            ),
            0.0,
        )

    @property
    def is_active(self) -> bool:
        return (
            self.opening_shares > 0
            or self.open_settling_shares > 0
            or self.closing_shares > 0
            or self.close_settling_shares > 0
            or self.holding_shares > 0
        )
