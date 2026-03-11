from dataclasses import dataclass

from domain import MarketOrderStatus, Side


@dataclass(slots=True, frozen=True)
class MarketOrderEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_ids: list[str]
    side: Side
    status: MarketOrderStatus
    ordered_shares: float
    matched_shares: float

    @property
    def pending_shares(self) -> float:
        return round(self.ordered_shares - self.matched_shares, 6)

    @property
    def cancelled(self) -> bool:
        return self.status in {
            MarketOrderStatus.INVALID,
            MarketOrderStatus.CANCELED,
            MarketOrderStatus.CANCELED_MARKET_RESOLVED,
        }
