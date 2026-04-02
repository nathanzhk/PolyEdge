from dataclasses import dataclass

from enums import MarketOrderStatus, MarketOrderType, Side


@dataclass(slots=True, frozen=True)
class MarketOrder:
    market_id: str
    token_id: str
    order_id: str
    trade_ids: list[str]
    status: MarketOrderStatus
    shares: float
    side: Side
    type: MarketOrderType
    price: float
    matched_shares: float

    @property
    def unmatched_shares(self) -> float:
        return round(self.shares - self.matched_shares, 6)
