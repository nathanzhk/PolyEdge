from dataclasses import dataclass

from enums import MarketOrderStatus, MarketOrderType, Side


@dataclass(slots=True, frozen=True)
class MarketOrder:
    id: str
    side: Side
    type: MarketOrderType
    status: MarketOrderStatus
    ordered_shares: float
    matched_shares: float
    market_id: str
    token_id: str
    price: float

    @property
    def pending_shares(self) -> float:
        return round(self.ordered_shares - self.matched_shares, 6)
