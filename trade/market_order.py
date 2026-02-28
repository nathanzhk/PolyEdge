from dataclasses import dataclass

from trade.enum import MarketOrderStatus, OrderType, Side


@dataclass(slots=True, frozen=True)
class MarketOrder:
    id: str
    side: Side
    type: OrderType
    status: MarketOrderStatus
    ordered_shares: float
    matched_shares: float
    market_id: str
    token_id: str
    price: float
