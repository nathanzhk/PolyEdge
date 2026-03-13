from dataclasses import dataclass

from domain.enums import MarketTradeStatus, Side


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_id: str
    side: Side
    status: MarketTradeStatus
    shares: float
    price: float
