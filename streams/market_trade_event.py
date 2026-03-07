from dataclasses import dataclass

from utils.enum import MarketTradeStatus


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_id: str
    status: MarketTradeStatus
    shares: float
