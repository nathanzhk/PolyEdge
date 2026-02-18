from dataclasses import dataclass
from enum import StrEnum


class MarketTradeEventStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    INVALID = "INVALID"


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_id: str
    raw_status: str
    status: MarketTradeEventStatus
    shares: float
