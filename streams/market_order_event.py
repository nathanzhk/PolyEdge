from dataclasses import dataclass
from enum import StrEnum


class MarketOrderEventStatus(StrEnum):
    PENDING = "PENDING"
    MATCHED = "MATCHED"
    INVALID = "INVALID"


@dataclass(slots=True, frozen=True)
class MarketOrderEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    status: MarketOrderEventStatus
    raw_status: str
    ordered_shares: float
    pending_shares: float
    matched_shares: float
    full_matched: bool
    cancelled: bool
