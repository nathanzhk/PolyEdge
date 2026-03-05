from dataclasses import dataclass
from enum import StrEnum
from warnings import deprecated

from utils.enum import MarketOrderStatus

_ORDER_INVALID_STATUSES: set[MarketOrderStatus] = {
    MarketOrderStatus.INVALID,
    MarketOrderStatus.CANCELED,
    MarketOrderStatus.CANCELED_MARKET_RESOLVED,
}


@deprecated("")
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
    trade_ids: list[str]
    status: MarketOrderStatus
    ordered_shares: float
    matched_shares: float

    @property
    def pending_shares(self) -> float:
        return round(self.ordered_shares - self.matched_shares, 6)

    @property
    def derived_status(self) -> MarketOrderEventStatus:
        if self.matched_shares > 0:
            return MarketOrderEventStatus.MATCHED
        if self.cancelled:
            return MarketOrderEventStatus.INVALID
        return MarketOrderEventStatus.PENDING

    @property
    def cancelled(self) -> bool:
        return self.status in _ORDER_INVALID_STATUSES
