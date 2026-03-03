from dataclasses import dataclass
from enum import StrEnum
from warnings import deprecated

from trade.enum import MarketTradeStatus


@deprecated("")
class MarketTradeEventStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


_TRADE_STATUS_MAP: dict[MarketTradeStatus, MarketTradeEventStatus] = {
    MarketTradeStatus.MATCHED: MarketTradeEventStatus.PENDING,
    MarketTradeStatus.MINED: MarketTradeEventStatus.PENDING,
    MarketTradeStatus.CONFIRMED: MarketTradeEventStatus.SUCCESS,
    MarketTradeStatus.RETRYING: MarketTradeEventStatus.PENDING,
    MarketTradeStatus.FAILED: MarketTradeEventStatus.FAILURE,
}


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_id: str
    raw_status: MarketTradeStatus
    shares: float

    @property
    def status(self) -> MarketTradeEventStatus:
        return _TRADE_STATUS_MAP[self.raw_status]
