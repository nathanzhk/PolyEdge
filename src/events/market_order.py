from dataclasses import dataclass, field
from time import perf_counter_ns

from enums import MarketOrderStatus, MarketOrderType, Side
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class MarketOrderEvent:
    event_source: str
    exch_ts_ms: int

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

    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def unmatched_shares(self) -> float:
        return (
            round(self.shares - self.matched_shares, 6)
            if self.shares > self.matched_shares
            else 0.0
        )

    @property
    def is_active(self) -> bool:
        return self.status == MarketOrderStatus.LIVE or self.status == MarketOrderStatus.MATCHED

    @property
    def is_inactive(self) -> bool:
        return not self.is_active
