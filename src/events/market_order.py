from dataclasses import dataclass, field
from time import perf_counter_ns

from enums import MarketOrderStatus, OrderType, Side
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class MarketOrderEvent:
    exch_ts_ms: int
    market_id: str
    token_id: str
    order_id: str
    trade_ids: list[str]
    status: MarketOrderStatus
    ordered_shares: float
    matched_shares: float
    side: Side
    type: OrderType
    price: float
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def pending_shares(self) -> float:
        return round(self.ordered_shares - self.matched_shares, 6)

    @property
    def is_canceled(self) -> bool:
        return self.status in {
            MarketOrderStatus.INVALID,
            MarketOrderStatus.CANCELED,
            MarketOrderStatus.CANCELED_MARKET_RESOLVED,
        }
