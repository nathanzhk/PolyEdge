from dataclasses import dataclass, field
from time import perf_counter_ns

from enums import MarketTradeStatus, Role, Side
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class MarketTradeEvent:
    event_source: str
    exch_ts_ms: int

    market_id: str
    token_id: str
    order_id: str
    trade_id: str

    status: MarketTradeStatus
    shares: float

    side: Side
    role: Role
    price: float

    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)
