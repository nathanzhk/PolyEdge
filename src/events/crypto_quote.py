from dataclasses import dataclass, field
from time import perf_counter_ns

from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class CryptoQuoteEvent:
    exch_ut_id: int
    symbol: str
    best_bid: float
    best_ask: float
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)
