from dataclasses import dataclass, field
from time import perf_counter_ns

from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class CryptoOHLCVEvent:
    exch_ts_ms: int
    symbol: str
    start_ts_ms: int
    close_ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)
