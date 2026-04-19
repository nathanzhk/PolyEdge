from dataclasses import dataclass, field
from time import perf_counter_ns

from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class CryptoQuoteEvent:
    exch_ts_ms: int
    symbol: str
    best_bid: float
    best_ask: float
    baseline: float | None = None
    change: float | None = None
    price: float | None = None
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)

    @property
    def mid(self) -> float:
        if self.price is not None:
            return round(self.price, 3)
        return round((self.best_bid + self.best_ask) / 2, 3)
