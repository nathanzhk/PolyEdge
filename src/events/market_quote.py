from dataclasses import dataclass, field
from time import perf_counter_ns

from infra.time import now_ts_ms
from markets.base import Market, Token


@dataclass(slots=True, frozen=True)
class MarketQuoteEvent:
    exch_ts_ms: int
    market: Market
    token: Token
    best_bid: float
    best_ask: float
    recv_ts_ms: int = field(default_factory=now_ts_ms)
    recv_mono_ns: int = field(default_factory=perf_counter_ns)
