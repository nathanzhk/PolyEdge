from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter_ns

from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_quote import CryptoQuoteEvent
from events.current_position import CurrentPositionEvent
from events.market_quote import MarketQuoteEvent
from markets.base import Market
from utils.time import now_ts_ms


@dataclass(slots=True, frozen=True)
class RuntimeStateEvent:
    reason: str
    market: Market
    yes_token_quote: MarketQuoteEvent
    no_token_quote: MarketQuoteEvent
    crypto_quote: CryptoQuoteEvent
    crypto_ohlcv: CryptoOHLCVEvent
    beat_price: float
    positions: tuple[CurrentPositionEvent, ...]
    event_ts_ms: int = field(default_factory=now_ts_ms)
    event_mono_ns: int = field(default_factory=perf_counter_ns)
