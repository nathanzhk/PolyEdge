from __future__ import annotations

import asyncio

from events import CryptoOHLCVEvent, CryptoQuoteEvent, MarketQuoteEvent
from markets.base import Market
from utils.logger import get_logger

logger = get_logger("MARKET STATE")

_MAX_BEAT_OFFSET_MS = 200


class RuntimeState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market: Market | None = None
        self._beat_price: float | None = None
        self._beat_offset_ms: int | None = None
        self._market_quote: MarketQuoteEvent | None = None
        self._crypto_quote: CryptoQuoteEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

    async def update_market_quote(self, quote: MarketQuoteEvent) -> None:
        async with self._lock:
            self._market_quote = quote
            if self._market is None or self._market.id != quote.market.id:
                self._market = quote.market
                self._beat_price = None
                self._beat_offset_ms = None
        logger.debug(
            "Market Quote -> bid_Yes=%.2f ask_Yes=%.2f bid_No=%.2f ask_No=%.2f",
            quote.bid_yes,
            quote.ask_yes,
            quote.bid_no,
            quote.ask_no,
        )

    async def update_crypto_quote(self, quote: CryptoQuoteEvent) -> None:
        async with self._lock:
            self._crypto_quote = quote
            self._record_beat_price()
        if self._beat_price is None:
            logger.debug("Crypto Quote -> mid=%.2f", quote.mid)
        else:
            beat_diff = quote.mid - self._beat_price
            beat_pct = beat_diff / self._beat_price * 100
            logger.debug(
                "Crypto Quote -> mid=%.2f diff=%+.2f %+.3f%%",
                quote.mid,
                beat_diff,
                beat_pct,
            )

    async def update_crypto_ohlcv(self, ohlcv: CryptoOHLCVEvent) -> None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv

    def _record_beat_price(self) -> None:
        if self._market is None or self._crypto_quote is None:
            return
        beat_offset_ms = self._crypto_quote.ts_ms - self._market.start_ts_ms
        beat_offset_abs_ms = abs(beat_offset_ms)
        if beat_offset_abs_ms > _MAX_BEAT_OFFSET_MS:
            return
        should_update = self._beat_offset_ms is None or beat_offset_abs_ms < abs(
            self._beat_offset_ms
        )
        if not should_update:
            return
        self._beat_price = self._crypto_quote.mid
        self._beat_offset_ms = beat_offset_ms
        logger.info("Beat Price -> %.2f offset_ms=%+d", self._beat_price, beat_offset_ms)
