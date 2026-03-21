from __future__ import annotations

import asyncio

from events import CryptoOHLCVEvent, CryptoQuoteEvent, MarketQuoteEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import elapsed_ms_since

logger = get_logger("STATE")

_MAX_BEAT_OFFSET_MS = 200


class RuntimeState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        self._yes_token: Token
        self._no_token: Token
        self._market: Market | None = None

        self._yes_quote: MarketQuoteEvent | None = None
        self._no_quote: MarketQuoteEvent | None = None

        self._crypto_quote: CryptoQuoteEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

        self._beat_price: float | None = None
        self._beat_offset_ms: int | None = None

    async def update_market_quote(self, quote: MarketQuoteEvent) -> None:
        async with self._lock:
            if self._market is None or quote.market.id != self._market.id:
                self._switch_market(quote.market)
            if quote.token.id == self._yes_token.id:
                self._yes_quote = quote
            if quote.token.id == self._no_token.id:
                self._no_quote = quote
        logger.debug(
            "latency=%.2fms outcome=%s best_bid=%.2f best_ask=%.2f",
            elapsed_ms_since(quote.recv_mono_ns),
            quote.token.key,
            quote.best_bid,
            quote.best_ask,
        )

    async def update_crypto_quote(self, quote: CryptoQuoteEvent) -> None:
        async with self._lock:
            self._crypto_quote = quote
            self._record_beat_price(quote)
        if self._beat_price is not None:
            logger.debug(
                "latency=%.2fms symbol=%s best_bid=%.2f best_ask=%.2f diff=%+.2f",
                elapsed_ms_since(self._crypto_quote.recv_mono_ns),
                self._crypto_quote.symbol,
                self._crypto_quote.best_bid,
                self._crypto_quote.best_ask,
                (self._crypto_quote.best_bid + self._crypto_quote.best_ask) / 2 - self._beat_price,
            )

    async def update_crypto_ohlcv(self, ohlcv: CryptoOHLCVEvent) -> None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv
        logger.debug(
            "latency=%.2fms open=%.2f high=%.2f low=%.2f close=%.2f volume=%.2f",
            elapsed_ms_since(self._crypto_ohlcv.recv_mono_ns),
            self._crypto_ohlcv.open,
            self._crypto_ohlcv.high,
            self._crypto_ohlcv.low,
            self._crypto_ohlcv.close,
            self._crypto_ohlcv.volume,
        )

    def _switch_market(self, market: Market) -> None:
        self._yes_token = market.yes_token
        self._no_token = market.no_token
        self._market = market
        self._beat_price = None
        self._beat_offset_ms = None
        logger.info("new market %s", market.title)

    def _record_beat_price(self, quote: CryptoQuoteEvent) -> None:
        if self._market is None:
            return
        beat_offset_ms = quote.recv_ts_ms - self._market.start_ts_ms
        beat_offset_abs_ms = abs(beat_offset_ms)
        if beat_offset_abs_ms > _MAX_BEAT_OFFSET_MS:
            return
        if self._beat_offset_ms is None or beat_offset_abs_ms < abs(self._beat_offset_ms):
            self._beat_price = round((quote.best_bid + quote.best_ask) / 2, 3)
            self._beat_offset_ms = beat_offset_ms
            logger.info("beat=%.2f offset=%dms", self._beat_price, beat_offset_ms)
