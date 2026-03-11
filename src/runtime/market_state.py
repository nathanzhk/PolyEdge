from __future__ import annotations

import asyncio

from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_price import CryptoPriceEvent
from events.market_price import MarketPriceEvent
from infra.logger import get_logger
from markets.base import Market
from strategies.context import MarketLatestState

logger = get_logger("MARKET STATE")

_MAX_BEAT_OFFSET_MS = 200


class MarketState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market: Market | None = None
        self._beat_price: float | None = None
        self._beat_offset_ms: int | None = None
        self._market_price: MarketPriceEvent | None = None
        self._crypto_price: CryptoPriceEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

    async def update_market_price(self, price: MarketPriceEvent) -> None:
        async with self._lock:
            self._market_price = price
            if self._market is None or self._market.id != price.market.id:
                self._market = price.market
                self._beat_price = None
                self._beat_offset_ms = None
        logger.debug(
            "Market Price -> bid_Yes=%.2f ask_Yes=%.2f bid_No=%.2f ask_No=%.2f",
            price.bid_yes,
            price.ask_yes,
            price.bid_no,
            price.ask_no,
        )

    async def update_crypto_price(self, price: CryptoPriceEvent) -> None:
        async with self._lock:
            self._crypto_price = price
            self._record_beat_price()
        if self._beat_price is None:
            logger.debug("Crypto Price -> %.2f", price.price)
        else:
            beat_diff = price.price - self._beat_price
            beat_pct = beat_diff / self._beat_price * 100
            logger.debug(
                "Crypto Price -> %.2f diff=%+.2f %+.3f%%", price.price, beat_diff, beat_pct
            )

    async def update_crypto_ohlcv(self, ohlcv: CryptoOHLCVEvent) -> None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv

    async def latest_state(self) -> MarketLatestState:
        async with self._lock:
            return MarketLatestState(
                market=self._market,
                beat_price=self._beat_price,
                market_price=self._market_price,
                crypto_price=self._crypto_price,
                crypto_ohlcv=self._crypto_ohlcv,
            )

    def _record_beat_price(self) -> None:
        if self._market is None or self._crypto_price is None:
            return
        beat_offset_ms = self._crypto_price.ts_ms - self._market.start_ts_ms
        beat_offset_abs_ms = abs(beat_offset_ms)
        if beat_offset_abs_ms > _MAX_BEAT_OFFSET_MS:
            return
        should_update = self._beat_offset_ms is None or beat_offset_abs_ms < abs(
            self._beat_offset_ms
        )
        if not should_update:
            return
        self._beat_price = self._crypto_price.price
        self._beat_offset_ms = beat_offset_ms
        logger.info("Beat Price -> %.2f offset_ms=%+d", self._beat_price, beat_offset_ms)
