from __future__ import annotations

import asyncio

from models.market import Market
from strategies.context import MarketLatestState
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger

logger = get_logger("MARKET STATE")

_MAX_BEAT_OFFSET_MS = 200


class MarketState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market: Market | None = None
        self._beat_price: float | None = None
        self._market_price: MarketPriceEvent | None = None
        self._crypto_price: CryptoPriceEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

    async def update_market_price(self, price: MarketPriceEvent) -> None:
        async with self._lock:
            self._market_price = price
            if self._market is None or self._market.id != price.market.id:
                self._market = price.market
                self._beat_price = None
            logger.info(
                "market price -> bid_yes=%.2f ask_yes=%.2f bid_no=%.2f ask_no=%.2f",
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
                logger.info(
                    "crypto price -> %s=%.2f beat=N/A",
                    price.symbol,
                    price.price,
                )
            else:
                beat_diff = price.price - self._beat_price
                beat_pct = beat_diff / self._beat_price * 100
                logger.info(
                    "crypto price -> %s=%.2f diff=%+.2f %+.3f%%",
                    price.symbol,
                    price.price,
                    beat_diff,
                    beat_pct,
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
        if self._beat_price is not None:
            return
        if self._market is None or self._crypto_price is None:
            return
        beat_offset_ms = self._crypto_price.ts_ms - self._market.start_ts_ms
        if beat_offset_ms < 0 or beat_offset_ms > _MAX_BEAT_OFFSET_MS:
            return
        self._beat_price = self._crypto_price.price
        logger.info(
            "beat price -> %s=%.2f offset_ms=%d market=%s",
            self._crypto_price.symbol,
            self._beat_price,
            beat_offset_ms,
            self._market.slug,
        )
