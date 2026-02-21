from __future__ import annotations

import asyncio

from models.market import Market
from strategies.context import MarketLatestState
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger

logger = get_logger("MARKET STATE")


class MarketState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market: Market | None = None
        self._market_price: MarketPriceEvent | None = None
        self._crypto_price: CryptoPriceEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

    async def update_market_price(self, price: MarketPriceEvent) -> None:
        async with self._lock:
            self._market_price = price
            if self._market is None or self._market.id != price.market.id:
                self._market = price.market
            logger.debug(
                "market price -> bid_yes=%.2f ask_yes=%.2f bid_no=%.2f ask_no=%.2f",
                price.bid_yes,
                price.ask_yes,
                price.bid_no,
                price.ask_no,
            )

    async def update_crypto_price(self, price: CryptoPriceEvent) -> None:
        async with self._lock:
            self._crypto_price = price
            logger.debug(
                "crypto price -> %s=%.2f",
                price.symbol,
                price.price,
            )

    async def update_crypto_ohlcv(self, ohlcv: CryptoOHLCVEvent) -> None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv

    async def latest_state(self) -> MarketLatestState:
        async with self._lock:
            return MarketLatestState(
                market=self._market,
                market_price=self._market_price,
                crypto_price=self._crypto_price,
                crypto_ohlcv=self._crypto_ohlcv,
            )
