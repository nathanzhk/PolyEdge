from __future__ import annotations

import asyncio

from strategies.context import MarketLatestState
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent


class MarketState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market_price: MarketPriceEvent | None = None
        self._crypto_price: CryptoPriceEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

    async def update_market_price(self, price: MarketPriceEvent) -> None:
        async with self._lock:
            self._market_price = price

    async def update_crypto_price(self, price: CryptoPriceEvent) -> None:
        async with self._lock:
            self._crypto_price = price

    async def update_crypto_ohlcv(self, ohlcv: CryptoOHLCVEvent) -> None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv

    async def latest_state(self) -> MarketLatestState:
        async with self._lock:
            return MarketLatestState(
                market_price=self._market_price,
                crypto_price=self._crypto_price,
                crypto_ohlcv=self._crypto_ohlcv,
            )
