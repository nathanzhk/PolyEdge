from __future__ import annotations

import asyncio

from indicators import MacdValue
from strategies import IndicatorLatestState


class IndicatorState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._crypto_macd: MacdValue | None = None

    async def update_crypto_macd(self, value: MacdValue) -> None:
        async with self._lock:
            self._crypto_macd = value

    async def latest_state(self) -> IndicatorLatestState:
        async with self._lock:
            return IndicatorLatestState(
                crypto_macd=self._crypto_macd,
            )
