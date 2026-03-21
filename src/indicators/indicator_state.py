from __future__ import annotations

import asyncio

from indicators.indicator import MacdValue


class IndicatorState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._crypto_macd: MacdValue | None = None

    async def update_crypto_macd(self, value: MacdValue) -> None:
        async with self._lock:
            self._crypto_macd = value
