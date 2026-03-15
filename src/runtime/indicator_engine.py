from __future__ import annotations

from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_quote import CryptoQuoteEvent
from indicators.macd import MacdIndicator
from runtime.indicator_state import IndicatorState


class IndicatorEngine:
    def __init__(self, state: IndicatorState) -> None:
        self._state = state
        self._crypto_macd = MacdIndicator()

    async def on_crypto_quote(self, event: CryptoQuoteEvent) -> None:
        return None

    async def on_crypto_ohlcv(self, event: CryptoOHLCVEvent) -> None:
        crypto_macd = self._crypto_macd.calc(event.close)
        if crypto_macd is not None:
            await self._state.update_crypto_macd(crypto_macd)
