from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from utils.logger import get_logger

_BASE_URL = "wss://stream.binance.com:443/ws"
_RECONNECT_DELAY_S = 2

logger = get_logger("CRYPTO STREAM")


class CryptoPriceStream:
    def __init__(self, symbol: str = "btcusdt") -> None:
        self._symbol = symbol.lower()
        self._ws_url = f"{_BASE_URL}/{self._symbol}@aggTrade"

    def __aiter__(self) -> AsyncIterator[CryptoPriceEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[CryptoPriceEvent]:
        while True:
            try:
                logger.info("connecting crypto price websocket %s", self._symbol.upper())
                async with connect(self._ws_url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("connected crypto price websocket %s", self._symbol.upper())
                    async for raw in ws:
                        event = self._build_event(raw)
                        if event is not None:
                            yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("websocket disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, raw: str | bytes) -> CryptoPriceEvent | None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            return None
        if not isinstance(message, dict) or message.get("e") != "aggTrade":
            return None
        try:
            return CryptoPriceEvent(
                ts_ms=int(message["T"]),
                symbol=self._symbol.upper(),
                price=round(float(message["p"]), 3),
            )
        except (KeyError, TypeError, ValueError):
            return None


class CryptoOhlcvStream:
    def __init__(self, symbol: str = "btcusdt", interval: str = "1s") -> None:
        self._symbol = symbol.lower()
        self._es_url = f"{_BASE_URL}/{self._symbol}@kline_{interval}"

    def __aiter__(self) -> AsyncIterator[CryptoOHLCVEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[CryptoOHLCVEvent]:
        while True:
            try:
                logger.info("connecting crypto ohlcv websocket %s", self._symbol.upper())
                async with connect(self._es_url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("connected crypto ohlcv websocket %s", self._symbol.upper())
                    async for raw in ws:
                        event = self._build_event(raw)
                        if event is not None:
                            yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("websocket disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, raw: str | bytes) -> CryptoOHLCVEvent | None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            return None
        if not isinstance(message, dict) or message.get("e") != "kline":
            return None
        ohlcv = message.get("k")
        if not isinstance(ohlcv, dict):
            return None
        try:
            return CryptoOHLCVEvent(
                ts_ms=int(ohlcv["t"]),
                symbol=self._symbol.upper(),
                open=float(ohlcv["o"]),
                high=float(ohlcv["h"]),
                low=float(ohlcv["l"]),
                close=float(ohlcv["c"]),
                volume=float(ohlcv["v"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
