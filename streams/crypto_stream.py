from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

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
    def __init__(self, symbol: str = "btcusdt", interval_ms: int = 100) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms must be greater than 0")
        self._symbol = symbol.lower()
        self._interval_ms = interval_ms
        self._next_bucket_ts_ms = 0
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
                        message = self._parse_message(raw)
                        if message is None:
                            continue
                        ts_ms = self._message_ts_ms(message)
                        if ts_ms is None or not self._advance_bucket(ts_ms):
                            continue
                        event = self._build_event(message, ts_ms)
                        if event is None:
                            continue
                        logger.debug("crypto price -> %s=%.2f", event.symbol, event.price)
                        yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("websocket disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _parse_message(self, raw: str | bytes) -> dict[str, Any] | None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            return None
        if not isinstance(message, dict) or message.get("e") != "aggTrade":
            return None
        return message

    def _message_ts_ms(self, message: dict[str, Any]) -> int | None:
        try:
            return int(message["T"])
        except (KeyError, TypeError, ValueError):
            return None

    def _build_event(self, message: dict[str, Any], ts_ms: int) -> CryptoPriceEvent | None:
        try:
            return CryptoPriceEvent(
                ts_ms=ts_ms,
                symbol=str(message["s"]),
                price=round(float(message["p"]), 3),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _advance_bucket(self, ts_ms: int) -> bool:
        if ts_ms < self._next_bucket_ts_ms:
            return False
        self._next_bucket_ts_ms = ts_ms - (ts_ms % self._interval_ms) + self._interval_ms
        return True


class CryptoOHLCVStream:
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
                        message = self._parse_message(raw)
                        if message is None:
                            continue
                        ts_ms = self._message_ts_ms(message)
                        if ts_ms is None:
                            continue
                        event = self._build_event(message, ts_ms)
                        if event is None:
                            continue
                        yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("websocket disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _parse_message(self, raw: str | bytes) -> dict[str, Any] | None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            return None
        if not isinstance(message, dict) or message.get("e") != "kline":
            return None
        return message

    def _message_ts_ms(self, message: dict[str, Any]) -> int | None:
        ohlcv = message.get("k")
        if not isinstance(ohlcv, dict):
            return None
        try:
            return int(ohlcv["t"])
        except (KeyError, TypeError, ValueError):
            return None

    def _build_event(self, message: dict[str, Any], ts_ms: int) -> CryptoOHLCVEvent | None:
        ohlcv = message.get("k")
        if not isinstance(ohlcv, dict):
            return None
        try:
            return CryptoOHLCVEvent(
                ts_ms=ts_ms,
                symbol=str(ohlcv["s"]),
                open=float(ohlcv["o"]),
                high=float(ohlcv["h"]),
                low=float(ohlcv["l"]),
                close=float(ohlcv["c"]),
                volume=float(ohlcv["v"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
