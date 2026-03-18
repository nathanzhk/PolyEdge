from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from events.crypto_ohlcv import CryptoOHLCVEvent
from utils.env import Env
from utils.logger import get_logger

_RECONNECT_DELAY_S = 2

logger = get_logger("CRYPTO OHLCV")


class CryptoOHLCVStream:
    def __init__(self, symbol: str, interval: str = "1s") -> None:
        if symbol is None or symbol.strip() == "":
            raise ValueError("cannot load current symbol")
        self._symbol = symbol.strip().lower()
        self._ws_url = f"{Env.BINANCE_WS_BASE_URL}/{self._symbol}@kline_{interval}"

    def __aiter__(self) -> AsyncIterator[CryptoOHLCVEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[CryptoOHLCVEvent]:
        while True:
            try:
                logger.info("connecting crypto ohlcv websocket")
                async with connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=5,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    logger.info("connected crypto ohlcv websocket")
                    async for raw in ws:
                        try:
                            message = orjson.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(message, dict):
                            continue
                        if message.get("e") == "kline":
                            event = self._build_event(message)
                            if event is not None:
                                yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("disconnected crypto ohlcv websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, message: dict) -> CryptoOHLCVEvent | None:
        try:
            ts_ms = int(message["E"])
            symbol = str(message["s"]).lower()
            ohlcv = message.get("k")
            if not isinstance(ohlcv, dict):
                return None
            start_ts_ms = int(ohlcv["t"])
            close_ts_ms = int(ohlcv["T"])
            open = float(ohlcv["o"])
            high = float(ohlcv["h"])
            low = float(ohlcv["l"])
            close = float(ohlcv["c"])
            volume = float(ohlcv["v"])
        except Exception:
            return None

        if symbol != self._symbol:
            return None

        return CryptoOHLCVEvent(
            exch_ts_ms=ts_ms,
            symbol=symbol,
            start_ts_ms=start_ts_ms,
            close_ts_ms=close_ts_ms,
            open=round(open, 3),
            high=round(high, 3),
            low=round(low, 3),
            close=round(close, 3),
            volume=round(volume * 1000, 3),
        )
