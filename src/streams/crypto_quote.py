from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from events import CryptoQuoteEvent
from utils.logger import get_logger
from utils.time import now_ts_ms

_BUCKET_INTERVAL_MS = 5
_RECONNECT_DELAY_S = 2
_LOCAL_QUOTE_WS_URL = "ws://127.0.0.1:8080/ws"

logger = get_logger("CRYPTO QUOTE")


class CryptoQuoteStream:
    def __init__(self, symbol: str) -> None:
        if symbol is None or symbol.strip() == "":
            raise ValueError("cannot load current symbol")
        self._symbol = symbol.strip().lower()
        self._ws_url = _LOCAL_QUOTE_WS_URL
        self._next_bucket_ts_ms = 0

    def __aiter__(self) -> AsyncIterator[CryptoQuoteEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[CryptoQuoteEvent]:
        while True:
            try:
                logger.info("connecting crypto quote websocket")
                async with connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=5,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    logger.info("connected crypto quote websocket")
                    async for raw in ws:
                        curr_ts_ms = now_ts_ms()
                        if curr_ts_ms < self._next_bucket_ts_ms:
                            continue
                        self._next_bucket_ts_ms = (
                            curr_ts_ms - (curr_ts_ms % _BUCKET_INTERVAL_MS) + _BUCKET_INTERVAL_MS
                        )
                        try:
                            message = orjson.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(message, dict):
                            continue
                        event = self._build_event(message)
                        if event is not None:
                            yield event
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("disconnected crypto quote websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, message: dict) -> CryptoQuoteEvent | None:
        try:
            ts_ms = int(float(message["timestamp"]))
            price = float(message["prices"]["combined"])
        except Exception:
            return None

        return CryptoQuoteEvent(
            exch_ts_ms=ts_ms,
            symbol=self._symbol,
            best_bid=round(price, 3),
            best_ask=round(price, 3),
        )
