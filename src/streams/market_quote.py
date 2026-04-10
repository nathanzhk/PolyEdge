from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from events import MarketQuoteEvent
from markets.base import Market
from utils.env import Env
from utils.logger import get_logger

_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET QUOTE")


class MarketQuoteStream:
    def __init__(self, market: Market) -> None:
        self._market = market

    def __aiter__(self) -> AsyncIterator[MarketQuoteEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketQuoteEvent]:
        while True:
            try:
                logger.info("connecting market quote websocket")
                async with connect(
                    f"{Env.POLYMARKET_WS_BASE_URL}/market",
                    ping_interval=20,
                    ping_timeout=5,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    ws_lock = asyncio.Lock()
                    await _initial_subscribe(ws, self._market)
                    logger.info("connected market quote websocket")
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    try:
                        async for raw in ws:
                            if raw == "PONG":
                                continue
                            try:
                                message = orjson.loads(raw)
                            except Exception:
                                continue
                            if not isinstance(message, dict):
                                continue
                            if message.get("event_type") == "best_bid_ask":
                                event = self._build_event(message)
                                if event is not None:
                                    yield event
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("disconnected market quote websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _build_event(self, message: dict) -> MarketQuoteEvent | None:
        try:
            ts_ms = int(message["timestamp"])
            market_id = message["market"]
            token_id = message["asset_id"]
            best_bid = float(message["best_bid"])
            best_ask = float(message["best_ask"])
            spread = float(message["spread"])
        except Exception:
            return None

        market = self._market
        if market_id != market.id:
            return None

        if token_id == market.yes_token.id:
            token = market.yes_token
        elif token_id == market.no_token.id:
            token = market.no_token
        else:
            return None

        return MarketQuoteEvent(
            exch_ts_ms=ts_ms,
            market=market,
            token=token,
            best_bid=round(best_bid, 3),
            best_ask=round(best_ask, 3),
            spread=round(spread, 3),
        )

    async def _heartbeat(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        try:
            while True:
                await asyncio.sleep(_PING_INTERVAL_S)
                async with ws_lock:
                    await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass


async def _initial_subscribe(ws: ClientConnection, market: Market) -> None:
    payload = {
        "type": "market",
        "custom_feature_enabled": True,
        "assets_ids": [market.yes_token.id, market.no_token.id],
    }
    await ws.send(orjson.dumps(payload).decode())
