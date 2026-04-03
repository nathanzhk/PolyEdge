from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass

import orjson
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from events import MarketQuoteEvent
from markets.base import Market, Token
from utils.env import Env
from utils.logger import get_logger
from utils.time import sleep_until

_SWITCH_BEFORE_END_S = 5
_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET QUOTE")


@dataclass(slots=True, frozen=True)
class _MarketContext:
    yes_token: Token
    no_token: Token
    market: Market


class MarketQuoteStream:
    def __init__(
        self, market_type: type[Market], on_switch: Sequence[Callable[[Market], None]] = ()
    ) -> None:
        market = market_type.curr_market()
        if market is None:
            raise ValueError("cannot load current market")
        self._on_switch = on_switch
        self._ctx = _MarketContext(
            yes_token=market.yes_token,
            no_token=market.no_token,
            market=market,
        )

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
                    await _initial_subscribe(ws, self._ctx.market)
                    logger.info("connected market quote websocket")
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    lifecycle_task = asyncio.create_task(self._lifecycle(ws, ws_lock))
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
                        lifecycle_task.cancel()
                        await asyncio.gather(heartbeat_task, lifecycle_task, return_exceptions=True)
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

        ctx = self._ctx

        if market_id != ctx.market.id:
            return None

        if token_id == ctx.yes_token.id:
            token = ctx.yes_token
        elif token_id == ctx.no_token.id:
            token = ctx.no_token
        else:
            return None

        return MarketQuoteEvent(
            exch_ts_ms=ts_ms,
            market=ctx.market,
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

    async def _lifecycle(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        while True:
            curr_market = self._ctx.market
            await sleep_until(curr_market.end_ts_s - _SWITCH_BEFORE_END_S)
            await _update_subscribe(ws, ws_lock, "unsubscribe", curr_market)
            next_market = await asyncio.to_thread(curr_market.next_market)
            if next_market is None:
                raise ValueError("cannot load next market")
            self._ctx = _MarketContext(
                yes_token=next_market.yes_token,
                no_token=next_market.no_token,
                market=next_market,
            )
            for callback in self._on_switch:
                await asyncio.to_thread(callback, next_market)
            await _update_subscribe(ws, ws_lock, "subscribe", next_market)
            logger.debug("switch market from %s to %s", curr_market.slug, next_market.slug)


async def _initial_subscribe(
    ws: ClientConnection,
    market: Market,
) -> None:
    payload = {
        "type": "market",
        "custom_feature_enabled": True,
        "assets_ids": [market.yes_token.id, market.no_token.id],
    }
    await ws.send(orjson.dumps(payload).decode())


async def _update_subscribe(
    ws: ClientConnection,
    ws_lock: asyncio.Lock,
    operation: str,
    market: Market,
) -> None:
    logger.debug("%s market %s", operation, market.slug)
    async with ws_lock:
        payload = {
            "operation": operation,
            "custom_feature_enabled": True,
            "assets_ids": [market.yes_token.id, market.no_token.id],
        }
        await ws.send(orjson.dumps(payload).decode())
