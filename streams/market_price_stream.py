from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any, NoReturn, Self

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from models.market import Market
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger
from utils.time import sleep_until

_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

_SWITCH_BEFORE_END_S = 5
_ACTIVATE_BEFORE_MS = 1000
_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET STREAM")


class _EndOfStream:
    pass


_STREAM_ENDED = _EndOfStream()
type _LatestEvent = MarketPriceEvent | _EndOfStream


class MarketPriceStream(AsyncIterator[MarketPriceEvent]):
    def __init__(self, market_type: type[Market]) -> None:
        self._market_type = market_type
        self._market: Market | None = None
        self._stopping = asyncio.Event()
        self._stream_task: asyncio.Task[None] | None = None
        self._stream_error: BaseException | None = None
        self._latest_event: asyncio.Queue[_LatestEvent] = asyncio.Queue(maxsize=1)

    def __aiter__(self) -> Self:
        self._ensure_stream_task()
        return self

    async def __anext__(self) -> MarketPriceEvent:
        self._ensure_stream_task()
        event = await self._latest_event.get()
        if isinstance(event, _EndOfStream):
            self._raise_stream_finished()
        return event

    def _ensure_stream_task(self) -> None:
        if self._stream_task is None:
            self._stream_task = asyncio.create_task(self._stream_worker())
            self._stream_error = None

    def _raise_stream_finished(self) -> NoReturn:
        if self._stream_error is not None:
            raise self._stream_error
        raise StopAsyncIteration

    async def _stream_worker(self) -> None:
        try:
            await self._maintain_connection()
        except asyncio.CancelledError as exc:
            if not self._stopping.is_set():
                self._stream_error = exc
            raise
        except BaseException as exc:
            self._stream_error = exc
            raise
        finally:
            self._set_latest(_STREAM_ENDED)

    async def stop(self) -> None:
        self._stopping.set()
        if self._stream_task is None:
            return
        self._stream_task.cancel()
        await asyncio.gather(self._stream_task, return_exceptions=True)

    async def _maintain_connection(self) -> None:
        self._market = await asyncio.to_thread(self._market_type.curr_market)
        while not self._stopping.is_set():
            try:
                async with connect(
                    _MARKET_WS_URL, ping_interval=20, ping_timeout=20, max_queue=1024, max_size=None
                ) as ws:
                    ws_lock = asyncio.Lock()
                    await _initial_subscribe(ws, self._market)
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    lifecycle_task = asyncio.create_task(self._lifecycle(ws, ws_lock))
                    try:
                        await self._receive_message(ws)
                    finally:
                        heartbeat_task.cancel()
                        lifecycle_task.cancel()
                        await asyncio.gather(heartbeat_task, lifecycle_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError) as e:
                if self._stopping.is_set():
                    break
                logger.error("websocket disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    async def _receive_message(self, ws: ClientConnection) -> None:
        async for raw in ws:
            if raw == "PONG":
                continue

            try:
                message = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                return None
            if not isinstance(message, dict) or message.get("event_type") != "price_change":
                continue

            try:
                ts_ms = int(message["timestamp"])
            except (KeyError, TypeError, ValueError):
                continue

            market = self._market
            if market is None:
                return

            start_ts_ms, end_ts_ms = market.start_ts_ms, market.end_ts_ms
            if ts_ms < start_ts_ms - _ACTIVATE_BEFORE_MS or ts_ms > end_ts_ms:
                continue

            event = _build_event(message, market, ts_ms)
            if event is None:
                continue

            self._set_latest(event)

    async def _heartbeat(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(_PING_INTERVAL_S)
                async with ws_lock:
                    await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass

    async def _lifecycle(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        while not self._stopping.is_set():
            curr_market = self._market
            if curr_market is None:
                return
            await sleep_until(curr_market.end_ts_s - _SWITCH_BEFORE_END_S)
            await _update_subscribe(ws, ws_lock, "unsubscribe", curr_market)
            next_market = await asyncio.to_thread(curr_market.next_market)
            await _update_subscribe(ws, ws_lock, "subscribe", next_market)
            self._market = next_market
            logger.info("switch market from %s to %s", curr_market.slug, next_market.slug)

    def _set_latest(self, item: _LatestEvent) -> None:
        if self._latest_event.full():
            try:
                self._latest_event.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self._latest_event.put_nowait(item)


def _token_ids(market: Market) -> list[str]:
    return [market.yes_token.id, market.no_token.id]


async def _initial_subscribe(ws: ClientConnection, market: Market) -> None:
    await ws.send(json.dumps({"type": "market", "assets_ids": _token_ids(market)}))


async def _update_subscribe(
    ws: ClientConnection, ws_lock: asyncio.Lock, operation: str, market: Market
) -> None:
    logger.debug("%s market %s", operation, market.slug)
    async with ws_lock:
        await ws.send(json.dumps({"operation": operation, "assets_ids": _token_ids(market)}))


def _build_event(data: dict[str, Any], market: Market, ts_ms: int) -> MarketPriceEvent | None:
    bid_yes_raw: str | None = None
    ask_yes_raw: str | None = None
    bid_no_raw: str | None = None
    ask_no_raw: str | None = None
    for price_change in data.get("price_changes", []):
        if not isinstance(price_change, dict):
            continue
        asset_id = price_change.get("asset_id")
        if asset_id == market.yes_token.id:
            bid_yes_raw = price_change.get("best_bid")
            ask_yes_raw = price_change.get("best_ask")
        elif asset_id == market.no_token.id:
            bid_no_raw = price_change.get("best_bid")
            ask_no_raw = price_change.get("best_ask")
    if bid_yes_raw is None or ask_yes_raw is None or bid_no_raw is None or ask_no_raw is None:
        return None
    return MarketPriceEvent(
        ts_ms=ts_ms,
        market=market,
        bid_yes=round(float(bid_yes_raw), 3),
        ask_yes=round(float(ask_yes_raw), 3),
        bid_no=round(float(bid_no_raw), 3),
        ask_no=round(float(ask_no_raw), 3),
    )
