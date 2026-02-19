from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from typing import Any, NoReturn, Self

import orjson
from py_clob_client.clob_types import ApiCreds
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from streams.market_order_event import MarketOrderEvent, MarketOrderEventStatus
from streams.market_trade_event import (
    MarketTradeEvent,
    MarketTradeEventStatus,
)
from utils.logger import get_logger

_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

_ORDER_INVALID_STATUSES: set[str] = {
    "INVALID",
    "CANCELED",
    "CANCELED_MARKET_RESOLVED",
}
_TRADE_STATUS_MAP: dict[str, MarketTradeEventStatus] = {
    "MATCHED": MarketTradeEventStatus.PENDING,
    "MINED": MarketTradeEventStatus.PENDING,
    "CONFIRMED": MarketTradeEventStatus.SUCCESS,
    "RETRYING": MarketTradeEventStatus.PENDING,
    "FAILED": MarketTradeEventStatus.FAILURE,
}

logger = get_logger("MARKET TRADE STREAM")


class _EndOfStream:
    pass


_STREAM_ENDED = _EndOfStream()
type MarketUserEvent = MarketOrderEvent | MarketTradeEvent
type _QueuedMessage = MarketUserEvent | _EndOfStream


class MarketTradeStream(AsyncIterator[MarketUserEvent]):
    def __init__(self, credentials: ApiCreds) -> None:
        self._credentials = credentials
        proxy_wallet = os.getenv("POLYMARKET_PROXY_WALLET")
        if not proxy_wallet:
            raise ValueError("missing POLYMARKET_PROXY_WALLET")
        self._proxy_wallet = proxy_wallet
        self._messages: asyncio.Queue[_QueuedMessage] = asyncio.Queue()
        self._stopping = asyncio.Event()
        self._stream_task: asyncio.Task[None] | None = None
        self._stream_error: BaseException | None = None

    def __aiter__(self) -> Self:
        self._ensure_stream_task()
        return self

    async def __anext__(self) -> MarketUserEvent:
        self._ensure_stream_task()
        event = await self._messages.get()
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
            self._enqueue(_STREAM_ENDED)

    async def stop(self) -> None:
        self._stopping.set()
        if self._stream_task is None:
            return
        self._stream_task.cancel()
        await asyncio.gather(self._stream_task, return_exceptions=True)

    async def _maintain_connection(self) -> None:
        while not self._stopping.is_set():
            try:
                logger.info("connecting market trade websocket")
                async with connect(
                    _USER_WS_URL, ping_interval=20, ping_timeout=20, max_queue=1024, max_size=None
                ) as ws:
                    ws_lock = asyncio.Lock()
                    await _initial_subscribe(ws, self._credentials)
                    logger.info("connected market trade websocket")
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    try:
                        await self._receive_message(ws)
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
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
                message = orjson.loads(raw)
            except (TypeError, orjson.JSONDecodeError):
                return None
            if not isinstance(message, dict):
                continue

            if message.get("event_type") == "order":
                logger.debug("%r", message)
                order_event = _build_order_event(message)
                if order_event is not None:
                    self._enqueue(order_event)

            if message.get("event_type") == "trade":
                logger.debug("%r", message)
                trade_event = _build_trade_event(message, self._proxy_wallet)
                if trade_event is not None:
                    self._enqueue(trade_event)

    async def _heartbeat(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(_PING_INTERVAL_S)
                async with ws_lock:
                    await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass

    def _enqueue(self, item: _QueuedMessage) -> None:
        self._messages.put_nowait(item)


async def _initial_subscribe(ws: ClientConnection, credentials: ApiCreds) -> None:
    await ws.send(
        orjson.dumps(
            {
                "auth": {
                    "apiKey": credentials.api_key,
                    "secret": credentials.api_secret,
                    "passphrase": credentials.api_passphrase,
                },
                "type": "user",
            }
        ).decode("utf-8")
    )


def _build_order_event(data: dict[str, Any]) -> MarketOrderEvent | None:
    try:
        ts_ms = int(data["timestamp"])
        market_id = str(data["market"])
        token_id = str(data["asset_id"])
        order_id = str(data["id"])
        trade_ids = _string_list(data.get("associate_trades"))
        raw_status = str(data["status"])
        ordered_shares = float(data["original_size"])
        matched_shares = float(data["size_matched"])
        full_matched = matched_shares > 0.96 * ordered_shares
        cancelled = raw_status in _ORDER_INVALID_STATUSES
        if matched_shares > 0:
            status = MarketOrderEventStatus.MATCHED
        elif cancelled:
            status = MarketOrderEventStatus.INVALID
        else:
            status = MarketOrderEventStatus.PENDING

        return MarketOrderEvent(
            ts_ms=ts_ms,
            market_id=market_id,
            token_id=token_id,
            order_id=order_id,
            trade_ids=trade_ids,
            status=status,
            raw_status=raw_status,
            ordered_shares=round(ordered_shares, 6),
            pending_shares=round(ordered_shares - matched_shares, 6),
            matched_shares=round(matched_shares, 6),
            full_matched=full_matched,
            cancelled=cancelled,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _build_trade_event(data: dict[str, Any], proxy_wallet: str) -> MarketTradeEvent | None:
    try:
        ts_ms = int(data["timestamp"])
        market_id = str(data["market"])
        trade_id = str(data["id"])
        raw_status = str(data["status"])
        status = _TRADE_STATUS_MAP[raw_status]

        if _same_address(data.get("maker_address"), proxy_wallet):
            token_id = str(data["asset_id"])
            order_id = str(data["taker_order_id"])
            shares = float(data["size"])
        else:
            sub_order = _find_sub_order(data.get("maker_orders"), proxy_wallet)
            if sub_order is None:
                return None
            token_id = str(sub_order["asset_id"])
            order_id = str(sub_order["order_id"])
            shares = float(sub_order["matched_amount"])

        return MarketTradeEvent(
            ts_ms=ts_ms,
            market_id=market_id,
            token_id=token_id,
            order_id=order_id,
            trade_id=trade_id,
            raw_status=raw_status,
            status=status,
            shares=round(shares, 6),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _same_address(left: object, right: str) -> bool:
    return isinstance(left, str) and left.lower() == right.lower()


def _find_sub_order(sub_orders: object, proxy_wallet: str) -> dict[str, Any] | None:
    if not isinstance(sub_orders, list):
        return None
    for sub_order in sub_orders:
        if not isinstance(sub_order, dict):
            continue
        if _same_address(sub_order.get("maker_address"), proxy_wallet):
            return sub_order
    return None
