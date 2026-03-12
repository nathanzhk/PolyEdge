from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from time import perf_counter_ns
from typing import Any

import orjson
from py_clob_client.clob_types import ApiCreds
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from domain import MarketOrderStatus, MarketTradeStatus, Side
from events import MarketOrderEvent, MarketTradeEvent
from infra import Env
from infra.logger import get_logger
from infra.stats import LatencyStats

_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET STREAM")


type MarketUserEvent = MarketOrderEvent | MarketTradeEvent


class MarketTradeStream:
    def __init__(self, credentials: ApiCreds) -> None:
        self._credentials = credentials
        self._proxy_wallet = Env.POLYMARKET_PROXY_WALLET
        self._latency_stats = LatencyStats("market trade parse latency", logger)

    def __aiter__(self) -> AsyncIterator[MarketUserEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketUserEvent]:
        while True:
            try:
                logger.info("connecting market trade websocket")
                async with connect(
                    f"{Env.POLYMARKET_WS_BASE_URL}/user",
                    ping_interval=20,
                    ping_timeout=5,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    ws_lock = asyncio.Lock()
                    await _initial_subscribe(ws, self._credentials)
                    logger.info("connected market trade websocket")
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    try:
                        async for raw in ws:
                            if raw == "PONG":
                                continue
                            started_at_ns = perf_counter_ns() if self._latency_stats.enabled else 0
                            event = self._handle_message(raw)
                            self._latency_stats.record_ns(started_at_ns)
                            if event is not None:
                                yield event
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError) as e:
                logger.error("disconnected market trade websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _handle_message(self, raw: str | bytes) -> MarketUserEvent | None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            return None
        if not isinstance(message, dict):
            return None

        if message.get("event_type") == "order":
            logger.debug("%r", message)
            return _build_order_event(message)

        if message.get("event_type") == "trade":
            logger.debug("%r", message)
            return _build_trade_event(message, self._proxy_wallet)

        return None

    async def _heartbeat(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        try:
            while True:
                await asyncio.sleep(_PING_INTERVAL_S)
                async with ws_lock:
                    await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass


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
        trade_ids = data.get("associate_trades") or []
        return MarketOrderEvent(
            ts_ms=int(data["timestamp"]),
            market_id=str(data["market"]),
            token_id=str(data["asset_id"]),
            order_id=str(data["id"]),
            trade_ids=trade_ids if isinstance(trade_ids, list) else [],
            side=Side(data["side"]),
            status=MarketOrderStatus(str(data["status"])),
            ordered_shares=round(float(data["original_size"]), 6),
            matched_shares=round(float(data["size_matched"]), 6),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _build_trade_event(data: dict[str, Any], proxy_wallet: str) -> MarketTradeEvent | None:
    try:
        if _same_address(data.get("maker_address"), proxy_wallet):
            token_id = str(data["asset_id"])
            order_id = str(data["taker_order_id"])
            shares = float(data["size"])
            side = Side(str(data["side"]))
            price = float(data["price"])
        else:
            sub_order = _find_sub_order(data.get("maker_orders"), proxy_wallet)
            if sub_order is None:
                return None
            token_id = str(sub_order["asset_id"])
            order_id = str(sub_order["order_id"])
            shares = float(sub_order["matched_amount"])
            side = Side(str(sub_order["side"]))
            price = float(sub_order["price"])
        return MarketTradeEvent(
            ts_ms=int(data["timestamp"]),
            market_id=str(data["market"]),
            token_id=token_id,
            order_id=order_id,
            trade_id=str(data["id"]),
            side=side,
            status=MarketTradeStatus(str(data["status"])),
            shares=round(shares, 6),
            price=round(price, 3),
        )
    except (KeyError, TypeError, ValueError):
        return None


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
