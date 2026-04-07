from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import orjson
from py_clob_client.clob_types import ApiCreds
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from enums import MarketOrderStatus, MarketOrderType, MarketTradeStatus, Role, Side
from events import MarketOrderEvent, MarketTradeEvent
from utils.env import Env
from utils.logger import get_logger
from utils.time import now_ts_ms

_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET TRADE")


class MarketTradeStream:
    def __init__(self, credentials: ApiCreds) -> None:
        self._credentials = credentials
        self._proxy_wallet = Env.POLYMARKET_PROXY_WALLET

    def __aiter__(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[MarketOrderEvent | MarketTradeEvent]:
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
                            try:
                                message = orjson.loads(raw)
                            except Exception:
                                continue
                            if not isinstance(message, dict):
                                continue
                            event_type = message.get("event_type")
                            if event_type == "order":
                                event = build_order_event(
                                    message,
                                    source="push",
                                )
                                if event is not None:
                                    yield event
                            elif event_type == "trade":
                                event = build_trade_event(
                                    message,
                                    self._proxy_wallet,
                                    source="push",
                                )
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

    async def _heartbeat(self, ws: ClientConnection, ws_lock: asyncio.Lock) -> None:
        try:
            while True:
                await asyncio.sleep(_PING_INTERVAL_S)
                async with ws_lock:
                    await ws.send("PING")
        except (ConnectionClosed, asyncio.CancelledError):
            pass


def build_order_event(message: dict, *, source: str) -> MarketOrderEvent | None:
    logger.debug("%s order event: %s", source, message)
    try:
        ts_ms = message.get("timestamp") or None
        trade_ids = message["associate_trades"]
        return MarketOrderEvent(
            event_source=source,
            exch_ts_ms=int(ts_ms) if ts_ms else now_ts_ms(),
            market_id=message["market"],
            token_id=message["asset_id"],
            order_id=message["id"],
            trade_ids=trade_ids if isinstance(trade_ids, list) else [],
            status=MarketOrderStatus(str(message["status"]).upper()),
            shares=round(float(message["original_size"]), 6),
            side=Side(str(message["side"]).upper()),
            type=MarketOrderType(str(message["order_type"]).upper()),
            price=round(float(message["price"]), 3),
            matched_shares=round(float(message["size_matched"]), 6),
        )
    except Exception:
        return None


def build_trade_event(message: dict, proxy_wallet: str, *, source: str) -> MarketTradeEvent | None:
    logger.debug("%s trade event: %s", source, message)
    try:
        ts_ms = message.get("timestamp") or None
        if str(message.get("maker_address")).lower() == proxy_wallet.lower():
            token_id = message["asset_id"]
            order_id = message["taker_order_id"]
            shares = float(message["size"])
            side = Side(str(message["side"]).upper())
            price = float(message["price"])
        else:
            sub_order = _find_sub_order(message.get("maker_orders"), proxy_wallet)
            if sub_order is None:
                return None
            token_id = sub_order["asset_id"]
            order_id = sub_order["order_id"]
            shares = float(sub_order["matched_amount"])
            side = Side(str(sub_order["side"]).upper())
            price = float(sub_order["price"])
        return MarketTradeEvent(
            event_source=source,
            exch_ts_ms=int(ts_ms) if ts_ms else now_ts_ms(),
            market_id=message["market"],
            token_id=token_id,
            order_id=order_id,
            trade_id=message["id"],
            status=MarketTradeStatus(str(message["status"]).upper()),
            shares=round(shares, 6),
            side=side,
            role=Role(str(message["trader_side"]).upper()),
            price=round(price, 3),
        )
    except Exception:
        return None


def _find_sub_order(sub_orders: object, proxy_wallet: str) -> dict[str, Any] | None:
    if not isinstance(sub_orders, list):
        return None
    for sub_order in sub_orders:
        if not isinstance(sub_order, dict):
            continue
        if str(sub_order.get("maker_address")).lower() == proxy_wallet.lower():
            return sub_order
    return None


async def _initial_subscribe(ws: ClientConnection, credentials: ApiCreds) -> None:
    payload = {
        "type": "user",
        "auth": {
            "apiKey": credentials.api_key,
            "secret": credentials.api_secret,
            "passphrase": credentials.api_passphrase,
        },
    }
    await ws.send(orjson.dumps(payload).decode())
