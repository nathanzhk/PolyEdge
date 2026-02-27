from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from time import perf_counter_ns
from typing import Any, NoReturn, Self

import orjson
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from models.market import Market
from streams.market_price_event import MarketPriceEvent
from utils.env import Env
from utils.logger import get_logger
from utils.stats import LatencyStats, StreamStats
from utils.time import now_ts_ms, sleep_until

_SWITCH_BEFORE_END_S = 5
_ACTIVATE_BEFORE_MS = 2_000

_RECONNECT_DELAY_S = 2
_PING_INTERVAL_S = 10

logger = get_logger("MARKET STREAM")


class _EndOfStream:
    pass


_STREAM_ENDED = _EndOfStream()
type _LatestEvent = MarketPriceEvent | _EndOfStream


class MarketPriceStream(AsyncIterator[MarketPriceEvent]):
    def __init__(self, market_type: type[Market], interval_ms: int) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms must be greater than 0")
        self._market_type = market_type
        self._interval_ms = interval_ms
        self._next_bucket_ts_ms = 0
        self._market: Market | None = None
        self._stopping = asyncio.Event()
        self._stream_task: asyncio.Task[None] | None = None
        self._stream_error: BaseException | None = None
        self._latest_event: asyncio.Queue[_LatestEvent] = asyncio.Queue(maxsize=1)
        self._raw_stats = StreamStats("market price stream", logger)
        self._latency_stats = LatencyStats("market price parse", logger)

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
        logger.info("loading current market")
        self._market = await asyncio.to_thread(self._market_type.curr_market)
        logger.info("loaded current market: %s", self._market.slug)
        while not self._stopping.is_set():
            try:
                logger.info("connecting market price websocket")
                async with connect(
                    f"{Env.POLYMARKET_WS_BASE_URL}/market",
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1024,
                    max_size=None,
                ) as ws:
                    ws_lock = asyncio.Lock()
                    await _initial_subscribe(ws, self._market)
                    logger.info("connected market price websocket")
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws, ws_lock))
                    lifecycle_task = asyncio.create_task(self._lifecycle(ws, ws_lock))
                    try:
                        async for raw in ws:
                            self._raw_stats.record_raw()
                            if raw == "PONG":
                                self._raw_stats.record_pong()
                                continue
                            recv_ts_ms = now_ts_ms()
                            if not self._advance_bucket(recv_ts_ms):
                                self._raw_stats.record_bucket_drop()
                                continue
                            started_at_ns = perf_counter_ns() if self._latency_stats.enabled else 0
                            self._handle_message(raw)
                            self._latency_stats.record_ns(started_at_ns)
                    finally:
                        heartbeat_task.cancel()
                        lifecycle_task.cancel()
                        await asyncio.gather(heartbeat_task, lifecycle_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError) as e:
                if self._stopping.is_set():
                    break
                logger.error("disconnected market price websocket: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_S)

    def _handle_message(self, raw: str | bytes) -> None:
        try:
            message = orjson.loads(raw)
        except (TypeError, orjson.JSONDecodeError):
            self._raw_stats.record_parse_drop()
            return
        if not isinstance(message, dict):
            self._raw_stats.record_parse_drop()
            return
        if message.get("event_type") != "price_change":
            self._raw_stats.record_filter_drop()
            return

        try:
            ts_ms = int(message["timestamp"])
        except (KeyError, TypeError, ValueError):
            self._raw_stats.record_parse_drop()
            return

        market = self._market
        if market is None:
            self._raw_stats.record_filter_drop()
            return

        start_ts_ms, end_ts_ms = market.start_ts_ms, market.end_ts_ms
        if ts_ms < start_ts_ms - _ACTIVATE_BEFORE_MS or ts_ms > end_ts_ms:
            self._raw_stats.record_filter_drop()
            return

        event = _build_event(message, market, ts_ms)
        if event is None:
            self._raw_stats.record_build_drop()
            return
        self._set_latest(event)
        self._raw_stats.record_event()

    def _advance_bucket(self, ts_ms: int) -> bool:
        if ts_ms < self._next_bucket_ts_ms:
            return False
        self._next_bucket_ts_ms = ts_ms - (ts_ms % self._interval_ms) + self._interval_ms
        return True

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
            logger.debug("switch market from %s to %s", curr_market.slug, next_market.slug)

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
    payload = {"type": "market", "assets_ids": _token_ids(market)}
    await ws.send(orjson.dumps(payload).decode("utf-8"))


async def _update_subscribe(
    ws: ClientConnection, ws_lock: asyncio.Lock, operation: str, market: Market
) -> None:
    logger.debug("%s market %s", operation, market.slug)
    async with ws_lock:
        payload = {"operation": operation, "assets_ids": _token_ids(market)}
        await ws.send(orjson.dumps(payload).decode("utf-8"))


def _build_event(data: dict[str, Any], market: Market, ts_ms: int) -> MarketPriceEvent | None:
    bid_yes_raw: str | None = None
    ask_yes_raw: str | None = None
    bid_no_raw: str | None = None
    ask_no_raw: str | None = None
    price_changes = data.get("price_changes", [])
    if not isinstance(price_changes, list):
        return None
    for price_change in price_changes:
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
    try:
        return MarketPriceEvent(
            ts_ms=ts_ms,
            market=market,
            bid_yes=round(float(bid_yes_raw), 3),
            ask_yes=round(float(ask_yes_raw), 3),
            bid_no=round(float(bid_no_raw), 3),
            ask_no=round(float(ask_no_raw), 3),
        )
    except (TypeError, ValueError):
        return None
