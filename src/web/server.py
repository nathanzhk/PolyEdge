from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import orjson
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from events import RuntimeStateEvent

app = FastAPI()

_clients: set[WebSocket] = set()
_broadcast_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)
_latest_by_worker: dict[str, bytes] = {}

_STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _clients.add(ws)
    try:
        for payload in _latest_by_worker.values():
            await ws.send_bytes(payload)
        while True:
            await ws.receive_text()
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    finally:
        _clients.discard(ws)


@app.post("/state")
async def ingest_state(request: Request) -> dict[str, bool]:
    enqueue_payload(await request.body())
    return {"ok": True}


async def broadcast_loop() -> None:
    while True:
        payload = await _broadcast_queue.get()
        to_remove: list[WebSocket] = []
        for ws in _clients:
            try:
                await ws.send_bytes(payload)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            _clients.discard(ws)


def enqueue_event(event: RuntimeStateEvent) -> None:
    enqueue_payload(serialize_event(event))


def enqueue_payload(payload: bytes) -> None:
    _remember_latest(payload)
    try:
        _broadcast_queue.put_nowait(payload)
    except asyncio.QueueFull:
        try:
            _broadcast_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        _broadcast_queue.put_nowait(payload)


def serialize_event(event: RuntimeStateEvent) -> bytes:
    return orjson.dumps(_serialize_state(event))


def _remember_latest(payload: bytes) -> None:
    try:
        state = orjson.loads(payload)
    except orjson.JSONDecodeError:
        return
    worker_id = state.get("worker_id")
    if not worker_id:
        market = state.get("market") or {}
        worker_id = market.get("slug")
    if worker_id:
        _latest_by_worker[worker_id] = payload


def _serialize_state(event: RuntimeStateEvent) -> dict[str, Any]:
    return {
        "worker_id": event.market.slug,
        "event_ts_ms": event.event_ts_ms,
        "reason": event.reason,
        "market": {
            "slug": event.market.slug,
            "title": event.market.title,
            "start_ts_s": event.market.start_ts_s,
            "end_ts_s": event.market.end_ts_s,
            "fee_rate": event.market.fee_rate,
        },
        "yes_quote": _serialize_quote(event.yes_token_quote),
        "no_quote": _serialize_quote(event.no_token_quote),
        "crypto": {
            "symbol": event.crypto_quote.symbol,
            "baseline": event.crypto_quote.baseline,
            "change": event.crypto_quote.change,
            "price": event.crypto_quote.price,
            "best_bid": event.crypto_quote.best_bid,
            "best_ask": event.crypto_quote.best_ask,
            "mid": event.crypto_quote.mid,
        },
        "ohlcv": {
            "open": event.crypto_ohlcv.open,
            "high": event.crypto_ohlcv.high,
            "low": event.crypto_ohlcv.low,
            "close": event.crypto_ohlcv.close,
            "volume": event.crypto_ohlcv.volume,
        },
        "prev_side": event.prev_side,
        "curr_side": event.curr_side,
        "yes_position": _serialize_position(event.yes_token_position),
        "no_position": _serialize_position(event.no_token_position),
    }


def _serialize_quote(q: Any) -> dict[str, Any]:
    return {
        "best_bid": q.best_bid,
        "best_ask": q.best_ask,
        "mid": q.mid,
        "spread": q.spread,
    }


def _serialize_position(p: Any) -> dict[str, Any] | None:
    if p is None:
        return None
    return {
        "opening_shares": p.opening_shares,
        "open_settling_shares": p.open_settling_shares,
        "holding_shares": p.holding_shares,
        "holding_avg_price": p.holding_avg_price,
        "holding_cost": p.holding_cost,
        "closing_shares": p.closing_shares,
        "close_settling_shares": p.close_settling_shares,
        "realized_pnl": p.realized_pnl,
        "effective_shares": p.effective_shares,
        "sellable_shares": p.sellable_shares,
    }
