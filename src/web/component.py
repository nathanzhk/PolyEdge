from __future__ import annotations

import asyncio
import errno
import socket
from typing import TYPE_CHECKING

import requests
import uvicorn

from event_bus import EventBus, OverflowPolicy, Subscription
from events import RuntimeStateEvent
from utils.logger import get_logger
from web.server import app, broadcast_loop, enqueue_event, enqueue_payload, serialize_event

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("WEB")

_DEFAULT_PORT = 8177
_HOST = "0.0.0.0"


class WebComponent:
    def __init__(self, *, bus: EventBus, port: int = _DEFAULT_PORT) -> None:
        self._bus = bus
        self._port = port

    def start(self, tasks: asyncio.TaskGroup) -> None:
        state_events = self._bus.subscribe(
            RuntimeStateEvent,
            name="web.runtime-state",
            maxsize=200,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._run(state_events))

    async def _run(self, events: Subscription[RuntimeStateEvent]) -> None:
        sock = self._bind_socket()
        if sock is not None:
            await self._run_server(events, sock)
            return

        logger.info("port %d in use; forwarding web state to existing dashboard", self._port)
        async for event in events:
            payload = serialize_event(event)
            if await self._forward_payload(payload):
                continue

            sock = self._bind_socket()
            if sock is None:
                logger.warning("dashboard forward failed; retrying on next state event")
                continue

            logger.info("dashboard port %d is free; this worker is serving dashboard", self._port)
            enqueue_payload(payload)
            await self._run_server(events, sock)
            return

    async def _run_server(
        self,
        events: Subscription[RuntimeStateEvent],
        sock: socket.socket,
    ) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._event_loop(events))
            tasks.create_task(broadcast_loop())
            tasks.create_task(self._serve(sock))

    async def _event_loop(self, events: Subscription[RuntimeStateEvent]) -> None:
        async for event in events:
            enqueue_event(event)

    async def _serve(self, sock: socket.socket) -> None:
        config = uvicorn.Config(
            app,
            host=_HOST,
            port=self._port,
            log_level="warning",
            lifespan="off",
        )
        try:
            server = uvicorn.Server(config)
            await server.serve(sockets=[sock])
        finally:
            if sock.fileno() != -1:
                sock.close()

    async def _forward_payload(self, payload: bytes) -> bool:
        try:
            await asyncio.to_thread(self._post_payload, payload)
            return True
        except requests.RequestException:
            return False

    def _post_payload(self, payload: bytes) -> None:
        response = requests.post(
            f"http://127.0.0.1:{self._port}/state",
            data=payload,
            headers={"content-type": "application/json"},
            timeout=1,
        )
        response.raise_for_status()

    def _bind_socket(self) -> socket.socket | None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((_HOST, self._port))
            sock.listen()
            sock.setblocking(False)
            return sock
        except OSError as exc:
            sock.close()
            if exc.errno == errno.EADDRINUSE:
                return None
            raise


def web_component() -> ComponentFactory:
    return lambda context: WebComponent(bus=context.bus)
