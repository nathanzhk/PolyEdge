from __future__ import annotations

import asyncio
import errno
import socket
from typing import TYPE_CHECKING

import uvicorn

from event_bus import EventBus, OverflowPolicy, Subscription
from events import RuntimeStateEvent
from utils.logger import get_logger
from web.server import app, broadcast_loop, enqueue_event

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
        tasks.create_task(self._event_loop(state_events))
        tasks.create_task(broadcast_loop())
        tasks.create_task(self._serve())

    async def _event_loop(self, events: Subscription[RuntimeStateEvent]) -> None:
        async for event in events:
            enqueue_event(event)

    async def _serve(self) -> None:
        config = uvicorn.Config(
            app,
            host=_HOST,
            port=self._port,
            log_level="warning",
            lifespan="off",
        )
        while True:
            sock = self._bind_socket()
            if sock is None:
                logger.info("port %d in use, retrying in 2s", self._port)
                await asyncio.sleep(2)
                continue

            try:
                server = uvicorn.Server(config)
                await server.serve(sockets=[sock])
                return
            finally:
                if sock.fileno() != -1:
                    sock.close()

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
