from __future__ import annotations

import asyncio
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
            host="0.0.0.0",
            port=self._port,
            log_level="warning",
        )
        server = uvicorn.Server(config)
        logger.info("dashboard http://localhost:%d", self._port)
        try:
            await server.serve()
        except SystemExit:
            logger.warning("port %d already in use; dashboard disabled for this worker", self._port)
        except OSError as exc:
            logger.warning("dashboard failed to start: %s", exc)


def web_component() -> ComponentFactory:
    return lambda context: WebComponent(bus=context.bus)
