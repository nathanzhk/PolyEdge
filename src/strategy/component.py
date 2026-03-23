from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events import RuntimeStateEvent
from strategy.engine import StrategyEngine

if TYPE_CHECKING:
    from app import ComponentFactory


class StrategyComponent:
    def __init__(self, *, bus: EventBus, engine: StrategyEngine) -> None:
        self._bus = bus
        self._engine = engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        runtime_state_events = self._bus.subscribe(
            RuntimeStateEvent,
            name="strategy-engine.runtime-state",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._strategy_loop(runtime_state_events))

    async def _strategy_loop(self, events: Subscription[RuntimeStateEvent]) -> None:
        async for runtime_state in events:
            try:
                position_event = await self._engine.evaluate(runtime_state)
                if position_event is not None:
                    await self._bus.publish(position_event)
            except Exception:
                raise


def strategy_component() -> ComponentFactory:
    return lambda context: StrategyComponent(
        bus=context.bus,
        engine=context.strategy_engine,
    )
