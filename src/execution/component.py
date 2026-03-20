from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events import DesiredPositionEvent, MarketOrderEvent, MarketTradeEvent, RuntimeStateEvent
from execution.engine import ExecutionEngine

if TYPE_CHECKING:
    from app import ComponentFactory


class ExecutionComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._bus = bus
        self._execution_engine = execution_engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_trade_events = self._bus.subscribe(
            (MarketOrderEvent, MarketTradeEvent),
            name="execution.market-user",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        position_targets = self._bus.subscribe(
            DesiredPositionEvent,
            name="execution.position-targets",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )

        tasks.create_task(self._market_trade_loop(market_trade_events))
        tasks.create_task(self._position_target_loop(position_targets))

    async def _market_trade_loop(
        self,
        events: Subscription[MarketOrderEvent | MarketTradeEvent],
    ) -> None:
        async for event in events:
            await self._execution_engine.handle_event(event)
            await self._bus.publish(RuntimeStateEvent(ts_ms=event.ts_ms, reason="market_trade"))

    async def _position_target_loop(self, targets: Subscription[DesiredPositionEvent]) -> None:
        async for target in targets:
            await self._execution_engine.handle_position_target(target)


def execution_component() -> ComponentFactory:
    return lambda context: ExecutionComponent(
        bus=context.bus,
        execution_engine=context.execution_engine,
    )
