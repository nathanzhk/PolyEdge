from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events import DesiredPositionEvent, MarketOrderEvent, MarketTradeEvent
from execution.engine import ExecutionEngine

if TYPE_CHECKING:
    from app import ComponentFactory


class ExecutionComponent:
    def __init__(self, *, bus: EventBus, engine: ExecutionEngine) -> None:
        self._bus = bus
        self._engine = engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_order_events = self._bus.subscribe(
            MarketOrderEvent,
            name="execution-engine.market-order",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        tasks.create_task(self._market_order_loop(market_order_events))

        market_trade_events = self._bus.subscribe(
            MarketTradeEvent,
            name="execution-engine.market-trade",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        tasks.create_task(self._market_trade_loop(market_trade_events))

        desired_position_events = self._bus.subscribe(
            DesiredPositionEvent,
            name="execution-engine.desired-position",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._desired_position_loop(desired_position_events))

    async def _market_order_loop(self, events: Subscription[MarketOrderEvent]) -> None:
        async for order in events:
            await self._engine.handle_order_event(order)

    async def _market_trade_loop(self, events: Subscription[MarketTradeEvent]) -> None:
        async for trade in events:
            position_event = await self._engine.handle_trade_event(trade)
            if position_event is not None:
                await self._bus.publish(position_event)

    async def _desired_position_loop(self, targets: Subscription[DesiredPositionEvent]) -> None:
        async for target in targets:
            await self._engine.handle_desired_position(target)


def execution_component() -> ComponentFactory:
    return lambda context: ExecutionComponent(
        bus=context.bus,
        engine=context.execution_engine,
    )
