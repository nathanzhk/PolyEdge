from __future__ import annotations

import asyncio
from time import perf_counter

from strategies.strategy_engine import StrategyEngine

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events.strategy_trigger import StrategyTriggerEvent
from registry import ComponentFactory
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("RUNTIME")


class StrategyComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        strategy_engine: StrategyEngine,
        timer_interval_s: float = 0.05,
        min_interval_s: float = 0.05,
    ) -> None:
        self._bus = bus
        self._strategy_engine = strategy_engine
        self._timer_interval_s = timer_interval_s
        self._min_interval_s = min_interval_s

    def start(self, tasks: asyncio.TaskGroup) -> None:
        triggers = self._bus.subscribe(
            StrategyTriggerEvent,
            name="strategy.triggers",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )

        tasks.create_task(self._timer_loop())
        tasks.create_task(self._strategy_loop(triggers))

    async def _timer_loop(self) -> None:
        while True:
            await asyncio.sleep(self._timer_interval_s)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=now_ts_ms(), reason="timer"))

    async def _strategy_loop(self, triggers: Subscription[StrategyTriggerEvent]) -> None:
        next_allowed_at = 0.0
        async for _ in triggers:
            now = perf_counter()
            if now < next_allowed_at:
                await asyncio.sleep(next_allowed_at - now)
            next_allowed_at = perf_counter() + self._min_interval_s
            try:
                target = await self._strategy_engine.evaluate_once()
                if target is not None:
                    await self._bus.publish(target)
            except Exception:
                logger.exception("strategy failed")
                raise


def strategy_component() -> ComponentFactory:
    return lambda ctx: StrategyComponent(
        bus=ctx.bus,
        strategy_engine=ctx.strategy_engine,
    )
