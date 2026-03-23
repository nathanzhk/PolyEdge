from __future__ import annotations

from events import DesiredPositionEvent, RuntimeStateEvent
from strategy.strategy import Strategy


class StrategyEngine:
    def __init__(self, strategy: Strategy) -> None:
        self._strategy = strategy

    async def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        return self._strategy.evaluate(state)
