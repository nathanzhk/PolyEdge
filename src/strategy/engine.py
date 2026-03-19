from __future__ import annotations

from events import DesiredPositionEvent
from strategy.strategy import Strategy


class StrategyEngine:
    def __init__(
        self,
        strategy: Strategy,
    ) -> None:
        self._strategy = strategy

    async def evaluate_once(self) -> DesiredPositionEvent | None:
        return
