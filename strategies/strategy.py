from __future__ import annotations

from typing import Protocol

from strategies.context import StrategyContext
from strategies.target import PositionTarget


class Strategy(Protocol):
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        raise NotImplementedError


class DefaultStrategy:
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        return None
