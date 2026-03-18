from __future__ import annotations

from typing import Protocol

from strategies.target import PositionTarget

from state.context import StrategyContext


class Strategy(Protocol):
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        raise NotImplementedError


class DefaultStrategy:
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        return None
