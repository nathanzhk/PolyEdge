from __future__ import annotations

from typing import Protocol

from strategies import PositionTarget, StrategyContext


class Strategy(Protocol):
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        raise NotImplementedError


class DefaultStrategy:
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        return None
