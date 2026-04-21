from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from events import DesiredPositionEvent, RuntimeStateEvent
from utils.logger import get_logger

logger = get_logger("DEFAULT STRATEGY")

StrategyOutput = Sequence[DesiredPositionEvent]


class Strategy(Protocol):
    def evaluate(self, state: RuntimeStateEvent) -> StrategyOutput:
        raise NotImplementedError


class DefaultStrategy:
    def evaluate(self, state: RuntimeStateEvent) -> StrategyOutput:
        return []
