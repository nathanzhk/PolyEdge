from __future__ import annotations

from typing import Protocol

from events import DesiredPositionEvent, RuntimeStateEvent
from utils.logger import get_logger

logger = get_logger("DEFAULT STRATEGY")


class Strategy(Protocol):
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        raise NotImplementedError


class DefaultStrategy:
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        return None
