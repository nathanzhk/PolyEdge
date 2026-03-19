from __future__ import annotations

from typing import Protocol

from events import DesiredPositionEvent
from state.runtime_state import RuntimeState


class Strategy(Protocol):
    def on_market(self, context: RuntimeState) -> DesiredPositionEvent | None:
        raise NotImplementedError


class DefaultStrategy:
    def on_market(self, context: RuntimeState) -> DesiredPositionEvent | None:
        return None
