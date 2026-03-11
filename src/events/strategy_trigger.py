from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class StrategyTriggerEvent:
    ts_ms: int
    reason: str
