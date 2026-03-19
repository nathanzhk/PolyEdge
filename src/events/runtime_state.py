from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class RuntimeStateEvent:
    ts_ms: int
    reason: str
