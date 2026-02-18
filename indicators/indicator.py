from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class MacdValue:
    value: float
    signal: float
    histogram: float
