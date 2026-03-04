from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from markets.base import Market, Token
from strategies.context import StrategyContext


class ExecutionStyle(StrEnum):
    URGENT = "urgent"
    PASSIVE = "passive"


@dataclass(slots=True, frozen=True)
class PositionTarget:
    market: Market
    token: Token
    shares: float
    price: float
    style: ExecutionStyle = ExecutionStyle.PASSIVE


class Strategy(Protocol):
    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        raise NotImplementedError
