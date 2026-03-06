from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from markets.base import Market, Token


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
