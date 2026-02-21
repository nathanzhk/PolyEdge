from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Protocol

from models.market import Market, Token
from strategies.context import StrategyContext


@dataclass(slots=True, frozen=True)
class PositionTarget:
    target_id: str
    market: Market
    token: Token
    shares: float
    price: float
    post_only: bool = True
    ttl_s: float = 2.0
    replace_price_gap: float = 0.05
    replace_on_ttl: bool = True
    max_replace_count: int = 20


@dataclass(slots=True, frozen=True)
class StrategyDecision:
    window_id: str | None = None
    targets: list[PositionTarget] = field(default_factory=list)

    @classmethod
    def empty(cls, window_id: str | None = None) -> StrategyDecision:
        return cls(window_id=window_id)

    @classmethod
    def target(cls, target: PositionTarget) -> StrategyDecision:
        return cls(window_id=target.market.id, targets=[target])

    @property
    def latest_target(self) -> PositionTarget | None:
        return self.targets[-1] if self.targets else None


class Strategy(Protocol):
    def on_market(
        self, context: StrategyContext
    ) -> StrategyDecision | Awaitable[StrategyDecision | None] | None:
        raise NotImplementedError
