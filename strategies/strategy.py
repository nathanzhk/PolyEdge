from __future__ import annotations

from collections.abc import Awaitable
from typing import Protocol

from execution.trade_intent import TradeIntent
from strategies.context import StrategyContext


class Strategy(Protocol):
    def on_market(
        self, context: StrategyContext
    ) -> TradeIntent | Awaitable[TradeIntent | None] | None:
        raise NotImplementedError
