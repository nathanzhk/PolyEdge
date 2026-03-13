from __future__ import annotations

from execution.engine import ExecutionEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from strategies.context import StrategyContext
from strategies.strategy import Strategy
from strategies.target import PositionTarget


class StrategyEngine:
    def __init__(
        self,
        *,
        market_state: MarketState,
        indicator_state: IndicatorState,
        execution_engine: ExecutionEngine,
        strategy: Strategy,
    ) -> None:
        self._market_state = market_state
        self._indicator_state = indicator_state
        self._execution_engine = execution_engine
        self._strategy = strategy

    async def evaluate_once(self) -> PositionTarget | None:
        context = StrategyContext(
            market=await self._market_state.latest_state(),
            position=await self._execution_engine.latest_state(),
            indicators=await self._indicator_state.latest_state(),
        )
        return self._strategy.on_market(context)
