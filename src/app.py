from __future__ import annotations

import asyncio

from strategies.component import strategy_component
from strategies.strategy import Strategy
from strategies.strategy_engine import StrategyEngine

from clients.polymarket_clob import MakerTradeClient, TakerTradeClient
from event_bus import EventBus
from execution.component import execution_component
from execution.engine import ExecutionEngine
from indicators.indicator_engine import IndicatorEngine
from indicators.indicator_state import IndicatorState
from markets.base import Market
from registry import ComponentFactory, RuntimeContext
from state.component import market_state_component
from state.market_state import MarketState
from streams.component import (
    crypto_ohlcv_component,
    crypto_quote_component,
    market_quote_component,
    market_trade_component,
)
from streams.crypto_ohlcv import CryptoOHLCVStream
from streams.crypto_quote import CryptoQuoteStream
from streams.market_quote import MarketQuoteStream
from streams.market_trade import MarketTradeStream


class TradingApp:
    def __init__(
        self,
        *,
        market: type[Market],
        symble: str,
        strategy: Strategy,
    ) -> None:
        self._component_factories: list[ComponentFactory] = []
        maker = MakerTradeClient()
        taker = TakerTradeClient()

        market_state = MarketState()
        indicator_state = IndicatorState()
        indicator_engine = IndicatorEngine(indicator_state)
        execution_engine = ExecutionEngine(maker, taker)
        strategy_engine = StrategyEngine(
            market_state=market_state,
            indicator_state=indicator_state,
            execution_engine=execution_engine,
            strategy=strategy,
        )
        self._context = RuntimeContext(
            bus=EventBus(),
            market_quote_stream=MarketQuoteStream(market),
            crypto_quote_stream=CryptoQuoteStream(symble),
            market_trade_stream=MarketTradeStream(maker.get_credentials()),
            crypto_ohlcv_stream=CryptoOHLCVStream(symble),
            market_state=market_state,
            indicator_engine=indicator_engine,
            execution_engine=execution_engine,
            strategy_engine=strategy_engine,
        )
        self._register_components()

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for factory in self._component_factories:
                factory(self._context).start(tasks)

    def _register_components(self) -> None:
        self._register_component(market_quote_component())
        self._register_component(crypto_quote_component())
        self._register_component(market_trade_component())
        self._register_component(crypto_ohlcv_component())
        self._register_component(market_state_component())
        self._register_component(execution_component())
        self._register_component(strategy_component())

    def _register_component(self, factory: ComponentFactory) -> None:
        self._component_factories.append(factory)
