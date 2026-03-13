from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_price import CryptoPriceEvent
from events.market_quote import MarketQuoteEvent
from execution.engine import ExecutionEngine
from feeds.market_trade import MarketUserEvent
from runtime.components import (
    CryptoOHLCVSourceComponent,
    CryptoPriceSourceComponent,
    ExecutionComponent,
    MarketQuoteSourceComponent,
    MarketStateComponent,
    MarketTradeSourceComponent,
    RuntimeComponent,
    StrategyComponent,
)
from runtime.event_bus import EventBus
from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from runtime.strategy_engine import StrategyEngine
from strategies.strategy import Strategy


class Runner:
    def __init__(
        self,
        *,
        market_quote_stream: AsyncIterable[MarketQuoteEvent],
        market_trade_stream: AsyncIterable[MarketUserEvent] | None = None,
        crypto_price_stream: AsyncIterable[CryptoPriceEvent],
        crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent] | None = None,
        strategy: Strategy,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._market_quote_stream = market_quote_stream
        self._market_trade_stream = market_trade_stream
        self._crypto_price_stream = crypto_price_stream
        self._crypto_ohlcv_stream = crypto_ohlcv_stream
        self._market_state = MarketState()
        self._indicator_state = IndicatorState()
        self._indicator_engine = IndicatorEngine(self._indicator_state)
        self._strategy = strategy
        self._execution_engine = execution_engine
        self._strategy_engine = StrategyEngine(
            market_state=self._market_state,
            indicator_state=self._indicator_state,
            execution_engine=self._execution_engine,
            strategy=self._strategy,
        )
        self._bus = EventBus()

    async def run(self) -> None:
        components = self._components()
        async with asyncio.TaskGroup() as tasks:
            for component in components:
                component.start(tasks)

    def _components(self) -> list[RuntimeComponent]:
        components: list[RuntimeComponent] = [
            MarketQuoteSourceComponent(
                stream=self._market_quote_stream,
                bus=self._bus,
            ),
            CryptoPriceSourceComponent(
                stream=self._crypto_price_stream,
                bus=self._bus,
            ),
            MarketStateComponent(
                bus=self._bus,
                market_state=self._market_state,
                indicator_engine=self._indicator_engine,
            ),
            ExecutionComponent(
                bus=self._bus,
                execution_engine=self._execution_engine,
            ),
            StrategyComponent(
                bus=self._bus,
                strategy_engine=self._strategy_engine,
            ),
        ]
        if self._market_trade_stream is not None:
            components.append(
                MarketTradeSourceComponent(
                    stream=self._market_trade_stream,
                    bus=self._bus,
                )
            )
        if self._crypto_ohlcv_stream is not None:
            components.append(
                CryptoOHLCVSourceComponent(
                    stream=self._crypto_ohlcv_stream,
                    bus=self._bus,
                )
            )
        return components
