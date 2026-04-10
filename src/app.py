from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from typing import Protocol

from clients.polymarket_clob import MakerTradeClient, TakerTradeClient
from event_bus import EventBus
from events import (
    CryptoOHLCVEvent,
    CryptoQuoteEvent,
    MarketOrderEvent,
    MarketQuoteEvent,
    MarketTradeEvent,
)
from execution.component import execution_component
from execution.engine import ExecutionEngine
from markets.base import Market
from state.component import runtime_state_component
from strategy.component import strategy_component
from strategy.engine import StrategyEngine
from strategy.strategy import Strategy
from streams import (
    CryptoOHLCVStream,
    CryptoQuoteStream,
    MarketQuoteStream,
    MarketTradeStream,
    crypto_ohlcv_component,
    crypto_quote_component,
    market_quote_component,
    market_trade_component,
)


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    bus: EventBus
    market: Market
    strategy_engine: StrategyEngine
    execution_engine: ExecutionEngine
    market_quote_stream: AsyncIterable[MarketQuoteEvent]
    market_trade_stream: AsyncIterable[MarketOrderEvent | MarketTradeEvent]
    crypto_quote_stream: AsyncIterable[CryptoQuoteEvent]
    crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent]


class RuntimeComponent(Protocol):
    def start(self, tasks: asyncio.TaskGroup) -> None:
        raise NotImplementedError


ComponentFactory = Callable[[RuntimeContext], RuntimeComponent]


class Runtime:
    def __init__(self, *, market: Market, symbol: str, strategy: Strategy) -> None:
        self._component_factories: list[ComponentFactory] = []
        bus = EventBus()
        maker_client = MakerTradeClient()
        taker_client = TakerTradeClient()
        maker_client.warm_up(market)
        taker_client.warm_up(market)
        self._context = RuntimeContext(
            bus=bus,
            market=market,
            strategy_engine=StrategyEngine(strategy),
            execution_engine=ExecutionEngine(
                market, maker_client, taker_client, event_publisher=bus
            ),
            market_quote_stream=MarketQuoteStream(market),
            market_trade_stream=MarketTradeStream(maker_client.get_credentials()),
            crypto_quote_stream=CryptoQuoteStream(symbol),
            crypto_ohlcv_stream=CryptoOHLCVStream(symbol),
        )
        self._register_components()

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for component_factory in self._component_factories:
                component_factory(self._context).start(tasks)

    def _register_components(self) -> None:
        self._register_component(strategy_component())
        self._register_component(execution_component())
        self._register_component(market_quote_component())
        self._register_component(market_trade_component())
        self._register_component(crypto_quote_component())
        self._register_component(crypto_ohlcv_component())
        self._register_component(runtime_state_component())

    def _register_component(self, factory: ComponentFactory) -> None:
        self._component_factories.append(factory)
