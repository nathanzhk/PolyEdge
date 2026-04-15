from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from event_bus import EventBus
from events import (
    CryptoOHLCVEvent,
    CryptoQuoteEvent,
    MarketOrderEvent,
    MarketQuoteEvent,
    MarketTradeEvent,
)
from execution.clients import MakerTradeClient, TakerTradeClient
from execution.component import execution_component
from execution.engine import ExecutionEngine
from markets.base import Market
from paper import (
    PaperExchangeSimulator,
    PaperMakerTradeClient,
    PaperTakerTradeClient,
    PaperTradeStream,
    paper_match_component,
)
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
from web import web_component

ExecutionMode = Literal["live", "paper"]


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    bus: EventBus
    market: Market
    strategy_engine: StrategyEngine
    execution_engine: ExecutionEngine
    paper_simulator: PaperExchangeSimulator | None
    market_quote_stream: AsyncIterable[MarketQuoteEvent]
    market_trade_stream: AsyncIterable[MarketOrderEvent | MarketTradeEvent]
    crypto_quote_stream: AsyncIterable[CryptoQuoteEvent]
    crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent]


class RuntimeComponent(Protocol):
    def start(self, tasks: asyncio.TaskGroup) -> None:
        raise NotImplementedError


ComponentFactory = Callable[[RuntimeContext], RuntimeComponent]


class Runtime:
    def __init__(
        self,
        *,
        market: Market,
        symbol: str,
        strategy: Strategy,
        execution_mode: ExecutionMode = "live",
    ) -> None:
        self._component_factories: list[ComponentFactory] = []
        self._execution_mode = execution_mode
        bus = EventBus()
        paper_simulator: PaperExchangeSimulator | None = None
        if execution_mode == "paper":
            paper_simulator = PaperExchangeSimulator()
            maker_client = PaperMakerTradeClient(paper_simulator)
            taker_client = PaperTakerTradeClient(paper_simulator)
            market_trade_stream = PaperTradeStream(paper_simulator)
        else:
            maker_client = MakerTradeClient()
            taker_client = TakerTradeClient()
            market_trade_stream = MarketTradeStream(maker_client.get_credentials())
        maker_client.warm_up(market)
        taker_client.warm_up(market)
        self._context = RuntimeContext(
            bus=bus,
            market=market,
            strategy_engine=StrategyEngine(strategy),
            execution_engine=ExecutionEngine(
                market,
                maker_client,
                taker_client,
                event_publisher=bus,
            ),
            paper_simulator=paper_simulator,
            market_quote_stream=MarketQuoteStream(market),
            market_trade_stream=market_trade_stream,
            crypto_quote_stream=CryptoQuoteStream(symbol),
            crypto_ohlcv_stream=CryptoOHLCVStream(symbol),
        )
        self._register_components()

    async def run(self) -> None:
        try:
            async with asyncio.TaskGroup() as tasks:
                for component_factory in self._component_factories:
                    component_factory(self._context).start(tasks)
        finally:
            if self._context.paper_simulator is not None:
                self._context.paper_simulator.close()

    def _register_components(self) -> None:
        self._register_component(strategy_component())
        self._register_component(execution_component())
        self._register_component(market_quote_component())
        if self._execution_mode == "paper":
            self._register_component(paper_match_component())
        self._register_component(market_trade_component())
        self._register_component(crypto_quote_component())
        self._register_component(crypto_ohlcv_component())
        self._register_component(runtime_state_component())
        self._register_component(web_component())

    def _register_component(self, factory: ComponentFactory) -> None:
        self._component_factories.append(factory)
