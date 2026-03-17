from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from typing import Protocol

from strategies.strategy_engine import StrategyEngine

from event_bus import EventBus
from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_quote import CryptoQuoteEvent
from events.market_order import MarketOrderEvent
from events.market_quote import MarketQuoteEvent
from events.market_trade import MarketTradeEvent
from execution.engine import ExecutionEngine
from indicators.indicator_engine import IndicatorEngine
from state.market_state import MarketState


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    bus: EventBus
    market_quote_stream: AsyncIterable[MarketQuoteEvent]
    crypto_quote_stream: AsyncIterable[CryptoQuoteEvent]
    market_trade_stream: AsyncIterable[MarketOrderEvent | MarketTradeEvent]
    crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent]
    market_state: MarketState
    indicator_engine: IndicatorEngine
    execution_engine: ExecutionEngine
    strategy_engine: StrategyEngine


class RuntimeComponent(Protocol):
    def start(self, tasks: asyncio.TaskGroup) -> None:
        raise NotImplementedError


ComponentFactory = Callable[[RuntimeContext], RuntimeComponent]
