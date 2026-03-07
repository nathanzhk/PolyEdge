from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from runtime.workers import (
    crypto_ohlcv_loop,
    crypto_price_loop,
    execution_loop,
    market_price_loop,
    market_trade_loop,
    strategy_loop,
)
from strategies.strategy import PositionTarget, Strategy
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from streams.market_trade_stream import MarketUserEvent
from trade.execution_engine import ExecutionEngine


class Runner:
    def __init__(
        self,
        *,
        market_price_stream: AsyncIterable[MarketPriceEvent],
        market_trade_stream: AsyncIterable[MarketUserEvent] | None = None,
        crypto_price_stream: AsyncIterable[CryptoPriceEvent],
        crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent] | None = None,
        strategy: Strategy,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._market_price_stream = market_price_stream
        self._market_trade_stream = market_trade_stream
        self._crypto_price_stream = crypto_price_stream
        self._crypto_ohlcv_stream = crypto_ohlcv_stream
        self._market_state = MarketState()
        self._indicator_state = IndicatorState()
        self._indicator_engine = IndicatorEngine(self._indicator_state)
        self._strategy = strategy
        self._latest_target: asyncio.Queue[PositionTarget] = asyncio.Queue(maxsize=1)
        self._execution_engine = execution_engine

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                market_price_loop(
                    self._market_price_stream,
                    self._market_state,
                )
            )
            if self._market_trade_stream is not None:
                tasks.create_task(
                    market_trade_loop(
                        self._market_trade_stream,
                        self._execution_engine,
                    )
                )
            tasks.create_task(
                crypto_price_loop(
                    self._crypto_price_stream,
                    self._market_state,
                    self._indicator_engine,
                )
            )
            if self._crypto_ohlcv_stream is not None:
                tasks.create_task(
                    crypto_ohlcv_loop(
                        self._crypto_ohlcv_stream,
                        self._market_state,
                        self._indicator_engine,
                    )
                )
            tasks.create_task(
                strategy_loop(
                    self._market_state,
                    self._indicator_state,
                    self._strategy,
                    self._latest_target,
                    self._execution_engine,
                )
            )
            tasks.create_task(
                execution_loop(
                    self._latest_target,
                    self._execution_engine,
                )
            )
