from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

from execution.trade_intent import TradeIntent
from execution.trader import Trader
from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from runtime.workers import (
    crypto_ohlcv_loop,
    crypto_price_loop,
    execution_loop,
    market_price_loop,
    strategy_loop,
)
from strategies.strategy import Strategy
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent


class Runner:
    def __init__(
        self,
        *,
        market_stream: AsyncIterable[MarketPriceEvent],
        crypto_price_stream: AsyncIterable[CryptoPriceEvent],
        crypto_ohlcv_stream: AsyncIterable[CryptoOHLCVEvent] | None = None,
        strategy: Strategy,
        trader: Trader,
        intent_queue_size: int = 100,
        strategy_interval_s: float = 0.1,
    ) -> None:
        self._market_stream = market_stream
        self._crypto_price_stream = crypto_price_stream
        self._crypto_ohlcv_stream = crypto_ohlcv_stream
        self._strategy = strategy
        self._trader = trader
        self._strategy_interval_s = strategy_interval_s
        self._market_state = MarketState()
        self._indicator_state = IndicatorState()
        self._indicator_engine = IndicatorEngine(self._indicator_state)
        self._intent_queue: asyncio.Queue[TradeIntent] = asyncio.Queue(maxsize=intent_queue_size)

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                market_price_loop(
                    self._market_stream,
                    self._market_state,
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
                    self._intent_queue,
                    self._strategy,
                    interval_s=self._strategy_interval_s,
                )
            )
            tasks.create_task(
                execution_loop(
                    self._intent_queue,
                    self._trader,
                )
            )
