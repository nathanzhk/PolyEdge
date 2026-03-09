from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from time import perf_counter_ns

from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from strategies.context import StrategyContext
from strategies.strategy import PositionTarget, Strategy
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from streams.market_trade_stream import MarketUserEvent
from trade.execution_engine import ExecutionEngine
from utils.logger import get_logger
from utils.stats import LatencyStats

logger = get_logger("RUNTIME")


async def market_price_loop(
    stream: AsyncIterable[MarketPriceEvent],
    market_state: MarketState,
) -> None:
    latency_stats = LatencyStats("market tick", logger)
    async for price in stream:
        started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
        await market_state.update_market_price(price)
        latency_stats.record_ns(started_at_ns)


async def market_trade_loop(
    stream: AsyncIterable[MarketUserEvent],
    execution_engine: ExecutionEngine,
) -> None:
    async for event in stream:
        await execution_engine.handle_event(event)


async def crypto_price_loop(
    stream: AsyncIterable[CryptoPriceEvent],
    market_state: MarketState,
    indicator_engine: IndicatorEngine,
) -> None:
    latency_stats = LatencyStats("crypto tick", logger)
    async for price in stream:
        started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
        await market_state.update_crypto_price(price)
        await indicator_engine.on_crypto_price(price)
        latency_stats.record_ns(started_at_ns)


async def crypto_ohlcv_loop(
    stream: AsyncIterable[CryptoOHLCVEvent],
    market_state: MarketState,
    indicator_engine: IndicatorEngine,
) -> None:
    async for ohlcv in stream:
        await market_state.update_crypto_ohlcv(ohlcv)
        await indicator_engine.on_crypto_ohlcv(ohlcv)


async def strategy_loop(
    market_state: MarketState,
    indicator_state: IndicatorState,
    strategy: Strategy,
    latest_target: asyncio.Queue[PositionTarget],
    execution_engine: ExecutionEngine,
) -> None:
    while True:
        await asyncio.sleep(0.05)
        try:
            context = StrategyContext(
                market=await market_state.latest_state(),
                position=await execution_engine.latest_state(),
                indicators=await indicator_state.latest_state(),
            )
            target = strategy.on_market(context)
            if target is not None:
                _set_latest_target(latest_target, target)
        except Exception:
            logger.exception("strategy failed")
            raise


async def execution_loop(
    latest_target: asyncio.Queue[PositionTarget],
    execution_engine: ExecutionEngine,
) -> None:
    while True:
        target = await latest_target.get()
        await execution_engine.handle_position_target(target)


def _set_latest_target(
    latest_target: asyncio.Queue[PositionTarget], target: PositionTarget
) -> None:
    if latest_target.full():
        try:
            latest_target.get_nowait()
        except asyncio.QueueEmpty:
            pass
    latest_target.put_nowait(target)
