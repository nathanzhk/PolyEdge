from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable

from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from strategies.context import StrategyContext
from strategies.strategy import Strategy, StrategyDecision
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from streams.market_trade_stream import MarketUserEvent
from trade.execution_engine import ExecutionEngine
from utils.logger import get_logger

logger = get_logger("RUNTIME")


async def market_price_loop(
    stream: AsyncIterable[MarketPriceEvent],
    market_state: MarketState,
) -> None:
    async for price in stream:
        await market_state.update_market_price(price)


async def market_trade_loop(
    stream: AsyncIterable[MarketUserEvent],
    execution_engine: ExecutionEngine,
) -> None:
    async for event in stream:
        await execution_engine.handle_market_event(event)


async def crypto_price_loop(
    stream: AsyncIterable[CryptoPriceEvent],
    market_state: MarketState,
    indicator_engine: IndicatorEngine,
) -> None:
    async for price in stream:
        await market_state.update_crypto_price(price)
        await indicator_engine.on_crypto_price(price)


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
    latest_decision: asyncio.Queue[StrategyDecision],
    execution_engine: ExecutionEngine,
) -> None:
    while True:
        await asyncio.sleep(0.05)
        try:
            context = StrategyContext(
                market=await market_state.latest_state(),
                indicators=await indicator_state.latest_state(),
                trade=await execution_engine.latest_state(),
            )
            decision = strategy.on_market(context)
            if inspect.isawaitable(decision):
                decision = await decision
            if decision is not None:
                _set_latest_decision(latest_decision, decision)
        except Exception:
            logger.exception("strategy failed")
            raise


async def execution_loop(
    latest_decision: asyncio.Queue[StrategyDecision],
    execution_engine: ExecutionEngine,
    *,
    watch_orders: bool = True,
) -> None:
    await execution_engine.run(latest_decision, watch_orders=watch_orders)


def _set_latest_decision(
    latest_decision: asyncio.Queue[StrategyDecision], decision: StrategyDecision
) -> None:
    if latest_decision.full():
        try:
            latest_decision.get_nowait()
        except asyncio.QueueEmpty:
            pass
    latest_decision.put_nowait(decision)
