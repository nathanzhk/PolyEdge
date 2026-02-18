from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable

from execution.trade_intent import TradeIntent
from execution.trader import Trader
from runtime.indicator_engine import IndicatorEngine
from runtime.indicator_state import IndicatorState
from runtime.market_state import MarketState
from strategies.context import StrategyContext
from strategies.strategy import Strategy
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger

logger = get_logger("RUNTIME")


async def market_price_loop(
    stream: AsyncIterable[MarketPriceEvent],
    market_state: MarketState,
) -> None:
    async for price in stream:
        await market_state.update_market_price(price)


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
    intent_queue: asyncio.Queue[TradeIntent],
    strategy: Strategy,
    *,
    interval_s: float,
) -> None:
    while True:
        try:
            context = StrategyContext(
                market=await market_state.latest_state(),
                indicators=await indicator_state.latest_state(),
            )
            intent = strategy.on_market(context)
            if inspect.isawaitable(intent):
                intent = await intent
            if intent is not None:
                _try_put_intent(intent_queue, intent)
        except Exception:
            logger.exception("strategy failed")
            raise

        await asyncio.sleep(interval_s)


async def execution_loop(
    intent_queue: asyncio.Queue[TradeIntent],
    trader: Trader,
) -> None:
    while True:
        intent = await intent_queue.get()
        try:
            await trader.execute(intent)
        except Exception:
            logger.exception("execution failed")
            raise
        finally:
            intent_queue.task_done()


def _try_put_intent(queue: asyncio.Queue[TradeIntent], intent: TradeIntent) -> None:
    if queue.full():
        logger.warning("intent queue is full; dropping intent")
        return
    queue.put_nowait(intent)
