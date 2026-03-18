from __future__ import annotations

import asyncio
from time import perf_counter_ns

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_quote import CryptoQuoteEvent
from events.market_quote import MarketQuoteEvent
from events.strategy_trigger import StrategyTriggerEvent
from indicators.indicator_engine import IndicatorEngine
from registry import ComponentFactory
from state.market_state import MarketState
from utils.logger import get_logger
from utils.stats import LatencyStats

logger = get_logger("RUNTIME")


class MarketStateComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        market_state: MarketState,
        indicator_engine: IndicatorEngine,
    ) -> None:
        self._bus = bus
        self._market_state = market_state
        self._indicator_engine = indicator_engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_quote_events = self._bus.subscribe(
            MarketQuoteEvent,
            name="market-state.market-quote",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        crypto_quote_events = self._bus.subscribe(
            CryptoQuoteEvent,
            name="market-state.crypto-quote",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        crypto_ohlcv_events = self._bus.subscribe(
            CryptoOHLCVEvent,
            name="market-state.crypto-ohlcv",
            maxsize=100,
            overflow=OverflowPolicy.BLOCK,
        )

        tasks.create_task(self._market_quote_loop(market_quote_events))
        tasks.create_task(self._crypto_quote_loop(crypto_quote_events))
        tasks.create_task(self._crypto_ohlcv_loop(crypto_ohlcv_events))

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        latency_stats = LatencyStats("market tick", logger)
        async for quote in events:
            started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
            await self._market_state.update_market_quote(quote)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=quote.ts_ms, reason="market_quote"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_quote_loop(self, events: Subscription[CryptoQuoteEvent]) -> None:
        latency_stats = LatencyStats("crypto tick", logger)
        async for quote in events:
            started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
            await self._market_state.update_crypto_quote(quote)
            await self._indicator_engine.on_crypto_quote(quote)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=quote.ts_ms, reason="crypto_quote"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_ohlcv_loop(self, events: Subscription[CryptoOHLCVEvent]) -> None:
        async for ohlcv in events:
            await self._market_state.update_crypto_ohlcv(ohlcv)
            await self._indicator_engine.on_crypto_ohlcv(ohlcv)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=ohlcv.ts_ms, reason="crypto_ohlcv"))


def market_state_component() -> ComponentFactory:
    return lambda ctx: MarketStateComponent(
        bus=ctx.bus,
        market_state=ctx.market_state,
        indicator_engine=ctx.indicator_engine,
    )
