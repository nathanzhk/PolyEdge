from __future__ import annotations

import asyncio
from time import perf_counter_ns
from typing import TYPE_CHECKING

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events import CryptoOHLCVEvent, CryptoQuoteEvent, MarketQuoteEvent, RuntimeStateEvent
from state.runtime_state import RuntimeState
from utils.logger import get_logger
from utils.stats import LatencyStats

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("RUNTIME")


class RuntimeStateComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        runtime_state: RuntimeState,
    ) -> None:
        self._bus = bus
        self._runtime_state = runtime_state

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
            await self._runtime_state.update_market_quote(quote)
            await self._bus.publish(RuntimeStateEvent(ts_ms=quote.ts_ms, reason="market_quote"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_quote_loop(self, events: Subscription[CryptoQuoteEvent]) -> None:
        latency_stats = LatencyStats("crypto tick", logger)
        async for quote in events:
            started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
            await self._runtime_state.update_crypto_quote(quote)
            await self._bus.publish(RuntimeStateEvent(ts_ms=quote.ts_ms, reason="crypto_quote"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_ohlcv_loop(self, events: Subscription[CryptoOHLCVEvent]) -> None:
        async for ohlcv in events:
            await self._runtime_state.update_crypto_ohlcv(ohlcv)
            await self._bus.publish(RuntimeStateEvent(ts_ms=ohlcv.ts_ms, reason="crypto_ohlcv"))


def runtime_state_component() -> ComponentFactory:
    return lambda context: RuntimeStateComponent(
        bus=context.bus,
        runtime_state=context.runtime_state,
    )
