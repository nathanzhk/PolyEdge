from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from time import perf_counter, perf_counter_ns
from typing import Protocol

from events import (
    CryptoOHLCVEvent,
    CryptoPriceEvent,
    MarketOrderEvent,
    MarketQuoteEvent,
    MarketTradeEvent,
    StrategyTriggerEvent,
)
from execution.engine import ExecutionEngine
from feeds.market_trade import MarketUserEvent
from infra import now_ts_ms
from infra.logger import get_logger
from infra.stats import LatencyStats
from runtime.event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from runtime.indicator_engine import IndicatorEngine
from runtime.market_state import MarketState
from runtime.strategy_engine import StrategyEngine
from strategies import PositionTarget

logger = get_logger("RUNTIME")


class RuntimeComponent(Protocol):
    def start(self, tasks: asyncio.TaskGroup) -> None:
        raise NotImplementedError


class _StreamSourceComponent[T]:
    def __init__(
        self,
        *,
        stream: AsyncIterable[T],
        bus: EventBus,
    ) -> None:
        self._stream = stream
        self._bus = bus

    def start(self, tasks: asyncio.TaskGroup) -> None:
        tasks.create_task(self._run())

    async def _run(self) -> None:
        async for event in self._stream:
            await self._bus.publish(event)


class MarketQuoteSourceComponent(_StreamSourceComponent[MarketQuoteEvent]):
    pass


class CryptoPriceSourceComponent(_StreamSourceComponent[CryptoPriceEvent]):
    pass


class MarketTradeSourceComponent(_StreamSourceComponent[MarketUserEvent]):
    pass


class CryptoOHLCVSourceComponent(_StreamSourceComponent[CryptoOHLCVEvent]):
    pass


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
        crypto_price_events = self._bus.subscribe(
            CryptoPriceEvent,
            name="market-state.crypto-price",
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
        tasks.create_task(self._crypto_price_loop(crypto_price_events))
        tasks.create_task(self._crypto_ohlcv_loop(crypto_ohlcv_events))

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        latency_stats = LatencyStats("market tick", logger)
        async for quote in events:
            started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
            await self._market_state.update_market_quote(quote)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=quote.ts_ms, reason="market_quote"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_price_loop(self, events: Subscription[CryptoPriceEvent]) -> None:
        latency_stats = LatencyStats("crypto tick", logger)
        async for price in events:
            started_at_ns = perf_counter_ns() if latency_stats.enabled else 0
            await self._market_state.update_crypto_price(price)
            await self._indicator_engine.on_crypto_price(price)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=price.ts_ms, reason="crypto_price"))
            latency_stats.record_ns(started_at_ns)

    async def _crypto_ohlcv_loop(self, events: Subscription[CryptoOHLCVEvent]) -> None:
        async for ohlcv in events:
            await self._market_state.update_crypto_ohlcv(ohlcv)
            await self._indicator_engine.on_crypto_ohlcv(ohlcv)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=ohlcv.ts_ms, reason="crypto_ohlcv"))


class ExecutionComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._bus = bus
        self._execution_engine = execution_engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_trade_events = self._bus.subscribe(
            (MarketOrderEvent, MarketTradeEvent),
            name="execution.market-user",
            maxsize=1000,
            overflow=OverflowPolicy.BLOCK,
        )
        position_targets = self._bus.subscribe(
            PositionTarget,
            name="execution.position-targets",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )

        tasks.create_task(self._market_trade_loop(market_trade_events))
        tasks.create_task(self._position_target_loop(position_targets))

    async def _market_trade_loop(
        self,
        events: Subscription[MarketOrderEvent | MarketTradeEvent],
    ) -> None:
        async for event in events:
            await self._execution_engine.handle_event(event)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=event.ts_ms, reason="market_trade"))

    async def _position_target_loop(self, targets: Subscription[PositionTarget]) -> None:
        async for target in targets:
            await self._execution_engine.handle_position_target(target)


class StrategyComponent:
    def __init__(
        self,
        *,
        bus: EventBus,
        strategy_engine: StrategyEngine,
        timer_interval_s: float = 0.05,
        min_interval_s: float = 0.05,
    ) -> None:
        self._bus = bus
        self._strategy_engine = strategy_engine
        self._timer_interval_s = timer_interval_s
        self._min_interval_s = min_interval_s

    def start(self, tasks: asyncio.TaskGroup) -> None:
        triggers = self._bus.subscribe(
            StrategyTriggerEvent,
            name="strategy.triggers",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )

        tasks.create_task(self._timer_loop())
        tasks.create_task(self._strategy_loop(triggers))

    async def _timer_loop(self) -> None:
        while True:
            await asyncio.sleep(self._timer_interval_s)
            await self._bus.publish(StrategyTriggerEvent(ts_ms=now_ts_ms(), reason="timer"))

    async def _strategy_loop(self, triggers: Subscription[StrategyTriggerEvent]) -> None:
        next_allowed_at = 0.0
        async for _ in triggers:
            now = perf_counter()
            if now < next_allowed_at:
                await asyncio.sleep(next_allowed_at - now)
            next_allowed_at = perf_counter() + self._min_interval_s
            try:
                target = await self._strategy_engine.evaluate_once()
                if target is not None:
                    await self._bus.publish(target)
            except Exception:
                logger.exception("strategy failed")
                raise
