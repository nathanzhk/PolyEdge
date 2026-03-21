from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from event_bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from events import CryptoOHLCVEvent, CryptoQuoteEvent, MarketQuoteEvent
from state.runtime_state import RuntimeState

if TYPE_CHECKING:
    from app import ComponentFactory


class RuntimeStateComponent:
    def __init__(self, *, bus: EventBus) -> None:
        self._bus = bus
        self._state = RuntimeState()

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_quote_events = self._bus.subscribe(
            MarketQuoteEvent,
            name="runtime-state.market-quote",
            maxsize=100,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._market_quote_loop(market_quote_events))

        crypto_quote_events = self._bus.subscribe(
            CryptoQuoteEvent,
            name="runtime-state.crypto-quote",
            maxsize=100,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._crypto_quote_loop(crypto_quote_events))

        crypto_ohlcv_events = self._bus.subscribe(
            CryptoOHLCVEvent,
            name="runtime-state.crypto-ohlcv",
            maxsize=100,
            overflow=OverflowPolicy.BLOCK,
        )
        tasks.create_task(self._crypto_ohlcv_loop(crypto_ohlcv_events))

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        async for quote in events:
            await self._state.update_market_quote(quote)

    async def _crypto_quote_loop(self, events: Subscription[CryptoQuoteEvent]) -> None:
        async for quote in events:
            await self._state.update_crypto_quote(quote)

    async def _crypto_ohlcv_loop(self, events: Subscription[CryptoOHLCVEvent]) -> None:
        async for ohlcv in events:
            await self._state.update_crypto_ohlcv(ohlcv)


def runtime_state_component() -> ComponentFactory:
    return lambda context: RuntimeStateComponent(bus=context.bus)
