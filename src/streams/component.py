from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING

from event_bus import EventBus
from events import (
    CryptoOHLCVEvent,
    CryptoQuoteEvent,
    MarketOrderEvent,
    MarketQuoteEvent,
    MarketTradeEvent,
)

if TYPE_CHECKING:
    from app import ComponentFactory


class _StreamComponent[T]:
    def __init__(self, *, bus: EventBus, stream: AsyncIterable[T]) -> None:
        self._bus = bus
        self._stream = stream

    def start(self, tasks: asyncio.TaskGroup) -> None:
        tasks.create_task(self._run())

    async def _run(self) -> None:
        async for event in self._stream:
            await self._bus.publish(event)


def market_quote_component() -> ComponentFactory:
    return lambda context: _StreamComponent[MarketQuoteEvent](
        bus=context.bus,
        stream=context.market_quote_stream,
    )


def market_trade_component() -> ComponentFactory:
    return lambda context: _StreamComponent[MarketOrderEvent | MarketTradeEvent](
        bus=context.bus,
        stream=context.market_trade_stream,
    )


def crypto_quote_component() -> ComponentFactory:
    return lambda context: _StreamComponent[CryptoQuoteEvent](
        bus=context.bus,
        stream=context.crypto_quote_stream,
    )


def crypto_ohlcv_component() -> ComponentFactory:
    return lambda context: _StreamComponent[CryptoOHLCVEvent](
        bus=context.bus,
        stream=context.crypto_ohlcv_stream,
    )
