from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable

from event_bus import EventBus
from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_quote import CryptoQuoteEvent
from events.market_order import MarketOrderEvent
from events.market_quote import MarketQuoteEvent
from events.market_trade import MarketTradeEvent
from registry import ComponentFactory


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


class CryptoQuoteSourceComponent(_StreamSourceComponent[CryptoQuoteEvent]):
    pass


class MarketTradeSourceComponent(_StreamSourceComponent[MarketOrderEvent | MarketTradeEvent]):
    pass


class CryptoOHLCVSourceComponent(_StreamSourceComponent[CryptoOHLCVEvent]):
    pass


def market_quote_component() -> ComponentFactory:
    return lambda ctx: MarketQuoteSourceComponent(
        stream=ctx.market_quote_stream,
        bus=ctx.bus,
    )


def crypto_quote_component() -> ComponentFactory:
    return lambda ctx: CryptoQuoteSourceComponent(
        stream=ctx.crypto_quote_stream,
        bus=ctx.bus,
    )


def market_trade_component() -> ComponentFactory:
    return lambda ctx: MarketTradeSourceComponent(
        stream=ctx.market_trade_stream,
        bus=ctx.bus,
    )


def crypto_ohlcv_component() -> ComponentFactory:
    return lambda ctx: CryptoOHLCVSourceComponent(
        stream=ctx.crypto_ohlcv_stream,
        bus=ctx.bus,
    )
