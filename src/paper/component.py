from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from event_bus import EventBus, OverflowPolicy, Subscription
from events import MarketQuoteEvent
from paper.simulator import PaperExchangeSimulator

if TYPE_CHECKING:
    from app import ComponentFactory


class PaperMatchComponent:
    def __init__(self, *, bus: EventBus, simulator: PaperExchangeSimulator) -> None:
        self._bus = bus
        self._simulator = simulator

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_quote_events = self._bus.subscribe(
            MarketQuoteEvent,
            name="paper-match.market-quote",
            maxsize=100,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._market_quote_loop(market_quote_events))

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        async for quote in events:
            self._simulator.on_quote(quote)


def paper_match_component() -> ComponentFactory:
    return lambda context: PaperMatchComponent(
        bus=context.bus,
        simulator=_require_simulator(context.paper_simulator),
    )


def _require_simulator(simulator: PaperExchangeSimulator | None) -> PaperExchangeSimulator:
    if simulator is None:
        raise RuntimeError("paper simulator is required for paper match component")
    return simulator
