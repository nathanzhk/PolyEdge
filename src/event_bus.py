from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, TypeVar

from utils.logger import get_logger

T = TypeVar("T")

logger = get_logger("BUS")


class OverflowPolicy(Enum):
    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    RAISE = "raise"


class Subscription[T]:
    def __init__(
        self,
        event_types: type[Any] | tuple[type[Any], ...],
        *,
        name: str,
        maxsize: int,
        overflow: OverflowPolicy,
    ) -> None:
        self.event_types = event_types
        self.name = name
        self.queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self.overflow = overflow
        self.dropped_count = 0

    def matches(self, event: object) -> bool:
        return isinstance(event, self.event_types)

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        event = await self.queue.get()
        _log_queue_state("dequeue", self, event)
        return event


class EventBus:
    def __init__(self) -> None:
        self._subscriptions: list[Subscription[Any]] = []

    def subscribe(
        self,
        event_types: type[T] | tuple[type[Any], ...],
        *,
        name: str,
        maxsize: int = 100,
        overflow: OverflowPolicy = OverflowPolicy.BLOCK,
    ) -> Subscription[T]:
        subscription = Subscription[T](
            event_types,
            name=name,
            maxsize=maxsize,
            overflow=overflow,
        )
        self._subscriptions.append(subscription)
        return subscription

    async def publish(self, event: object) -> None:
        subscriptions = [
            subscription for subscription in self._subscriptions if subscription.matches(event)
        ]
        for subscription in subscriptions:
            await self._deliver(subscription, event)

    async def _deliver(self, subscription: Subscription[Any], event: object) -> None:
        if subscription.overflow == OverflowPolicy.BLOCK:
            await subscription.queue.put(event)
            _log_queue_state("enqueue", subscription, event)
            return

        if subscription.overflow == OverflowPolicy.DROP_NEWEST:
            try:
                subscription.queue.put_nowait(event)
                _log_queue_state("enqueue", subscription, event)
            except asyncio.QueueFull:
                subscription.dropped_count += 1
                _log_queue_state("drop_newest", subscription, event)
            return

        if subscription.overflow == OverflowPolicy.DROP_OLDEST:
            if subscription.queue.full():
                try:
                    dropped_event = subscription.queue.get_nowait()
                    subscription.dropped_count += 1
                    _log_queue_state("drop_oldest", subscription, dropped_event)
                except asyncio.QueueEmpty:
                    pass
            subscription.queue.put_nowait(event)
            _log_queue_state("enqueue", subscription, event)
            return

        if subscription.overflow == OverflowPolicy.RAISE:
            subscription.queue.put_nowait(event)
            _log_queue_state("enqueue", subscription, event)
            return


def _log_queue_state(action: str, subscription: Subscription[Any], event: object) -> None:
    maxsize = subscription.queue.maxsize
    maxsize_label = "unbounded" if maxsize <= 0 else str(maxsize)
    logger.debug(
        "queue=%s action=%s event=%s qsize=%d/%s dropped=%d overflow=%s",
        subscription.name,
        action,
        type(event).__name__,
        subscription.queue.qsize(),
        maxsize_label,
        subscription.dropped_count,
        subscription.overflow.value,
    )
