from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, TypeVar

T = TypeVar("T")


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
        return await self.queue.get()


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
            return

        if subscription.overflow == OverflowPolicy.DROP_NEWEST:
            try:
                subscription.queue.put_nowait(event)
            except asyncio.QueueFull:
                subscription.dropped_count += 1
            return

        if subscription.overflow == OverflowPolicy.DROP_OLDEST:
            if subscription.queue.full():
                try:
                    subscription.queue.get_nowait()
                    subscription.dropped_count += 1
                except asyncio.QueueEmpty:
                    pass
            subscription.queue.put_nowait(event)
            return

        if subscription.overflow == OverflowPolicy.RAISE:
            subscription.queue.put_nowait(event)
            return
