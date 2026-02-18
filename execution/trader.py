from __future__ import annotations

import asyncio
from typing import Protocol

from execution.trade_client import TradeClient
from execution.trade_intent import TradeIntent


class Trader(Protocol):
    async def execute(self, intent: TradeIntent) -> str | None:
        raise NotImplementedError


class TradeClientExecutor:
    def __init__(self, client: TradeClient) -> None:
        self._client = client

    async def execute(self, intent: TradeIntent) -> str | None:
        if intent.side == "buy":
            return await asyncio.to_thread(
                self._client.buy,
                intent.token,
                intent.shares,
                intent.price,
            )
        if intent.side == "sell":
            return await asyncio.to_thread(
                self._client.sell,
                intent.token,
                intent.shares,
                intent.price,
            )
        raise ValueError(f"invalid order side: {intent.side}")
