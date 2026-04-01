from __future__ import annotations

import asyncio

from clients.polymarket_clob import TradeClient
from enums import Side
from events import CurrentPositionEvent, DesiredPositionEvent, MarketOrderEvent, MarketTradeEvent
from execution.manager import ManagedOrder, OrderManager
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("TRADE")

ZERO = 0.0
SHARES_EPSILON = 0.000001


class ExecutionEngine:
    def __init__(self, maker_client: TradeClient, taker_client: TradeClient) -> None:
        self._lock = asyncio.Lock()
        self._replace_ttl_s = 2.00
        self._replace_price_gap = 0.05
        self._replace_shares_gap = 0.10

        self._order_manager = OrderManager(maker_client, taker_client)

    async def handle_order_event(self, event: MarketOrderEvent) -> None:
        await self._order_manager.handle_order_event(event)

    async def handle_trade_event(self, event: MarketTradeEvent) -> CurrentPositionEvent | None:
        return await self._order_manager.handle_trade_event(event)

    async def handle_desired_position(
        self, desired_position: DesiredPositionEvent
    ) -> CurrentPositionEvent:
        async with self._lock:
            current_position, active_order = await self._order_manager.get_position_by_token(
                desired_position.market, desired_position.token
            )

            desired_shares = round(desired_position.shares, 6)
            current_shares = round(
                current_position.opening_shares
                + current_position.holding_shares
                - current_position.closing_shares,
                6,
            )

            if desired_shares > current_shares:
                delta = round(desired_shares - current_shares, 6)
                await self._reconcile_order(
                    desired_position,
                    side=Side.BUY,
                    shares=delta,
                    active_order=active_order,
                )
            elif desired_shares < current_shares:
                delta = round(current_shares - desired_shares, 6)
                await self._reconcile_order(
                    desired_position,
                    side=Side.SELL,
                    shares=delta,
                    active_order=active_order,
                )

            updated_position, _ = await self._order_manager.get_position_by_token(
                desired_position.market, desired_position.token
            )
            return updated_position

    async def _reconcile_order(
        self,
        desired_position: DesiredPositionEvent,
        *,
        side: Side,
        shares: float,
        active_order: ManagedOrder | None,
    ) -> None:

        if shares < self._replace_shares_gap:
            return None

        if active_order is None:
            price = _pick_price(desired_position, side, desired_position.force, shares)
            if side == Side.BUY:
                await self._order_manager.buy(
                    desired_position.market,
                    desired_position.token,
                    shares,
                    price,
                    desired_position.force,
                )
            else:
                await self._order_manager.sell(
                    desired_position.market,
                    desired_position.token,
                    shares,
                    price,
                    desired_position.force,
                )
            return

        if active_order.side != side:
            await self._order_manager.cancel(
                active_order.local_id,
                active_order.order_id,
                reason="target side changed",
            )
            return

        now = now_ts_ms()
        age_s = (now - active_order.created_ts_ms) / 1000
        ttl_expired = not desired_position.force and age_s >= self._replace_ttl_s
        new_price = _pick_price(desired_position, side, desired_position.force, shares)
        price_moved = abs(active_order.price - new_price) >= self._replace_price_gap
        if not ttl_expired and not price_moved:
            return

        if ttl_expired:
            reason = "ttl expired"
        else:
            reason = "price changed"
        logger.info(
            "%s: cancel order %s for replace %.6f @ %.2f -> %.6f @ %.2f",
            reason,
            active_order.order_id,
            active_order.shares,
            active_order.price,
            shares,
            new_price,
        )
        await self._order_manager.cancel(
            active_order.local_id,
            active_order.order_id,
            reason=reason,
        )


def _pick_price(desired: DesiredPositionEvent, side: Side, force: bool, shares: float) -> float:
    as_maker = not force and shares >= 5.0
    if side == Side.BUY:
        return desired.best_bid if as_maker else desired.best_ask
    else:
        return desired.best_ask if as_maker else desired.best_bid
