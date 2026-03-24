from __future__ import annotations

import asyncio

from clients.polymarket_clob import TradeClient
from enums import Side
from events import DesiredPositionEvent, MarketOrderEvent, MarketTradeEvent
from execution.order_manager import ManagedOrder, OrderManager
from execution.position_manager import PositionManager
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("TRADE")

ZERO = 0.0
SHARES_EPSILON = 0.000001


class ExecutionEngine:
    def __init__(
        self,
        maker_client: TradeClient,
        taker_client: TradeClient,
        *,
        order_ttl_s: float = 2.0,
        replace_price_gap: float = 0.05,
        replace_size_gap: float = 0.1,
        max_replace_count: int = 20,
    ) -> None:
        self._lock = asyncio.Lock()
        self._order_ttl_s = order_ttl_s
        self._replace_price_gap = replace_price_gap
        self._replace_size_gap = replace_size_gap
        self._max_replace_count = max_replace_count

        self.order_manager = OrderManager(maker_client, taker_client)
        self.position_manager = PositionManager()

    async def handle_order_event(self, event: MarketOrderEvent) -> None:
        await self.order_manager.handle_order_event(event)

    async def handle_trade_event(self, event: MarketTradeEvent) -> None:
        await self.order_manager.handle_trade_event(event)
        await self.position_manager.handle_trade_event(event)

    async def handle_desired_position(self, target: DesiredPositionEvent) -> None:
        async with self._lock:
            position = await self.position_manager.position_for_token(target.token.id)
            exposure = await self.order_manager.exposure_for_token(target.token)
            current_shares = round(
                position.holding_shares + exposure.opening_shares - exposure.closing_shares,
                6,
            )
            target_shares = max(ZERO, round(target.shares, 6))
            active_order = await self.order_manager.active_order_for_token(target.token)

            if active_order is not None and active_order.should_cancel:
                return

            if target_shares > current_shares + SHARES_EPSILON:
                delta = round(target_shares - current_shares, 6)
                await self._reconcile_order(
                    target,
                    side=Side.BUY,
                    shares=delta,
                    active_order=active_order,
                )
                return

            if target_shares < current_shares - SHARES_EPSILON:
                delta = round(current_shares - target_shares, 6)
                await self._reconcile_order(
                    target,
                    side=Side.SELL,
                    shares=delta,
                    active_order=active_order,
                )
                return

            if active_order is not None:
                await self.order_manager.cancel(active_order, reason="target reached")

    async def _reconcile_order(
        self,
        target: DesiredPositionEvent,
        *,
        side: Side,
        shares: float,
        active_order: ManagedOrder | None,
    ) -> None:
        if active_order is None:
            if side == Side.BUY:
                await self.order_manager.buy(
                    target.market,
                    target.token,
                    shares,
                    target.price,
                    target.force,
                    replace_count=0,
                )
            else:
                await self.order_manager.sell(
                    target.market,
                    target.token,
                    shares,
                    target.price,
                    target.force,
                    replace_count=0,
                )
            return

        if active_order.side != side:
            await self.order_manager.cancel(
                active_order,
                reason="target side changed",
                next_replace_count=active_order.replace_count + 1,
            )
            return

        now = now_ts_ms()
        age_s = (now - active_order.created_ts_ms) / 1000
        price_moved = abs(active_order.price - target.price) >= self._replace_price_gap
        size_changed = abs(active_order.shares - shares) >= self._replace_size_gap
        ttl_expired = not target.force and age_s >= self._order_ttl_s
        if not price_moved and not size_changed and not ttl_expired:
            return

        if active_order.replace_count >= self._max_replace_count:
            logger.warning("replace skipped: max replace count reached")
            return

        if ttl_expired:
            reason = "ttl expired"
        elif price_moved:
            reason = "price changed"
        else:
            reason = "size changed"
        logger.info(
            "%s: cancel order %s for replace %.6f @ %.2f -> %.6f @ %.2f",
            reason,
            active_order.order_id,
            active_order.shares,
            active_order.price,
            shares,
            target.price,
        )
        await self.order_manager.cancel(
            active_order,
            reason=reason,
            next_replace_count=active_order.replace_count + 1,
        )
