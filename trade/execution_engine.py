from __future__ import annotations

import asyncio
import uuid

from markets.base import Token
from strategies.context import PositionLatestState
from strategies.target import ExecutionStyle, PositionTarget
from streams.market_order_event import MarketOrderEvent
from streams.market_trade_event import MarketTradeEvent
from trade.managed_order import (
    ManagedOrder,
    ManagedTrade,
    Position,
    TradePurpose,
)
from trade.trade_client import TradeClient
from utils.enum import ManagedOrderStatus, ManagedTradeStatus, MarketTradeStatus, Side
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("TRADE")

ZERO = 0.0
MATCHED_SHARES_GAP = 0.1


class ExecutionEngine:
    def __init__(
        self,
        maker_client: TradeClient,
        taker_client: TradeClient | None = None,
        *,
        order_poll_interval_s: float = 0.25,
        position_sync_interval_s: float = 5.0,
        order_ttl_s: float = 2.0,
        replace_price_gap: float = 0.05,
        max_replace_count: int = 20,
    ) -> None:
        self._maker_client = maker_client
        self._taker_client = taker_client or maker_client
        self._order_poll_interval_s = order_poll_interval_s
        self._position_sync_interval_s = position_sync_interval_s
        self._order_ttl_s = order_ttl_s
        self._replace_price_gap = replace_price_gap
        self._max_replace_count = max_replace_count
        self._lock = asyncio.Lock()

        self._target: PositionTarget | None = None
        self._tokens_by_token_id: dict[str, Token] = {}

        self._orders_by_local_id: dict[str, ManagedOrder] = {}
        self._orders_by_order_id: dict[str, ManagedOrder] = {}

        self._trades_by_trade_id: dict[str, ManagedTrade] = {}
        self._trade_ids_by_order_id: dict[str, set[str]] = {}
        self._settled_position_by_token_id: dict[str, float] = {}

        self._cached_order_events_by_order_id: dict[str, list[MarketOrderEvent]] = {}
        self._cached_trade_events_by_order_id: dict[str, list[MarketTradeEvent]] = {}

    async def run(
        self,
        latest_target: asyncio.Queue[PositionTarget],
        *,
        watch_orders: bool = True,
    ) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._target_loop(latest_target))
            if watch_orders:
                tasks.create_task(self._order_watch_loop())

    async def latest_state(self) -> PositionLatestState:
        async with self._lock:
            open_orders = tuple(
                order for order in self._orders_by_local_id.values() if order.has_active_shares
            )
            positions = {
                token_id: Position(
                    token=token,
                    shares=shares,
                    updated_ts_ms=now_ts_ms(),
                    source="local",
                )
                for token_id, shares in self._settled_position_by_token_id.items()
                if (token := self._tokens_by_token_id.get(token_id)) is not None
            }
            unknown_count = 0
        return PositionLatestState(
            open_orders=open_orders,
            positions=positions,
            unknown_order_count=unknown_count,
        )

    async def _target_loop(self, latest_target_queue: asyncio.Queue[PositionTarget]) -> None:
        while True:
            target = await latest_target_queue.get()
            await self.handle_position_target(target)

    async def handle_position_target(self, target: PositionTarget) -> None:
        async with self._lock:
            self._target = target
            self._tokens_by_token_id[target.token.id] = target.token

        await self._reconcile_target(target)

    async def _reconcile_target(self, target: PositionTarget) -> None:
        await self._cancel_other_token_orders(target)

        current_shares = await self._position_shares(target.token)
        active_order = await self._active_order_for_token(target.token)
        target_shares = max(ZERO, target.shares)

        if active_order is not None:
            if active_order.should_cancel:
                return
            desired_side = _side_for_target(current_shares, target_shares)
            if desired_side is None or active_order.side != desired_side:
                await self._cancel_order(active_order, reason="target changed")
                return

        if target_shares > current_shares + MATCHED_SHARES_GAP:
            delta = round(target_shares - current_shares, 6)
            await self._ensure_target_order(
                target,
                side=Side.BUY,
                shares=delta,
                purpose="increase",
            )
            return

        if target_shares < current_shares - MATCHED_SHARES_GAP:
            delta = round(current_shares - target_shares, 6)
            await self._ensure_target_order(
                target,
                side=Side.SELL,
                shares=delta,
                purpose="reduce",
            )
            return

        if active_order is not None:
            await self._cancel_order(active_order, reason="target reached")

    async def _ensure_target_order(
        self,
        target: PositionTarget,
        *,
        side: Side,
        shares: float,
        purpose: TradePurpose,
    ) -> None:
        current = await self._active_order_for_token(target.token)
        if current is None:
            await self._submit_order(
                target,
                side=side,
                shares=shares,
                purpose=purpose,
                replace_count=0,
            )
            return

        # if current.status in {ManagedOrderStatus.UNKNOWN, ManagedOrderStatus.PENDING_CANCEL}:
        #     return
        if current.side != side or current.purpose != purpose:
            if await self._cancel_order(current, reason="target side changed"):
                await self._submit_order(
                    target,
                    side=side,
                    shares=shares,
                    purpose=purpose,
                    replace_count=current.replace_count + 1,
                )
            return

        now = now_ts_ms()
        age_s = (now - current.created_ts_ms) / 1000
        price_moved = abs(current.price - target.price) >= self._replace_price_gap
        size_changed = abs(current.ordered_shares - shares) >= MATCHED_SHARES_GAP
        ttl_expired = target.style == ExecutionStyle.PASSIVE and age_s >= self._order_ttl_s
        if not price_moved and not size_changed and not ttl_expired:
            return

        if current.replace_count >= self._max_replace_count:
            logger.warning("replace skipped: max replace count reached")
            return

        reason = "ttl expired" if ttl_expired else "price changed"
        logger.info(
            "%s: cancel order %s for replace %.6f @ %.2f -> %.6f @ %.2f",
            reason,
            current.order_id,
            current.ordered_shares,
            current.price,
            shares,
            target.price,
        )
        if await self._cancel_order(current, reason=reason):
            await self._submit_order(
                target,
                side=side,
                shares=shares,
                purpose=purpose,
                replace_count=current.replace_count + 1,
            )

    async def _order_watch_loop(self) -> None:
        while True:
            for order in await self._open_orders():
                if order.should_cancel:
                    continue
                await self._refresh_order(order.local_id)
            await asyncio.sleep(self._order_poll_interval_s)

    async def _refresh_order(self, local_id: str) -> ManagedOrder | None:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None or order.order_id is None:
                return order
            order_id = order.order_id

        exchange_order = await asyncio.to_thread(self._maker_client.get_order_by_id, order_id)
        if exchange_order is None:
            return None

        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return None
            order.ordered_shares = exchange_order.ordered_shares
            order.off_chain_matched_shares = exchange_order.matched_shares
            order.off_chain_pending_shares = max(
                ZERO,
                round(exchange_order.ordered_shares - exchange_order.matched_shares, 6),
            )
            order.updated_ts_ms = now_ts_ms()
            if order.is_effectively_matched:
                order.status = ManagedOrderStatus.MATCHED
            elif not order.should_cancel:
                order.status = ManagedOrderStatus.MATCHING

        if order.is_effectively_matched:
            logger.info(
                "order matched: %s %.6f/%.6f",
                order.order_id,
                order.off_chain_matched_shares,
                order.ordered_shares,
            )
        return order

    async def _open_orders(self) -> list[ManagedOrder]:
        async with self._lock:
            return [order for order in self._orders_by_local_id.values() if order.has_active_shares]

    async def _tracked_tokens(self) -> list[Token]:
        async with self._lock:
            tokens = {order.token.id: order.token for order in self._orders_by_local_id.values()}
        return list(tokens.values())

    async def _position_shares(self, token: Token) -> float:
        async with self._lock:
            return 0  # todo

    async def _active_order_for_token(self, token: Token) -> ManagedOrder | None:
        for order in await self._open_orders():
            if order.token.id == token.id:
                return order
        return None

    async def _cancel_other_token_orders(self, target: PositionTarget) -> None:
        for order in await self._open_orders():
            if order.token.id == target.token.id:
                continue
            await self._cancel_order(order, reason="target token changed")

    def _upsert_trade_locked(
        self,
        order: ManagedOrder,
        event: MarketTradeEvent,
        status: ManagedTradeStatus,
    ) -> bool:
        if order.order_id is None:
            return False
        if order.token.id != event.token_id or order.market.id != event.market_id:
            logger.warning("ignore mismatched trade event: %s", event)
            return False

        trade = self._trades_by_trade_id.get(event.trade_id)
        if trade is None:
            trade = ManagedTrade(
                trade_id=event.trade_id,
                order_id=event.order_id,
                market_id=event.market_id,
                token_id=event.token_id,
                shares=event.shares,
                mkt_status=event.status,
                status=ManagedTradeStatus.PENDING,
                created_ts_ms=event.ts_ms,
                updated_ts_ms=event.ts_ms,
            )
            self._trades_by_trade_id[event.trade_id] = trade
            self._trade_ids_by_order_id.setdefault(event.order_id, set()).add(event.trade_id)
        elif trade.order_id != event.order_id:
            logger.warning(
                "ignore trade event with changed order id: %s %s -> %s",
                event.trade_id,
                trade.order_id,
                event.order_id,
            )
            return False
        order.trades[event.trade_id] = trade

        if trade.status in {ManagedTradeStatus.SUCCESS, ManagedTradeStatus.FAILURE}:
            if trade.status != status:
                logger.warning(
                    "ignore terminal trade status change: %s %s -> %s",
                    trade.trade_id,
                    trade.status,
                    status,
                )
            return False

        trade.shares = event.shares
        trade.mkt_status = event.status
        trade.updated_ts_ms = max(trade.updated_ts_ms, event.ts_ms)
        order.updated_ts_ms = max(order.updated_ts_ms, event.ts_ms)

        if status == ManagedTradeStatus.PENDING:
            trade.status = ManagedTradeStatus.PENDING
            trade.on_chain_pending_shares = event.shares
            trade.on_chain_settled_shares = ZERO
            trade.on_chain_failure_shares = ZERO
            return False

        trade.on_chain_pending_shares = ZERO
        if status == ManagedTradeStatus.SUCCESS:
            trade.status = ManagedTradeStatus.SUCCESS
            trade.on_chain_settled_shares = event.shares
            trade.on_chain_failure_shares = ZERO
            self._apply_settled_position_locked(order, event.shares)
            return True

        trade.status = ManagedTradeStatus.FAILURE
        trade.on_chain_settled_shares = ZERO
        trade.on_chain_failure_shares = event.shares
        return True

    def _apply_settled_position_locked(self, order: ManagedOrder, shares: float) -> None:
        current = self._settled_position_by_token_id.get(order.token.id, ZERO)
        signed_shares = shares if order.side == Side.BUY else -shares
        self._settled_position_by_token_id[order.token.id] = round(current + signed_shares, 6)

    async def _submit_order(
        self,
        target: PositionTarget,
        *,
        side: Side,
        shares: float,
        purpose: TradePurpose,
        replace_count: int,
    ) -> None:
        now = now_ts_ms()
        local_id = uuid.uuid4().hex
        as_maker = target.style == ExecutionStyle.PASSIVE
        draft_order = ManagedOrder(
            local_id=local_id,
            order_id=None,
            market=target.market,
            token=target.token,
            side=side,
            price=target.price,
            status=ManagedOrderStatus.CRAFTED,
            purpose=purpose,
            as_maker=as_maker,
            created_ts_ms=now,
            updated_ts_ms=now,
            ordered_shares=shares,
            off_chain_pending_shares=shares,
            off_chain_matched_shares=ZERO,
            off_chain_invalid_shares=ZERO,
            replace_count=replace_count,
        )
        logger.info("submit order: %s", local_id)
        async with self._lock:
            self._orders_by_local_id[local_id] = draft_order
            self._tokens_by_token_id[target.token.id] = target.token

        client = self._maker_client if draft_order.as_maker else self._taker_client
        submit_order_func = client.buy if draft_order.side == Side.BUY else client.sell
        market_order_id = await asyncio.to_thread(
            submit_order_func, draft_order.token, draft_order.ordered_shares, draft_order.price
        )

        if market_order_id is not None:
            await self._handle_submit_order_success(local_id, market_order_id)
        else:
            await self._handle_submit_order_failed(local_id)

    async def _handle_submit_order_success(self, local_id: str, order_id: str) -> None:
        logger.info("submit order success: %s => %s", local_id, order_id)
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.order_id = order_id
            order.status = ManagedOrderStatus.MATCHING
            order.log("submit success")
            self._orders_by_order_id[order_id] = order
            self._trade_ids_by_order_id.setdefault(order_id, set())

        cached_events: list[MarketOrderEvent | MarketTradeEvent] = [
            *self._cached_order_events_by_order_id.pop(order_id, []),
            *self._cached_trade_events_by_order_id.pop(order_id, []),
        ]
        for event in cached_events:
            await self.handle_event(event)

        if order.should_cancel:
            async with self._lock:
                order = self._orders_by_order_id.get(order_id)
                if order is None:
                    return
                order.log("find should cancel")
            await self._cancel_order(order, reason="should cancel while submitting")

    async def _handle_submit_order_failed(self, local_id: str) -> None:
        logger.warning("submit order failed: %s", local_id)
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.off_chain_invalid_shares = order.off_chain_pending_shares
            order.off_chain_pending_shares = ZERO
            if not order.should_cancel:
                order.status = ManagedOrderStatus.INVALID
                order.log("submit failed")
            else:
                order.status = ManagedOrderStatus.CANCELED
                order.should_cancel = False
                order.log("find should cancel")

    async def _cancel_order(self, order: ManagedOrder, *, reason: str) -> bool:
        logger.info("cancel order: %s => %s", order.local_id, order.order_id)
        if order.order_id is None:
            async with self._lock:
                latest_order = self._orders_by_local_id.get(order.local_id)
                if latest_order is None:
                    return False
                if not latest_order.has_active_shares:
                    latest_order.log("cancel without active shares")
                    return True
                if latest_order.status == ManagedOrderStatus.CRAFTED:
                    latest_order.should_cancel = True
                    latest_order.log("cancel while submitting")
                    logger.info(
                        "order should cancel until submit returns: %s (%s)", order.local_id, reason
                    )
                else:
                    latest_order.log("unexpected status")
                    logger.warning(
                        "unexpected order status without order id: %s %s",
                        latest_order.local_id,
                        latest_order.status,
                    )
                return False

        async with self._lock:
            latest_order = self._orders_by_order_id.get(order.order_id)
            if latest_order is None:
                return False
            if not latest_order.has_active_shares:
                latest_order.log("cancel without active shares")
                return True

        order_id = order.order_id
        is_success, error_message = await asyncio.to_thread(
            self._maker_client.cancel_order_by_id, order_id
        )
        if is_success:
            await self._handle_cancel_order_success(order_id)
        else:
            await self._handle_cancel_order_failed(order_id, error_message)
        return is_success

    async def _handle_cancel_order_success(self, order_id: str) -> None:
        logger.info("cancel order success: %s", order_id)
        async with self._lock:
            order = self._orders_by_order_id.get(order_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.status = ManagedOrderStatus.CANCELED
            order.off_chain_invalid_shares = order.off_chain_pending_shares
            order.off_chain_pending_shares = ZERO
            order.log("canceled")

    async def _handle_cancel_order_failed(self, order_id: str, error_message: str) -> None:
        logger.info("cancel order failed: %s", order_id)
        async with self._lock:
            order = self._orders_by_order_id.get(order_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.log(error_message)

    async def handle_event(self, event: MarketOrderEvent | MarketTradeEvent) -> None:
        if isinstance(event, MarketOrderEvent):
            await self._handle_order_event(event)
        else:
            await self._handle_trade_event(event)

    async def _handle_order_event(self, event: MarketOrderEvent) -> None:
        async with self._lock:
            order = self._orders_by_order_id.get(event.order_id)
            if order is None:
                cached_events = self._cached_order_events_by_order_id.setdefault(event.order_id, [])
                cached_events.append(event)
                logger.debug("cache event for untracked order: %s", event.order_id)
                return
            if event.market_id != order.market.id or event.token_id != order.token.id:
                logger.warning("ignore mismatched order event: %s", event)
                return

            order.updated_ts_ms = max(order.updated_ts_ms, event.ts_ms)
            if event.cancelled:
                order.off_chain_pending_shares = ZERO
                order.off_chain_matched_shares = event.matched_shares
                order.off_chain_invalid_shares = event.pending_shares
                order.status = ManagedOrderStatus.CANCELED
                order.log("receive cancel event")
            else:
                if order.status == ManagedOrderStatus.MATCHING:
                    order.off_chain_pending_shares = min(
                        order.off_chain_pending_shares, event.pending_shares
                    )
                    order.off_chain_matched_shares = max(
                        order.off_chain_matched_shares, event.matched_shares
                    )
                    order.off_chain_invalid_shares = ZERO
                    if order.is_effectively_matched:
                        order.status = ManagedOrderStatus.MATCHED
                        order.log("filled")

            trade_ids_set = self._trade_ids_by_order_id.setdefault(event.order_id, set())
            trade_ids_set.update(event.trade_ids)

    async def _handle_trade_event(self, event: MarketTradeEvent) -> None:
        async with self._lock:
            order = self._orders_by_order_id.get(event.order_id)
            if order is None:
                cached_events = self._cached_trade_events_by_order_id.setdefault(event.order_id, [])
                cached_events.append(event)
                logger.debug("cache event for untracked order: %s", event.order_id)
                return
            if event.market_id != order.market.id or event.token_id != order.token.id:
                logger.warning("ignore mismatched order event: %s", event)
                return

            trade = self._trades_by_trade_id.get(event.trade_id)
            if trade is None:
                trade = ManagedTrade(
                    market_id=event.market_id,
                    token_id=event.token_id,
                    order_id=event.order_id,
                    trade_id=event.trade_id,
                    shares=event.shares,
                    status=ManagedTradeStatus.PENDING,
                    mkt_status=event.status,
                    created_ts_ms=event.ts_ms,
                    updated_ts_ms=event.ts_ms,
                    on_chain_pending_shares=event.shares,
                    on_chain_settled_shares=ZERO,
                    on_chain_failure_shares=ZERO,
                )

            if trade.mkt_status == MarketTradeStatus.CONFIRMED:
                trade.status = ManagedTradeStatus.SUCCESS
                trade.on_chain_pending_shares = ZERO
                trade.on_chain_settled_shares = trade.shares
                trade.on_chain_failure_shares = ZERO
            elif trade.mkt_status == MarketTradeStatus.FAILED:
                trade.status = ManagedTradeStatus.FAILURE
                trade.on_chain_pending_shares = ZERO
                trade.on_chain_settled_shares = ZERO
                trade.on_chain_failure_shares = trade.shares

            order.trades[event.trade_id] = trade
            self._trades_by_trade_id[event.trade_id] = trade
            self._trade_ids_by_order_id.setdefault(event.order_id, set()).add(event.trade_id)


def _side_for_target(current_shares: float, target_shares: float) -> Side | None:
    if target_shares > current_shares + MATCHED_SHARES_GAP:
        return Side.BUY
    if target_shares < current_shares - MATCHED_SHARES_GAP:
        return Side.SELL
    return None
