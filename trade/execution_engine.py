from __future__ import annotations

import asyncio
import uuid

from models.market import Token
from models.market_order import MATCHED_SHARES_GAP, MarketOrder, OrderSide
from strategies.strategy import PositionTarget, StrategyDecision
from streams.market_order_event import MarketOrderEvent, MarketOrderEventStatus
from streams.market_trade_event import MarketTradeEvent, MarketTradeEventStatus
from trade.models import (
    ManagedOrder,
    ManagedOrderStatus,
    ManagedTrade,
    ManagedTradeStatus,
    Position,
    TradeLatestState,
    TradePurpose,
)
from trade.trade_client import TradeClient
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("TRADE")

_MAX_ORPHAN_EVENTS_PER_ORDER = 16


class ExecutionEngine:
    def __init__(
        self,
        maker_client: TradeClient,
        taker_client: TradeClient | None = None,
        *,
        order_poll_interval_s: float = 0.25,
        position_sync_interval_s: float = 5.0,
    ) -> None:
        self._maker_client = maker_client
        self._taker_client = taker_client or maker_client
        self._order_poll_interval_s = order_poll_interval_s
        self._position_sync_interval_s = position_sync_interval_s
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
        latest_decision: asyncio.Queue[StrategyDecision],
        *,
        watch_orders: bool = True,
    ) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._decision_loop(latest_decision))
            if watch_orders:
                tasks.create_task(self._order_watch_loop())

    async def latest_state(self) -> TradeLatestState:
        async with self._lock:
            open_orders = tuple(
                order
                for order in self._orders_by_local_id.values()
                if _is_open_managed_order(order)
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
            unknown_count = sum(
                1
                for order in self._orders_by_local_id.values()
                if order.status == ManagedOrderStatus.UNKNOWN
            )
        return TradeLatestState(
            open_orders=open_orders,
            positions=positions,
            unknown_order_count=unknown_count,
        )

    async def _decision_loop(self, latest_decision_queue: asyncio.Queue[StrategyDecision]) -> None:
        while True:
            decision = await latest_decision_queue.get()
            await self.handle_strategy_decision(decision)

    async def handle_strategy_decision(self, decision: StrategyDecision) -> None:
        target = decision.latest_target
        if target is None:
            return

        async with self._lock:
            self._target = target
            self._tokens_by_token_id[target.token.id] = target.token

        await self._reconcile_target(target)

    async def handle_market_event(self, event: MarketOrderEvent | MarketTradeEvent) -> None:
        if isinstance(event, MarketOrderEvent):
            await self._handle_order_event(event)
        else:
            await self._handle_trade_event(event)

    async def _handle_order_event(self, event: MarketOrderEvent) -> None:
        should_reconcile = False
        async with self._lock:
            order = self._orders_by_order_id.get(event.order_id)
            if order is None:
                self._buffer_order_event_locked(event)
                logger.debug("buffer order event for untracked order %s", event.order_id)
                return
            if order.token.id != event.token_id or order.market.id != event.market_id:
                logger.warning("ignore mismatched order event: %s", event)
                return

            previous_matched = order.off_chain_matched_shares
            self._trade_ids_by_order_id.setdefault(event.order_id, set()).update(event.trade_ids)
            order.ordered_shares = event.ordered_shares
            order.off_chain_matched_shares = event.matched_shares
            if event.cancelled:
                order.off_chain_pending_shares = 0.0
                order.off_chain_invalid_shares = event.pending_shares
            else:
                order.off_chain_pending_shares = event.pending_shares
                order.off_chain_invalid_shares = 0.0
            order.updated_ts_ms = max(order.updated_ts_ms, event.ts_ms)

            if previous_matched != order.off_chain_matched_shares:
                should_reconcile = True

            if event.status == MarketOrderEventStatus.INVALID:
                order.status = (
                    ManagedOrderStatus.CANCELED
                    if event.cancelled
                    else ManagedOrderStatus.SUBMIT_FAILED
                )
                order.last_error = event.raw_status
                should_reconcile = True
            elif event.status == MarketOrderEventStatus.MATCHED:
                order.status = ManagedOrderStatus.MATCHED
                should_reconcile = True
            elif order.status != ManagedOrderStatus.PENDING_CANCEL:
                order.status = ManagedOrderStatus.PENDING_MATCH

            target = self._target
            self._log_order_trade_gap_locked(order)

        logger.info(
            "order event: %s %s matched=%.6f pending=%.6f",
            event.order_id,
            event.raw_status,
            event.matched_shares,
            event.pending_shares,
        )
        if should_reconcile and target is not None and target.token.id == event.token_id:
            await self._reconcile_target(target)

    async def _handle_trade_event(self, event: MarketTradeEvent) -> None:
        if event.status == MarketTradeEventStatus.FAILURE:
            async with self._lock:
                order = self._orders_by_order_id.get(event.order_id)
                if order is None:
                    self._buffer_trade_event_locked(event)
                    return
                if order is not None:
                    self._upsert_trade_locked(order, event, ManagedTradeStatus.FAILURE)
            logger.warning("trade event invalid: %s %s", event.trade_id, event.raw_status)
            return
        if event.status != MarketTradeEventStatus.SUCCESS:
            async with self._lock:
                order = self._orders_by_order_id.get(event.order_id)
                if order is None:
                    self._buffer_trade_event_locked(event)
                    return
                self._upsert_trade_locked(order, event, ManagedTradeStatus.PENDING)
            logger.debug("trade event pending: %s %s", event.trade_id, event.raw_status)
            return

        should_reconcile = False
        async with self._lock:
            order = self._orders_by_order_id.get(event.order_id)
            if order is None:
                self._buffer_trade_event_locked(event)
                logger.debug("buffer trade event for untracked order %s", event.order_id)
                return
            if order.token.id != event.token_id or order.market.id != event.market_id:
                logger.warning("ignore mismatched trade event: %s", event)
                return

            should_reconcile = self._upsert_trade_locked(order, event, ManagedTradeStatus.SUCCESS)
            target = self._target

        logger.info(
            "trade confirmed: %s order=%s shares=%.6f",
            event.trade_id,
            event.order_id,
            event.shares,
        )
        if should_reconcile and target is not None and target.token.id == event.token_id:
            await self._reconcile_target(target)

    async def _reconcile_target(self, target: PositionTarget) -> None:
        if await self._has_unknown_for_token(target.token):
            logger.warning("target skipped: unknown order exists for token %s", target.token.key)
            return

        await self._cancel_other_token_orders(target)

        current_shares = await self._position_shares(target.token)
        active_order = await self._active_order_for_token(target.token)
        target_shares = max(0.0, target.shares)

        if active_order is not None:
            if active_order.status == ManagedOrderStatus.PENDING_CANCEL:
                return
            desired_side = _side_for_target(current_shares, target_shares)
            if desired_side is None or active_order.side != desired_side:
                await self._cancel_order(active_order, reason="target changed")
                return

        if target_shares > current_shares + MATCHED_SHARES_GAP:
            delta = round(target_shares - current_shares, 6)
            await self._ensure_target_order(target, side="BUY", shares=delta, purpose="increase")
            return

        if target_shares < current_shares - MATCHED_SHARES_GAP:
            delta = round(current_shares - target_shares, 6)
            await self._ensure_target_order(target, side="SELL", shares=delta, purpose="reduce")
            return

        if active_order is not None:
            await self._cancel_order(active_order, reason="target reached")

    async def _ensure_target_order(
        self,
        target: PositionTarget,
        *,
        side: OrderSide,
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

        if current.status in {ManagedOrderStatus.UNKNOWN, ManagedOrderStatus.PENDING_CANCEL}:
            return
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
        price_moved = abs(current.price - target.price) >= target.replace_price_gap
        size_changed = abs(current.shares - shares) >= MATCHED_SHARES_GAP
        ttl_expired = target.replace_on_ttl and age_s >= target.ttl_s
        if not price_moved and not size_changed and not ttl_expired:
            return

        if current.replace_count >= target.max_replace_count:
            logger.warning("replace skipped: max replace count reached for %s", target.target_id)
            return

        reason = "ttl expired" if ttl_expired else "price changed"
        logger.info(
            "%s: cancel order %s for replace %.6f @ %.2f -> %.6f @ %.2f",
            reason,
            current.order_id,
            current.shares,
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

    async def _submit_order(
        self,
        target: PositionTarget,
        *,
        side: OrderSide,
        shares: float,
        purpose: TradePurpose,
        replace_count: int,
    ) -> None:
        now = now_ts_ms()
        local_id = uuid.uuid4().hex
        draft = ManagedOrder(
            local_id=local_id,
            signal_id=target.target_id,
            purpose=purpose,
            market=target.market,
            token=target.token,
            side=side,
            ordered_shares=shares,
            price=target.price,
            as_maker=target.post_only,
            order_id=None,
            created_ts_ms=now,
            updated_ts_ms=now,
            status=ManagedOrderStatus.PENDING_SUBMIT,
            off_chain_pending_shares=shares,
            replace_count=replace_count,
        )
        async with self._lock:
            self._orders_by_local_id[local_id] = draft
            self._tokens_by_token_id[target.token.id] = target.token

        client = self._maker_client if draft.as_maker else self._taker_client
        submit_func = client.buy if draft.side == "BUY" else client.sell
        order_id = await asyncio.to_thread(submit_func, draft.token, draft.shares, draft.price)

        if order_id is None:
            await self._handle_submit_order_failed(local_id, target)
        else:
            cached_events, should_cancel = await self._handle_submit_order_succeeded(
                local_id, order_id, target
            )
            for event in cached_events:
                await self.handle_market_event(event)
            if should_cancel:
                async with self._lock:
                    order = self._orders_by_order_id.get(order_id)
                if order is not None and await self._cancel_order(
                    order, reason="cancel requested while submitting"
                ):
                    async with self._lock:
                        latest_target = self._target
                    if latest_target is not None:
                        await self._reconcile_target(latest_target)

    async def _handle_submit_order_failed(self, local_id: str, target: PositionTarget) -> None:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            if order.status == ManagedOrderStatus.PENDING_CANCEL:
                order.status = ManagedOrderStatus.CANCELED
                order.off_chain_invalid_shares = order.off_chain_pending_shares
                order.off_chain_pending_shares = 0.0
                order.last_error = None
                return
            order.status = ManagedOrderStatus.SUBMIT_FAILED
            order.off_chain_invalid_shares = order.off_chain_pending_shares
            order.off_chain_pending_shares = 0.0
            order.last_error = "submit failed"
        logger.warning("order submit failed: %s", target.target_id)

    async def _handle_submit_order_succeeded(
        self, local_id: str, order_id: str, target: PositionTarget
    ) -> tuple[list[MarketOrderEvent | MarketTradeEvent], bool]:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return [], False
            should_cancel = order.status == ManagedOrderStatus.PENDING_CANCEL
            order.updated_ts_ms = now_ts_ms()
            order.order_id = order_id
            self._orders_by_order_id[order_id] = order
            self._trade_ids_by_order_id.setdefault(order_id, set())
            if not should_cancel:
                order.status = ManagedOrderStatus.PENDING_MATCH
            cached_events: list[MarketOrderEvent | MarketTradeEvent] = [
                *self._cached_order_events_by_order_id.pop(order_id, []),
                *self._cached_trade_events_by_order_id.pop(order_id, []),
            ]
        logger.info("order open: %s target=%s", order_id, target.target_id)
        return cached_events, should_cancel

    async def _cancel_order(self, order: ManagedOrder, *, reason: str) -> bool:
        if order.order_id is None:
            async with self._lock:
                latest_order = self._orders_by_local_id.get(order.local_id)
                if latest_order is None:
                    return False
                if latest_order.status in {
                    ManagedOrderStatus.CANCELED,
                    ManagedOrderStatus.SUBMIT_FAILED,
                    ManagedOrderStatus.PENDING_CANCEL,
                }:
                    return True
                if latest_order.status != ManagedOrderStatus.PENDING_SUBMIT:
                    logger.warning(
                        "unexpected order status without order id: %s %s",
                        latest_order.local_id,
                        latest_order.status,
                    )
                    return False
                latest_order.status = ManagedOrderStatus.PENDING_CANCEL
                latest_order.cancel_attempts += 1
                latest_order.updated_ts_ms = now_ts_ms()
            logger.info("order cancel queued until submit returns: %s (%s)", order.local_id, reason)
            return False

        async with self._lock:
            latest_order = self._orders_by_local_id.get(order.local_id)
            if latest_order is None or latest_order.order_id is None:
                return False
            if latest_order.status in {ManagedOrderStatus.MATCHED, ManagedOrderStatus.CANCELED}:
                return latest_order.status == ManagedOrderStatus.CANCELED
            latest_order.status = ManagedOrderStatus.PENDING_CANCEL
            latest_order.cancel_attempts += 1
            latest_order.updated_ts_ms = now_ts_ms()
            order_id = latest_order.order_id

        if await asyncio.to_thread(self._maker_client.cancel_order_by_id, order_id):
            if await self._handle_cancel_order_succeeded(order.local_id, order_id):
                logger.info("order canceled: %s (%s)", order_id, reason)
                return True
            logger.info("cancel success ignored after newer order state: %s", order_id)
            return False
        else:
            await self._handle_cancel_order_failed(
                order.local_id,
                order_id,
                error="cancel rejected",
            )
            logger.warning("order cancel failed: %s", order_id)
            return False

    async def _handle_cancel_order_failed(
        self,
        local_id: str,
        order_id: str,
        *,
        error: str,
    ) -> None:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None or order.order_id != order_id:
                return
            if order.status in {ManagedOrderStatus.MATCHED, ManagedOrderStatus.CANCELED}:
                return
            if order.status != ManagedOrderStatus.PENDING_CANCEL:
                return
            order.status = ManagedOrderStatus.CANCEL_FAILED
            order.updated_ts_ms = now_ts_ms()
            order.last_error = error

    async def _handle_cancel_order_succeeded(self, local_id: str, order_id: str) -> bool:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None or order.order_id != order_id:
                return False
            if order.status in {ManagedOrderStatus.MATCHED, ManagedOrderStatus.CANCELED}:
                return order.status == ManagedOrderStatus.CANCELED
            if order.status != ManagedOrderStatus.PENDING_CANCEL:
                return False
            order.status = ManagedOrderStatus.CANCELED
            order.off_chain_invalid_shares = round(
                order.off_chain_invalid_shares + order.off_chain_pending_shares,
                6,
            )
            order.off_chain_pending_shares = 0.0
            order.updated_ts_ms = now_ts_ms()
            order.last_error = None
            return True

    async def _order_watch_loop(self) -> None:
        while True:
            for order in await self._open_orders():
                if order.status == ManagedOrderStatus.PENDING_CANCEL:
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
                0.0,
                round(exchange_order.ordered_shares - exchange_order.matched_shares, 6),
            )
            order.updated_ts_ms = now_ts_ms()
            if _is_matched(exchange_order):
                order.status = ManagedOrderStatus.MATCHED
            elif order.status != ManagedOrderStatus.PENDING_CANCEL:
                order.status = ManagedOrderStatus.PENDING_MATCH

        if _is_matched(exchange_order):
            logger.info(
                "order matched: %s %.6f/%.6f",
                order.order_id,
                order.matched_shares,
                order.shares,
            )
        return order

    async def _open_orders(self) -> list[ManagedOrder]:
        async with self._lock:
            return [
                order
                for order in self._orders_by_local_id.values()
                if _is_open_managed_order(order)
            ]

    async def _tracked_tokens(self) -> list[Token]:
        async with self._lock:
            tokens = {order.token.id: order.token for order in self._orders_by_local_id.values()}
        return list(tokens.values())

    async def _has_unknown_for_token(self, token: Token) -> bool:
        async with self._lock:
            return any(
                order.token.id == token.id and order.status == ManagedOrderStatus.UNKNOWN
                for order in self._orders_by_local_id.values()
            )

    async def _position_shares(self, token: Token) -> float:
        async with self._lock:
            return self._committed_position_shares_locked(token.id)

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

    def _buffer_order_event_locked(self, event: MarketOrderEvent) -> None:
        events = self._cached_order_events_by_order_id.setdefault(event.order_id, [])
        if len(events) >= _MAX_ORPHAN_EVENTS_PER_ORDER:
            events.pop(0)
        events.append(event)

    def _buffer_trade_event_locked(self, event: MarketTradeEvent) -> None:
        events = self._cached_trade_events_by_order_id.setdefault(event.order_id, [])
        if len(events) >= _MAX_ORPHAN_EVENTS_PER_ORDER:
            events.pop(0)
        events.append(event)

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
                raw_status=event.raw_status,
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
        trade.raw_status = event.raw_status
        trade.updated_ts_ms = max(trade.updated_ts_ms, event.ts_ms)
        order.updated_ts_ms = max(order.updated_ts_ms, event.ts_ms)
        order.last_error = event.raw_status if status == ManagedTradeStatus.FAILURE else None

        if status == ManagedTradeStatus.PENDING:
            trade.status = ManagedTradeStatus.PENDING
            trade.on_chain_pending_shares = event.shares
            trade.on_chain_success_shares = 0.0
            trade.on_chain_failure_shares = 0.0
            self._log_order_trade_gap_locked(order)
            return False

        trade.on_chain_pending_shares = 0.0
        if status == ManagedTradeStatus.SUCCESS:
            trade.status = ManagedTradeStatus.SUCCESS
            trade.on_chain_success_shares = event.shares
            trade.on_chain_failure_shares = 0.0
            self._apply_settled_position_locked(order, event.shares)
            self._log_order_trade_gap_locked(order)
            return True

        trade.status = ManagedTradeStatus.FAILURE
        trade.on_chain_success_shares = 0.0
        trade.on_chain_failure_shares = event.shares
        self._log_order_trade_gap_locked(order)
        return True

    def _apply_settled_position_locked(self, order: ManagedOrder, shares: float) -> None:
        current = self._settled_position_by_token_id.get(order.token.id, 0.0)
        signed_shares = shares if order.side == "BUY" else -shares
        self._settled_position_by_token_id[order.token.id] = round(current + signed_shares, 6)

    def _committed_position_shares_locked(self, token_id: str) -> float:
        shares = self._settled_position_by_token_id.get(token_id, 0.0)
        for order in self._orders_by_local_id.values():
            if order.token.id != token_id or order.order_id is None:
                continue
            pending, success, failure = self._order_chain_totals_locked(order.order_id)
            unknown_matched = max(
                0.0,
                round(order.off_chain_matched_shares - pending - success - failure, 6),
            )
            unsettled = pending + unknown_matched
            if order.side == "BUY":
                shares += unsettled
            else:
                shares -= unsettled
        return max(0.0, round(shares, 6))

    def _order_chain_totals_locked(self, order_id: str) -> tuple[float, float, float]:
        pending = 0.0
        success = 0.0
        failure = 0.0
        for trade_id in self._trade_ids_by_order_id.get(order_id, set()):
            trade = self._trades_by_trade_id.get(trade_id)
            if trade is None:
                continue
            pending += trade.on_chain_pending_shares
            success += trade.on_chain_success_shares
            failure += trade.on_chain_failure_shares
        return round(pending, 6), round(success, 6), round(failure, 6)

    def _log_order_trade_gap_locked(self, order: ManagedOrder) -> None:
        if order.order_id is None:
            return
        pending, success, failure = self._order_chain_totals_locked(order.order_id)
        trade_total = round(pending + success + failure, 6)
        if trade_total > order.off_chain_matched_shares + MATCHED_SHARES_GAP:
            logger.warning(
                "trade shares exceed order matched shares: order=%s trades=%.6f matched=%.6f",
                order.order_id,
                trade_total,
                order.off_chain_matched_shares,
            )


def _is_matched(order: MarketOrder) -> bool:
    return (
        order.ordered_shares > 0
        and order.matched_shares > order.ordered_shares - MATCHED_SHARES_GAP
    )


def _is_open_managed_order(order: ManagedOrder) -> bool:
    return order.off_chain_pending_shares > 0


def _side_for_target(current_shares: float, target_shares: float) -> OrderSide | None:
    if target_shares > current_shares + MATCHED_SHARES_GAP:
        return "BUY"
    if target_shares < current_shares - MATCHED_SHARES_GAP:
        return "SELL"
    return None
