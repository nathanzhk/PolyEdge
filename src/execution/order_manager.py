from __future__ import annotations

import asyncio
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any

from clients.polymarket_clob import TradeClient
from enums import ManagedOrderStatus, ManagedTradeStatus, MarketTradeStatus, Role, Side
from events import MarketOrderEvent, MarketTradeEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("TRADE")

ZERO = 0.0
_MATCHED_SHARES_RATE = 0.98


@dataclass(slots=True)
class AuditRecord:
    ts_ms: int
    message: str


@dataclass(slots=True, frozen=True)
class OrderExposure:
    opening_shares: float = ZERO
    closing_shares: float = ZERO


@dataclass(slots=True)
class ManagedOrder:
    local_id: str
    order_id: str | None

    market: Market
    token: Token

    status: ManagedOrderStatus
    shares: float

    side: Side
    price: float
    as_maker: bool

    created_ts_ms: int
    updated_ts_ms: int

    off_chain_pending_shares: float
    off_chain_matched_shares: float
    off_chain_invalid_shares: float

    trades: dict[str, ManagedTrade] = field(default_factory=dict)
    should_cancel: bool = False

    replace_count: int = 0
    cancel_attempts: int = 0
    audit_records: list[AuditRecord] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        """off_chain_pending_shares > 0"""
        return (
            self.status == ManagedOrderStatus.SUBMITTING
            or self.status == ManagedOrderStatus.CANCELLING
            or self.status == ManagedOrderStatus.ZERO_MATCHED
            or self.status == ManagedOrderStatus.PART_MATCHED
        )

    @property
    def is_canceled(self) -> bool:
        """off_chain_invalid_shares > 0"""
        return (
            self.status == ManagedOrderStatus.ZERO_MATCHED_CANCELED
            or self.status == ManagedOrderStatus.PART_MATCHED_CANCELED
        )

    @property
    def is_settle_complete(self) -> bool:
        on_chain_shares = self.on_chain_settled_shares + self.on_chain_failure_shares
        return round(self.off_chain_matched_shares, 6) == round(on_chain_shares, 6)

    @property
    def has_settle_failure(self) -> bool:
        return self.on_chain_failure_shares > 0

    @property
    def on_chain_pending_shares(self) -> float:
        return round(sum(trade.on_chain_pending_shares for trade in self.trades.values()), 6)

    @property
    def on_chain_settled_shares(self) -> float:
        return round(sum(trade.on_chain_settled_shares for trade in self.trades.values()), 6)

    @property
    def on_chain_failure_shares(self) -> float:
        return round(sum(trade.on_chain_failure_shares for trade in self.trades.values()), 6)

    def log(self, message: str) -> None:
        self.audit_records.append(
            AuditRecord(
                ts_ms=now_ts_ms(),
                message=message,
            )
        )


@dataclass(slots=True)
class ManagedTrade:
    market_id: str
    token_id: str
    order_id: str
    trade_id: str

    status: ManagedTradeStatus
    shares: float

    side: Side
    role: Role
    price: float

    created_ts_ms: int
    updated_ts_ms: int

    on_chain_pending_shares: float = 0.0
    on_chain_settled_shares: float = 0.0
    on_chain_failure_shares: float = 0.0


class OrderManager:
    def __init__(self, maker_client: TradeClient, taker_client: TradeClient) -> None:
        self._lock = asyncio.Lock()
        self._maker_client = maker_client
        self._taker_client = taker_client

        self._tokens_by_token_id: dict[str, Token] = {}
        self._orders_by_local_id: dict[str, ManagedOrder] = {}
        self._orders_by_order_id: dict[str, ManagedOrder] = {}
        self._trades_by_trade_id: dict[str, ManagedTrade] = {}

        self._cached_order_events_by_order_id: dict[str, list[MarketOrderEvent]] = {}
        self._cached_trade_events_by_order_id: dict[str, list[MarketTradeEvent]] = {}

        self._pending_replace_count_by_token_id: dict[str, int] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def active_orders(self) -> list[ManagedOrder]:
        async with self._lock:
            return [order for order in self._orders_by_local_id.values() if order.is_active]

    async def active_order_for_token(self, token: Token) -> ManagedOrder | None:
        for order in await self.active_orders():
            if order.token.id == token.id:
                return order
        return None

    async def exposure_for_token(self, token: Token) -> OrderExposure:
        opening_shares = ZERO
        closing_shares = ZERO
        for order in await self.active_orders():
            if order.token.id != token.id:
                continue
            unsettled_shares = order.off_chain_pending_shares + order.on_chain_pending_shares
            if order.side == Side.BUY:
                opening_shares = round(opening_shares + unsettled_shares, 6)
            else:
                closing_shares = round(closing_shares + unsettled_shares, 6)
        return OrderExposure(opening_shares=opening_shares, closing_shares=closing_shares)

    async def buy(
        self,
        market: Market,
        token: Token,
        shares: float,
        price: float,
        urgent: bool,
        replace_count: int = 0,
    ) -> None:
        await self._submit_order(Side.BUY, market, token, shares, price, urgent, replace_count)

    async def sell(
        self,
        market: Market,
        token: Token,
        shares: float,
        price: float,
        urgent: bool,
        replace_count: int = 0,
    ) -> None:
        await self._submit_order(Side.SELL, market, token, shares, price, urgent, replace_count)

    async def _submit_order(
        self,
        side: Side,
        market: Market,
        token: Token,
        shares: float,
        price: float,
        urgent: bool,
        replace_count: int = 0,
    ) -> None:
        async with self._lock:
            replace_count = self._pending_replace_count_by_token_id.pop(token.id, replace_count)
        now = now_ts_ms()
        local_id = uuid.uuid4().hex
        as_maker = not urgent
        draft_order = ManagedOrder(
            local_id=local_id,
            order_id=None,
            market=market,
            token=token,
            status=ManagedOrderStatus.SUBMITTING,
            shares=shares,
            side=side,
            price=price,
            as_maker=as_maker,
            created_ts_ms=now,
            updated_ts_ms=now,
            off_chain_pending_shares=shares,
            off_chain_matched_shares=ZERO,
            off_chain_invalid_shares=ZERO,
            replace_count=replace_count,
        )
        logger.info("submit order: %s", local_id)
        async with self._lock:
            self._orders_by_local_id[local_id] = draft_order
            self._tokens_by_token_id[token.id] = token

        self._track_background_task(
            self._submit_order_worker(local_id),
            name=f"submit-order-{local_id}",
        )

    async def _submit_order_worker(self, local_id: str) -> None:
        async with self._lock:
            draft_order = self._orders_by_local_id.get(local_id)
            if draft_order is None:
                return
            client = self._maker_client if draft_order.as_maker else self._taker_client
            submit_order_func = client.buy if draft_order.side == Side.BUY else client.sell
            token = draft_order.token
            shares = draft_order.shares
            price = draft_order.price

        try:
            market_order_id = await asyncio.to_thread(submit_order_func, token, shares, price)
        except Exception:
            await self._handle_submit_order_failed(local_id)
            raise

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
            order.status = ManagedOrderStatus.ZERO_MATCHED
            order.log("submit success")
            self._orders_by_order_id[order_id] = order

        cached_events: list[MarketOrderEvent | MarketTradeEvent] = [
            *self._cached_order_events_by_order_id.pop(order_id, []),
            *self._cached_trade_events_by_order_id.pop(order_id, []),
        ]
        for event in cached_events:
            if isinstance(event, MarketOrderEvent):
                await self.handle_order_event(event)
            else:
                await self.handle_trade_event(event)

        if order.should_cancel:
            async with self._lock:
                order = self._orders_by_order_id.get(order_id)
                if order is None:
                    return
                order.should_cancel = False
                order.log("find should cancel")
            await self.cancel(order, reason="should cancel while submitting")

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
                order.status = ManagedOrderStatus.SUB_FAILED
                order.log("submit failed")
            else:
                order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
                order.should_cancel = False
                order.log("find should cancel")

    async def cancel(
        self,
        order: ManagedOrder,
        *,
        reason: str,
        next_replace_count: int | None = None,
    ) -> bool:
        logger.info("cancel order: %s => %s", order.local_id, order.order_id)
        async with self._lock:
            if order.order_id is None:
                latest_order = self._orders_by_local_id.get(order.local_id)
            else:
                latest_order = self._orders_by_order_id.get(order.order_id)
            if latest_order is None:
                return False
            if not latest_order.is_active:
                latest_order.log("cancel without active shares")
                return True
            if next_replace_count is not None:
                self._pending_replace_count_by_token_id[latest_order.token.id] = next_replace_count
            if latest_order.order_id is None:
                if latest_order.status == ManagedOrderStatus.SUBMITTING:
                    latest_order.should_cancel = True
                    latest_order.updated_ts_ms = now_ts_ms()
                    latest_order.log("cancel while submitting")
                    logger.info(
                        "order should cancel until submit returns: %s (%s)",
                        latest_order.local_id,
                        reason,
                    )
                else:
                    latest_order.log("unexpected status")
                    logger.warning(
                        "unexpected order status without order id: %s %s",
                        latest_order.local_id,
                        latest_order.status,
                    )
                return False
            if latest_order.should_cancel:
                return False
            latest_order.should_cancel = True
            latest_order.cancel_attempts += 1
            latest_order.updated_ts_ms = now_ts_ms()
            latest_order.log(f"cancel requested: {reason}")
            order_id = latest_order.order_id

        self._track_background_task(
            self._cancel_order_worker(order_id),
            name=f"cancel-order-{order_id}",
        )
        return True

    async def _cancel_order_worker(self, order_id: str) -> None:
        try:
            is_success, error_message = await asyncio.to_thread(
                self._maker_client.cancel_order_by_id, order_id
            )
        except Exception as exc:
            await self._handle_cancel_order_failed(order_id, f"cancel raised: {exc!r}")
            raise

        if is_success:
            await self._handle_cancel_order_success(order_id)
        else:
            await self._handle_cancel_order_failed(order_id, error_message)

    async def _handle_cancel_order_success(self, order_id: str) -> None:
        logger.info("cancel order success: %s", order_id)
        async with self._lock:
            order = self._orders_by_order_id.get(order_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.should_cancel = False
            order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
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
            order.should_cancel = False
            order.log(error_message)

    async def handle_order_event(self, event: MarketOrderEvent) -> None:
        async with self._lock:
            order = self._orders_by_order_id.get(event.order_id)
            if order is None:
                cached_events = self._cached_order_events_by_order_id.setdefault(event.order_id, [])
                cached_events.append(event)
                logger.debug("cache event for untracked order: %s", event.order_id)
                return

            if event.market_id != order.market.id or event.token_id != order.token.id:
                logger.error("ignore mismatched order event: %s", event)
                return

            if event.is_inactive or order.is_canceled:
                order.off_chain_matched_shares = max(
                    order.off_chain_matched_shares, event.matched_shares
                )
                order.off_chain_invalid_shares = min(
                    order.off_chain_invalid_shares, event.unmatched_shares
                )
                order.off_chain_pending_shares = ZERO
                if order.off_chain_matched_shares == ZERO:
                    order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
                else:
                    order.status = ManagedOrderStatus.PART_MATCHED_CANCELED
                order.should_cancel = False

            else:
                order.off_chain_matched_shares = max(
                    order.off_chain_matched_shares, event.matched_shares
                )
                order.off_chain_pending_shares = min(
                    order.off_chain_pending_shares, event.unmatched_shares
                )
                order.off_chain_invalid_shares = ZERO
                if order.off_chain_matched_shares == ZERO:
                    order.status = ManagedOrderStatus.ZERO_MATCHED
                elif order.off_chain_matched_shares < _MATCHED_SHARES_RATE * order.shares:
                    order.status = ManagedOrderStatus.PART_MATCHED
                else:
                    order.status = ManagedOrderStatus.FULL_MATCHED

    async def handle_trade_event(self, event: MarketTradeEvent) -> None:
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
                    status=ManagedTradeStatus.PENDING,
                    shares=event.shares,
                    side=event.side,
                    role=event.role,
                    price=event.price,
                    created_ts_ms=event.exch_ts_ms,
                    updated_ts_ms=event.exch_ts_ms,
                    on_chain_pending_shares=event.shares,
                    on_chain_settled_shares=ZERO,
                    on_chain_failure_shares=ZERO,
                )

            trade.updated_ts_ms = max(trade.updated_ts_ms, event.exch_ts_ms)

            if event.status == MarketTradeStatus.CONFIRMED:
                trade.status = ManagedTradeStatus.SUCCESS
                trade.on_chain_pending_shares = ZERO
                trade.on_chain_settled_shares = trade.shares
                trade.on_chain_failure_shares = ZERO
            elif event.status == MarketTradeStatus.FAILED:
                trade.status = ManagedTradeStatus.FAILURE
                trade.on_chain_pending_shares = ZERO
                trade.on_chain_settled_shares = ZERO
                trade.on_chain_failure_shares = trade.shares
            else:
                trade.status = ManagedTradeStatus.PENDING
                trade.on_chain_pending_shares = trade.shares
                trade.on_chain_settled_shares = ZERO
                trade.on_chain_failure_shares = ZERO

            order.trades[event.trade_id] = trade
            self._trades_by_trade_id[event.trade_id] = trade

    def _track_background_task(self, coro: Coroutine[Any, Any, None], *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._on_background_task_done)

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("background task failed: %s", task.get_name())
