from __future__ import annotations

import asyncio
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any, Protocol

from clients.polymarket_clob import TradeClient
from enums import ManagedOrderStatus, ManagedTradeStatus, MarketTradeStatus, Role, Side
from events import CurrentPositionEvent, MarketOrderEvent, MarketTradeEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.notification import send_trade
from utils.time import now_ts_ms

logger = get_logger("MANAGER")

_ZERO = 0.0
_MATCHED_SHARES_RATE = 0.98
_POSITION_SHARES_DUST = 0.01


class EventPublisher(Protocol):
    async def publish(self, event: object) -> None: ...


@dataclass(slots=True)
class _Position:
    token_id: str
    cost: float = _ZERO
    shares: float = _ZERO
    realized_pnl: float = _ZERO

    @property
    def avg_price(self) -> float:
        if self.shares <= _POSITION_SHARES_DUST:
            return _ZERO
        return round(self.cost / self.shares, 3)


@dataclass(slots=True)
class ManagedOrder:
    local_id: str
    order_id: str | None

    market: Market
    token: Token

    status: ManagedOrderStatus
    shares: float

    side: Side
    role: Role
    price: float

    created_ts_ms: int
    updated_ts_ms: int

    off_chain_pending_shares: float
    off_chain_matched_shares: float
    off_chain_invalid_shares: float

    trades: dict[str, ManagedTrade] = field(default_factory=dict)
    should_cancel: bool = False
    is_cancelling: bool = False
    audit_records: list[tuple[int, str]] = field(default_factory=list)

    @property
    def is_active(self) -> bool:
        """off_chain_pending_shares > 0"""
        return (
            self.status == ManagedOrderStatus.SUBMITTING
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
    def on_chain_pending_shares(self) -> float:
        return round(sum(trade.on_chain_pending_shares for trade in self.trades.values()), 6)

    @property
    def on_chain_settled_shares(self) -> float:
        return round(sum(trade.on_chain_settled_shares for trade in self.trades.values()), 6)

    @property
    def on_chain_failure_shares(self) -> float:
        return round(sum(trade.on_chain_failure_shares for trade in self.trades.values()), 6)

    @property
    def on_chain_takefee_shares(self) -> float:
        return round(sum(trade.on_chain_takefee_shares for trade in self.trades.values()), 6)

    def log(self, message: str) -> None:
        self.audit_records.append((now_ts_ms(), message))


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
    on_chain_takefee_shares: float = 0.0


class OrderManager:
    def __init__(
        self,
        maker_client: TradeClient,
        taker_client: TradeClient,
        *,
        event_publisher: EventPublisher,
    ) -> None:
        self._lock = asyncio.Lock()
        self._maker_client = maker_client
        self._taker_client = taker_client
        self._event_publisher = event_publisher

        self._tokens_by_token_id: dict[str, Token] = {}
        self._orders_by_local_id: dict[str, ManagedOrder] = {}
        self._orders_by_order_id: dict[str, ManagedOrder] = {}
        self._trades_by_trade_id: dict[str, ManagedTrade] = {}
        self._positions_by_token_id: dict[str, _Position] = {}

        self._cached_order_events_by_order_id: dict[str, list[MarketOrderEvent]] = {}
        self._cached_trade_events_by_order_id: dict[str, list[MarketTradeEvent]] = {}

        self._background_tasks: set[asyncio.Task[None]] = set()

    async def buy(
        self, market: Market, token: Token, shares: float, price: float, force: bool
    ) -> None:
        await self._submit_order(Side.BUY, market, token, shares, price, force)

    async def sell(
        self, market: Market, token: Token, shares: float, price: float, force: bool
    ) -> None:
        await self._submit_order(Side.SELL, market, token, shares, price, force)

    async def get_position_by_token(
        self, market: Market, token: Token
    ) -> tuple[CurrentPositionEvent, ManagedOrder | None]:
        async with self._lock:
            active_order: ManagedOrder | None = None
            for order in self._orders_by_local_id.values():
                if order.is_active and order.token.id == token.id:
                    active_order = order
                    break
            return self._build_current_position_event(market, token), active_order

    async def _submit_order(
        self, side: Side, market: Market, token: Token, shares: float, price: float, force: bool
    ) -> None:
        now = now_ts_ms()
        local_id = uuid.uuid4().hex
        as_maker = not force and shares >= 5.0
        order = ManagedOrder(
            local_id=local_id,
            order_id=None,
            market=market,
            token=token,
            status=ManagedOrderStatus.SUBMITTING,
            shares=shares,
            side=side,
            role=Role.MAKER if as_maker else Role.TAKER,
            price=price,
            created_ts_ms=now,
            updated_ts_ms=now,
            off_chain_pending_shares=shares,
            off_chain_matched_shares=_ZERO,
            off_chain_invalid_shares=_ZERO,
        )
        logger.info("submit order: %s", local_id)
        async with self._lock:
            self._orders_by_local_id[local_id] = order
            self._tokens_by_token_id[token.id] = token

        self._track_background_task(
            self._submit_order_worker(local_id, market, token),
            name=f"submit-order-{local_id}",
        )

        await self._publish_current_position_event(market, token)

    async def _submit_order_worker(self, local_id: str, market: Market, token: Token) -> None:
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return
            client = self._maker_client if order.role == Role.MAKER else self._taker_client
            submit_order_func = client.buy if order.side == Side.BUY else client.sell
            shares = order.shares
            price = order.price
            order.log("submitting")

        try:
            order_id = await asyncio.to_thread(submit_order_func, order.token, shares, price)
        except Exception:
            await self._handle_submit_order_failed(local_id)
            raise

        if order_id is not None:
            await self._handle_submit_order_success(local_id, order_id)
        else:
            await self._handle_submit_order_failed(local_id)

        await self._publish_current_position_event(market, token)

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
            cached_events: list[MarketOrderEvent | MarketTradeEvent] = sorted(
                [
                    *self._cached_order_events_by_order_id.pop(order_id, []),
                    *self._cached_trade_events_by_order_id.pop(order_id, []),
                ],
                key=lambda e: e.exch_ts_ms,
            )

        for event in cached_events:
            if isinstance(event, MarketOrderEvent):
                await self.handle_order_event(event)
            else:
                await self.handle_trade_event(event)

        self._track_background_task(
            self._sync_order_status(order_id),
            name=f"sync-order-{order_id}",
        )

        if order.should_cancel:
            async with self._lock:
                order = self._orders_by_order_id.get(order_id)
                if order is None:
                    return
                order.should_cancel = False
                order.log("find should cancel")
            await self.cancel(
                order.local_id, order.order_id, reason="should cancel while submitting"
            )

    async def _handle_submit_order_failed(self, local_id: str) -> None:
        logger.warning("submit order failed: %s", local_id)
        async with self._lock:
            order = self._orders_by_local_id.get(local_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.off_chain_invalid_shares = order.off_chain_pending_shares
            order.off_chain_pending_shares = _ZERO
            if not order.should_cancel:
                order.status = ManagedOrderStatus.SUB_FAILED
                order.log("submit failed")
            else:
                order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
                order.should_cancel = False
                order.log("find should cancel")

    async def cancel(self, local_id: str, order_id: str | None, *, reason: str) -> bool:
        logger.info("cancel order: (%s) %s => %s", reason, local_id, order_id)
        async with self._lock:
            if order_id is None:
                order = self._orders_by_local_id.get(local_id)
            else:
                order = self._orders_by_order_id.get(order_id)
            if order is None:
                return False
            if not order.is_active:
                order.log(f"cancel requested: {reason}")
                order.log("cancel without active shares")
                return True
            if order.order_id is None:
                if order.status == ManagedOrderStatus.SUBMITTING:
                    order.should_cancel = True
                    order.updated_ts_ms = now_ts_ms()
                    order.log(f"cancel requested: {reason}")
                    order.log("cancel while submitting")
                    logger.info(
                        "order should cancel until submit returns: %s (%s)",
                        order.local_id,
                        reason,
                    )
                    return False
                else:
                    order.log(f"cancel requested: {reason}")
                    order.log("unexpected status")
                    logger.warning(
                        "unexpected order status without order id: %s %s",
                        order.local_id,
                        order.status,
                    )
                return False
            if order.is_cancelling or order.should_cancel:
                order.log(f"cancel requested: {reason}")
                order.log("cancel already in progress")
                return False
            order.updated_ts_ms = now_ts_ms()
            order.is_cancelling = True
            order.log(f"cancel requested: {reason}")

        self._track_background_task(
            self._cancel_order_worker(order.order_id, order.market, order.token),
            name=f"cancel-order-{order.order_id}",
        )
        return True

    async def _cancel_order_worker(self, order_id: str, market: Market, token: Token) -> None:
        try:
            is_success, error_message = await asyncio.to_thread(
                self._maker_client.cancel_order_by_id, order_id
            )
        except Exception as e:
            await self._handle_cancel_order_failed(order_id, f"cancel raised: {e!r}")
            raise

        if is_success:
            await self._handle_cancel_order_success(order_id)
        else:
            await self._handle_cancel_order_failed(order_id, error_message.lower())

        await self._publish_current_position_event(market, token)

    async def _handle_cancel_order_success(self, order_id: str) -> None:
        logger.info("cancel order success: %s", order_id)
        async with self._lock:
            order = self._orders_by_order_id.get(order_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.is_cancelling = False
            order.off_chain_invalid_shares = order.off_chain_pending_shares
            order.off_chain_pending_shares = _ZERO
            if order.off_chain_matched_shares == _ZERO:
                order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
            else:
                order.status = ManagedOrderStatus.PART_MATCHED_CANCELED
            order.log("canceled")

    async def _handle_cancel_order_failed(self, order_id: str, error_message: str) -> None:
        logger.info("cancel order failed: %s", order_id)
        async with self._lock:
            order = self._orders_by_order_id.get(order_id)
            if order is None:
                return
            order.updated_ts_ms = now_ts_ms()
            order.is_cancelling = False
            order.log(error_message)

        if "matched" in error_message or "canceled" in error_message:
            self._track_background_task(
                self._sync_order_status(order_id),
                name=f"sync-order-{order_id}",
            )

    async def _sync_order_status(self, order_id: str) -> None:
        order = await asyncio.to_thread(self._maker_client.get_order_by_id, order_id)
        if order is None:
            logger.warning("sync order status failed: %s", order_id)
        else:
            await self.handle_order_event(order)
            for trade_id in order.trade_ids:
                self._track_background_task(
                    self._sync_trade_status(trade_id),
                    name=f"sync-trade-{trade_id}",
                )

    async def _sync_trade_status(self, trade_id: str) -> None:
        trade = await asyncio.to_thread(self._maker_client.get_trade_by_id, trade_id)
        if trade is None:
            logger.warning("sync trade status failed: %s", trade_id)
        else:
            await self.handle_trade_event(trade)

    async def handle_order_event(self, event: MarketOrderEvent) -> None:
        self._maker_client.get_cash_balance()
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
                order.off_chain_pending_shares = _ZERO
                if order.off_chain_matched_shares == _ZERO:
                    order.status = ManagedOrderStatus.ZERO_MATCHED_CANCELED
                else:
                    order.status = ManagedOrderStatus.PART_MATCHED_CANCELED

            else:
                order.off_chain_matched_shares = max(
                    order.off_chain_matched_shares, event.matched_shares
                )
                order.off_chain_pending_shares = min(
                    order.off_chain_pending_shares, event.unmatched_shares
                )
                order.off_chain_invalid_shares = _ZERO
                if order.off_chain_matched_shares == _ZERO:
                    order.status = ManagedOrderStatus.ZERO_MATCHED
                elif order.off_chain_matched_shares < _MATCHED_SHARES_RATE * order.shares:
                    order.status = ManagedOrderStatus.PART_MATCHED
                else:
                    order.status = ManagedOrderStatus.FULL_MATCHED

            market = order.market
            token = order.token

        await self._publish_current_position_event(market, token)
        logger.debug(
            "order %s status=%s pending=%.6f matched=%.6f canceled=%.6f",
            order.order_id,
            order.status,
            order.off_chain_pending_shares,
            order.off_chain_matched_shares,
            order.off_chain_invalid_shares,
        )

    async def handle_trade_event(self, event: MarketTradeEvent) -> None:
        self._maker_client.get_cash_balance()
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
                    on_chain_settled_shares=_ZERO,
                    on_chain_failure_shares=_ZERO,
                    on_chain_takefee_shares=_ZERO,
                )

            trade.updated_ts_ms = max(trade.updated_ts_ms, event.exch_ts_ms)

            if event.role == Role.TAKER and event.side == Side.BUY:
                net_shares, fee_shares = self._taker_client.calc_net_buy_shares(
                    order.market, event.shares, event.price
                )
            else:
                net_shares, fee_shares = event.shares, _ZERO

            if event.role == Role.TAKER and event.side == Side.SELL:
                net_amount, fee_amount = self._taker_client.calc_net_sell_amount(
                    order.market, event.shares, event.price
                )
            else:
                net_amount, fee_amount = round(event.shares * event.price, 6), _ZERO

            if event.status == MarketTradeStatus.CONFIRMED:
                trade.status = ManagedTradeStatus.SUCCESS
                trade.on_chain_pending_shares = _ZERO
                trade.on_chain_settled_shares = net_shares
                trade.on_chain_failure_shares = _ZERO
            elif event.status == MarketTradeStatus.FAILED:
                trade.status = ManagedTradeStatus.FAILURE
                trade.on_chain_pending_shares = _ZERO
                trade.on_chain_settled_shares = _ZERO
                trade.on_chain_failure_shares = net_shares
            else:
                trade.status = ManagedTradeStatus.PENDING
                trade.on_chain_pending_shares = net_shares
                trade.on_chain_settled_shares = _ZERO
                trade.on_chain_failure_shares = _ZERO
            trade.on_chain_takefee_shares = fee_shares

            order.trades[event.trade_id] = trade
            self._trades_by_trade_id[event.trade_id] = trade

            if trade.status == ManagedTradeStatus.SUCCESS:
                position = self._positions_by_token_id.get(trade.token_id)
                realized_pnl = position.realized_pnl if position is not None else _ZERO

                self._apply_confirmed_trade(trade.side, trade.token_id, net_amount, net_shares)

                position = self._positions_by_token_id[event.token_id]
                pnl = position.realized_pnl - realized_pnl if event.side == Side.SELL else None
                self._track_background_task(
                    asyncio.to_thread(
                        send_trade,
                        market_start_ms=order.market.start_ts_ms,
                        market_end_ms=order.market.end_ts_ms,
                        side=event.side,
                        token=order.token.key,
                        shares=net_shares,
                        price=round(net_amount / net_shares, 3),
                        pnl=pnl,
                    ),
                    name=f"send-trade-{event.trade_id}",
                )

            market = order.market
            token = order.token

        await self._publish_current_position_event(market, token)
        logger.debug(
            "trade %s status=%s net_shares=%.6f fee_shares=%.6f net_amount=%.6f fee_amount=%.6f",
            trade.trade_id,
            trade.status,
            net_shares,
            fee_shares,
            net_amount,
            fee_amount,
        )

    def _apply_confirmed_trade(
        self, side: Side, token_id: str, amount: float, shares: float
    ) -> None:
        position = self._positions_by_token_id.get(token_id)
        if position is None:
            position = _Position(token_id=token_id)
            self._positions_by_token_id[token_id] = position

        if side == Side.BUY:
            position.cost = round(position.cost + amount, 6)
            position.shares = round(position.shares + shares, 6)
        else:
            cost = shares * position.avg_price
            position.cost = round(position.cost - cost, 6)
            position.shares = round(position.shares - shares, 6)
            position.realized_pnl = round(position.realized_pnl + amount - cost, 6)

        if position.shares <= _POSITION_SHARES_DUST:
            position.cost = _ZERO
            position.shares = _ZERO

    def _build_current_position_event(self, market: Market, token: Token) -> CurrentPositionEvent:

        opening_shares = _ZERO
        open_settling_shares = _ZERO
        closing_shares = _ZERO
        close_settling_shares = _ZERO
        for order in self._orders_by_local_id.values():
            if order.token.id != token.id:
                continue
            settling_shares = (
                order.off_chain_matched_shares
                - order.on_chain_settled_shares
                - order.on_chain_failure_shares
                - order.on_chain_takefee_shares
            )
            if order.side == Side.BUY:
                opening_shares += order.off_chain_pending_shares
                open_settling_shares += settling_shares
            else:
                closing_shares += order.off_chain_pending_shares
                close_settling_shares += settling_shares

        position = self._positions_by_token_id.get(token.id)
        return CurrentPositionEvent(
            token=token,
            market=market,
            opening_shares=round(opening_shares, 6),
            open_settling_shares=round(open_settling_shares, 6),
            closing_shares=round(closing_shares, 6),
            close_settling_shares=round(close_settling_shares, 6),
            holding_cost=position.cost if position is not None else _ZERO,
            holding_shares=position.shares if position is not None else _ZERO,
            holding_avg_price=position.avg_price if position is not None else _ZERO,
            realized_pnl=position.realized_pnl if position is not None else _ZERO,
        )

    async def _publish_current_position_event(self, market: Market, token: Token) -> None:
        async with self._lock:
            event = self._build_current_position_event(market, token)
        await self._event_publisher.publish(event)

    def _track_background_task(self, coroutine: Coroutine[Any, Any, None], *, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
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
