from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from enums import Role, Side
from event_bus import EventBus, OverflowPolicy, Subscription
from events import CurrentPositionEvent, DesiredPositionEvent, MarketQuoteEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.notification import send_trade
from utils.time import now_ts_ms

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("PAPER")

_ZERO = 0.0
_SUBMIT_DELAY_S = 0.2
_REPLACE_TTL_S = 2.00
_REPLACE_PRICE_GAP = 0.05
_REPLACE_SHARES_GAP = 0.10
_POSITION_SHARES_DUST = 0.02


def _pick_price(desired: DesiredPositionEvent, side: Side, force: bool, shares: float) -> float:
    """Maker buy → bid, maker sell → ask, taker buy → ask, taker sell → bid."""
    as_maker = not force and shares >= 5.0
    if side == Side.BUY:
        return desired.best_bid if as_maker else desired.best_ask
    else:
        return desired.best_ask if as_maker else desired.best_bid


class _PaperOrderStatus(StrEnum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    MATCHED = "MATCHED"
    CANCELED = "CANCELED"


@dataclass(slots=True)
class _PaperOrder:
    local_id: str
    market: Market
    token: Token
    side: Side
    role: Role
    price: float
    shares: float
    status: _PaperOrderStatus
    created_ts_ms: int


@dataclass(slots=True)
class _Position:
    token_id: str
    cost: float = _ZERO
    shares: float = _ZERO
    open_ts_ms: int | None = None
    realized_pnl: float = _ZERO

    @property
    def avg_price(self) -> float:
        if self.shares <= _POSITION_SHARES_DUST:
            return _ZERO
        return round(self.cost / self.shares, 3)


class PaperExecutionEngine:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._orders: dict[str, _PaperOrder] = {}
        self._positions: dict[str, _Position] = {}
        self._pending_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------
    # Public interface (called by PaperExecutionComponent)
    # ------------------------------------------------------------------

    async def handle_desired_position(
        self, desired: DesiredPositionEvent
    ) -> tuple[CurrentPositionEvent, ...]:
        async with self._lock:
            position = self._positions.get(desired.token.id)
            holding_shares = position.shares if position else _ZERO

            opening_shares = _ZERO
            closing_shares = _ZERO
            active_order: _PaperOrder | None = None
            for order in self._orders.values():
                if order.token.id != desired.token.id:
                    continue
                if order.status not in (_PaperOrderStatus.PENDING, _PaperOrderStatus.ACTIVE):
                    continue
                if order.side == Side.BUY:
                    opening_shares += order.shares
                else:
                    closing_shares += order.shares
                active_order = order

            desired_shares = round(desired.shares, 6)
            current_shares = round(opening_shares + holding_shares - closing_shares, 6)

            if desired_shares > current_shares:
                delta = round(desired_shares - current_shares, 6)
                self._reconcile_order(
                    desired, side=Side.BUY, shares=delta, active_order=active_order
                )
            elif desired_shares < current_shares:
                delta = round(current_shares - desired_shares, 6)
                sellable_shares = max(round(holding_shares - closing_shares, 6), _ZERO)
                sell_shares = delta
                if active_order is None or active_order.side == Side.SELL:
                    sell_shares = min(delta, sellable_shares)
                self._reconcile_order(
                    desired, side=Side.SELL, shares=sell_shares, active_order=active_order
                )
            elif active_order is not None:
                self._reconcile_order(
                    desired,
                    side=active_order.side,
                    shares=active_order.shares,
                    active_order=active_order,
                )

        return ()

    async def handle_market_quote(
        self, quote: MarketQuoteEvent
    ) -> tuple[CurrentPositionEvent, ...]:
        results: list[CurrentPositionEvent] = []
        async with self._lock:
            for order in list(self._orders.values()):
                if order.status != _PaperOrderStatus.ACTIVE:
                    continue
                if order.token.id != quote.token.id:
                    continue
                if not self._is_price_matched(order, quote):
                    continue
                self._fill_order(order, quote.exch_ts_ms)
                results.append(self._build_current_position_event(order.market, order.token))
        return tuple(results)

    # ------------------------------------------------------------------
    # Reconciliation (mirrors ExecutionEngine._reconcile_order)
    # ------------------------------------------------------------------

    def _reconcile_order(
        self,
        desired: DesiredPositionEvent,
        *,
        side: Side,
        shares: float,
        active_order: _PaperOrder | None,
    ) -> None:
        if active_order is None:
            price = _pick_price(desired, side, desired.force, shares)
            self._submit_order(desired.market, desired.token, side, shares, price, desired.force)
            return

        if active_order.side != side:
            self._cancel_order(active_order, reason="target side changed")
            return

        now = now_ts_ms()
        age_s = (now - active_order.created_ts_ms) / 1000
        ttl_expired = not desired.force and age_s >= _REPLACE_TTL_S
        new_price = _pick_price(desired, side, desired.force, shares)
        price_moved = abs(active_order.price - new_price) >= _REPLACE_PRICE_GAP
        shares_changed = abs(active_order.shares - shares) >= _REPLACE_SHARES_GAP
        if not ttl_expired and not price_moved and not shares_changed:
            return

        if ttl_expired:
            reason = "ttl expired"
        elif price_moved:
            reason = "price changed"
        else:
            reason = "shares changed"
        logger.info(
            "%s: cancel paper order %s for replace %.6f @ %.2f -> %.6f @ %.2f",
            reason,
            active_order.local_id,
            active_order.shares,
            active_order.price,
            shares,
            new_price,
        )
        self._cancel_order(active_order, reason=reason)

    # ------------------------------------------------------------------
    # Order submission / cancellation
    # ------------------------------------------------------------------

    def _submit_order(
        self, market: Market, token: Token, side: Side, shares: float, price: float, force: bool
    ) -> None:
        local_id = uuid.uuid4().hex
        as_maker = not force and shares >= 5.0
        order = _PaperOrder(
            local_id=local_id,
            market=market,
            token=token,
            side=side,
            role=Role.MAKER if as_maker else Role.TAKER,
            price=price,
            shares=shares,
            status=_PaperOrderStatus.PENDING,
            created_ts_ms=now_ts_ms(),
        )
        self._orders[local_id] = order
        logger.info("submit paper order: %s %s %.6f @ %.2f", local_id, side, shares, price)

        task = asyncio.create_task(
            self._activate_after_delay(local_id), name=f"paper-activate-{local_id}"
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _activate_after_delay(self, local_id: str) -> None:
        await asyncio.sleep(_SUBMIT_DELAY_S)
        async with self._lock:
            order = self._orders.get(local_id)
            if order is not None and order.status == _PaperOrderStatus.PENDING:
                order.status = _PaperOrderStatus.ACTIVE
                logger.info("paper order active: %s", local_id)

    def _cancel_order(self, order: _PaperOrder, *, reason: str) -> None:
        order.status = _PaperOrderStatus.CANCELED
        logger.info("cancel paper order: %s (%s)", order.local_id, reason)

    # ------------------------------------------------------------------
    # Matching & filling
    # ------------------------------------------------------------------

    @staticmethod
    def _is_price_matched(order: _PaperOrder, quote: MarketQuoteEvent) -> bool:
        if order.side == Side.BUY:
            return quote.best_ask <= order.price
        else:
            return quote.best_bid >= order.price

    def _fill_order(self, order: _PaperOrder, ts_ms: int) -> None:
        order.status = _PaperOrderStatus.MATCHED

        position = self._positions.get(order.token.id)
        if position is None:
            position = _Position(token_id=order.token.id)
            self._positions[order.token.id] = position

        realized_pnl_before = position.realized_pnl

        if order.side == Side.BUY:
            position.cost = round(position.cost + order.shares * order.price, 6)
            position.shares = round(position.shares + order.shares, 6)
            position.open_ts_ms = (
                ts_ms if position.open_ts_ms is None else min(position.open_ts_ms, ts_ms)
            )
        else:
            reduced = min(order.shares, position.shares)
            avg = position.avg_price or _ZERO
            position.cost = round(position.cost - reduced * avg, 6)
            position.shares = round(position.shares - reduced, 6)
            position.realized_pnl = round(position.realized_pnl + reduced * (order.price - avg), 6)
            if order.shares > reduced:
                logger.warning(
                    "paper sell exceeds holding: token=%s sell=%.6f holding=%.6f",
                    order.token.id,
                    order.shares,
                    reduced,
                )

        if position.shares <= _POSITION_SHARES_DUST:
            position.cost = _ZERO
            position.shares = _ZERO
            position.open_ts_ms = None

        pnl = position.realized_pnl - realized_pnl_before if order.side == Side.SELL else None
        logger.info(
            "paper fill: %s %s %.6f @ %.2f pnl=%s",
            order.local_id,
            order.side,
            order.shares,
            order.price,
            pnl,
        )

        task = asyncio.create_task(
            asyncio.to_thread(
                send_trade,
                market_start_ms=order.market.start_ts_ms,
                market_end_ms=order.market.end_ts_ms,
                side=order.side,
                token=order.token.key,
                shares=order.shares,
                price=order.price,
                pnl=pnl,
            ),
            name=f"paper-send-trade-{order.local_id}",
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    # ------------------------------------------------------------------
    # Position event building
    # ------------------------------------------------------------------

    def _build_current_position_event(self, market: Market, token: Token) -> CurrentPositionEvent:
        opening_shares = _ZERO
        closing_shares = _ZERO
        for order in self._orders.values():
            if order.token.id != token.id:
                continue
            if order.status not in (_PaperOrderStatus.PENDING, _PaperOrderStatus.ACTIVE):
                continue
            if order.side == Side.BUY:
                opening_shares += order.shares
            else:
                closing_shares += order.shares

        position = self._positions.get(token.id)
        return CurrentPositionEvent(
            token=token,
            market=market,
            opening_shares=round(opening_shares, 6),
            open_settling_shares=_ZERO,
            holding_shares=position.shares if position else _ZERO,
            closing_shares=round(closing_shares, 6),
            close_settling_shares=_ZERO,
            holding_avg_price=position.avg_price if position else None,
            holding_cost=position.cost if position else _ZERO,
            realized_pnl=position.realized_pnl if position else _ZERO,
        )


class PaperExecutionComponent:
    def __init__(self, *, bus: EventBus, engine: PaperExecutionEngine) -> None:
        self._bus = bus
        self._engine = engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        market_quote_events = self._bus.subscribe(
            MarketQuoteEvent,
            name="paper-execution.market-quote",
            maxsize=100,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._market_quote_loop(market_quote_events))

        desired_position_events = self._bus.subscribe(
            DesiredPositionEvent,
            name="paper-execution.desired-position",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._desired_position_loop(desired_position_events))

    async def _market_quote_loop(self, events: Subscription[MarketQuoteEvent]) -> None:
        async for quote in events:
            await self._publish_positions(await self._engine.handle_market_quote(quote))

    async def _desired_position_loop(self, targets: Subscription[DesiredPositionEvent]) -> None:
        async for target in targets:
            await self._publish_positions(await self._engine.handle_desired_position(target))

    async def _publish_positions(self, positions: tuple[CurrentPositionEvent, ...]) -> None:
        for position in positions:
            await self._bus.publish(position)


def paper_execution_component() -> ComponentFactory:
    return lambda context: PaperExecutionComponent(
        bus=context.bus,
        engine=PaperExecutionEngine(),
    )
