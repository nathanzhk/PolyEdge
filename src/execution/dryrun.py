from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from enums import Side
from event_bus import EventBus, OverflowPolicy, Subscription
from events import CurrentPositionEvent, DesiredPositionEvent, MarketQuoteEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import now_ts_ms

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("PAPER")

ZERO = 0.0
SHARES_EPSILON = 0.000001


@dataclass(slots=True)
class _PaperOrder:
    id: str
    market: Market
    token: Token
    side: Side
    price: float
    shares: float
    force: bool
    created_ts_ms: int
    updated_ts_ms: int


@dataclass(slots=True)
class _PaperPosition:
    market: Market
    token: Token
    holding_shares: float = ZERO
    holding_cost: float = ZERO
    holding_open_ts_ms: int | None = None

    @property
    def holding_avg_price(self) -> float | None:
        if self.holding_shares <= SHARES_EPSILON:
            return None
        return round(self.holding_cost / self.holding_shares, 3)


class PaperExecutionEngine:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._market_id: str | None = None
        self._tokens_by_token_id: dict[str, Token] = {}
        self._markets_by_token_id: dict[str, Market] = {}
        self._quotes_by_token_id: dict[str, MarketQuoteEvent] = {}
        self._positions_by_token_id: dict[str, _PaperPosition] = {}
        self._orders_by_token_id: dict[str, _PaperOrder] = {}

    async def handle_market_quote(
        self,
        quote: MarketQuoteEvent,
    ) -> tuple[CurrentPositionEvent, ...]:
        async with self._lock:
            self._ensure_market(quote.market)
            self._tokens_by_token_id[quote.token.id] = quote.token
            self._markets_by_token_id[quote.token.id] = quote.market
            self._quotes_by_token_id[quote.token.id] = quote

            order = self._orders_by_token_id.get(quote.token.id)
            if order is None or not self._can_fill(order, quote):
                return ()

            token_id = order.token.id
            self._fill_order(order, quote)
            return (self._build_position_event(token_id),)

    async def handle_desired_position(
        self,
        target: DesiredPositionEvent,
    ) -> tuple[CurrentPositionEvent, ...]:
        async with self._lock:
            self._ensure_market(target.market)
            self._tokens_by_token_id[target.token.id] = target.token
            self._markets_by_token_id[target.token.id] = target.market

            changed_token_ids: set[str] = set()
            changed_token_ids.update(self._cancel_other_token_orders(target.token))

            position = self._position_for(target.market, target.token)
            target_shares = max(ZERO, round(target.shares, 6))
            holding_shares = position.holding_shares

            if target_shares > holding_shares + SHARES_EPSILON:
                changed_token_id = self._ensure_order(
                    target,
                    side=Side.BUY,
                    shares=target_shares - holding_shares,
                )
                if changed_token_id is not None:
                    changed_token_ids.add(changed_token_id)
            elif target_shares < holding_shares - SHARES_EPSILON:
                changed_token_id = self._ensure_order(
                    target,
                    side=Side.SELL,
                    shares=holding_shares - target_shares,
                )
                if changed_token_id is not None:
                    changed_token_ids.add(changed_token_id)
            else:
                canceled_token_id = self._cancel_order_for_token(target.token.id)
                if canceled_token_id is not None:
                    changed_token_ids.add(canceled_token_id)

            return tuple(self._build_position_event(token_id) for token_id in changed_token_ids)

    def _ensure_market(self, market: Market) -> None:
        if self._market_id == market.id:
            return
        if self._market_id is not None:
            logger.info("switch market: %s -> %s", self._market_id, market.id)
        self._market_id = market.id
        self._tokens_by_token_id.clear()
        self._markets_by_token_id.clear()
        self._quotes_by_token_id.clear()
        self._positions_by_token_id.clear()
        self._orders_by_token_id.clear()

    def _position_for(self, market: Market, token: Token) -> _PaperPosition:
        position = self._positions_by_token_id.get(token.id)
        if position is None:
            position = _PaperPosition(market=market, token=token)
            self._positions_by_token_id[token.id] = position
        return position

    def _cancel_other_token_orders(self, token: Token) -> set[str]:
        canceled_token_ids: set[str] = set()
        for token_id in list(self._orders_by_token_id):
            if token_id == token.id:
                continue
            self._cancel_order_for_token(token_id)
            canceled_token_ids.add(token_id)
        return canceled_token_ids

    def _cancel_order_for_token(self, token_id: str) -> str | None:
        order = self._orders_by_token_id.pop(token_id, None)
        if order is None:
            return None
        logger.info(
            "cancel %s %s %.6f @ %.3f",
            order.side.lower(),
            order.token.key,
            order.shares,
            order.price,
        )
        return token_id

    def _ensure_order(
        self,
        target: DesiredPositionEvent,
        *,
        side: Side,
        shares: float,
    ) -> str | None:
        shares = max(ZERO, round(shares, 6))
        if side == Side.SELL:
            shares = min(shares, self._position_for(target.market, target.token).holding_shares)
        if shares <= SHARES_EPSILON:
            self._cancel_order_for_token(target.token.id)
            return target.token.id

        now = now_ts_ms()
        order = self._orders_by_token_id.get(target.token.id)
        if order is None or order.side != side:
            order = _PaperOrder(
                id=uuid.uuid4().hex,
                market=target.market,
                token=target.token,
                side=side,
                price=round(target.price, 3),
                shares=shares,
                force=target.force,
                created_ts_ms=now,
                updated_ts_ms=now,
            )
            self._orders_by_token_id[target.token.id] = order
            logger.info(
                "submit %s %s %.6f @ %.3f force=%s",
                order.side.lower(),
                order.token.key,
                order.shares,
                order.price,
                order.force,
            )
        else:
            price = round(target.price, 3)
            quote = self._quotes_by_token_id.get(target.token.id)
            if (
                order.price == price
                and abs(order.shares - shares) <= SHARES_EPSILON
                and order.force == target.force
            ):
                if order.force or (quote is not None and self._can_fill(order, quote)):
                    self._fill_order(order, quote)
                    return target.token.id
                return None
            order.price = price
            order.shares = shares
            order.force = target.force
            order.updated_ts_ms = now
            logger.info(
                "replace %s %s %.6f @ %.3f force=%s",
                order.side.lower(),
                order.token.key,
                order.shares,
                order.price,
                order.force,
            )

        quote = self._quotes_by_token_id.get(target.token.id)
        if order.force or (quote is not None and self._can_fill(order, quote)):
            self._fill_order(order, quote)
        return target.token.id

    def _can_fill(self, order: _PaperOrder, quote: MarketQuoteEvent) -> bool:
        if order.force:
            return True
        if order.side == Side.BUY:
            return quote.best_ask <= order.price
        return quote.best_bid >= order.price

    def _fill_order(self, order: _PaperOrder, quote: MarketQuoteEvent | None) -> None:
        self._orders_by_token_id.pop(order.token.id, None)
        price = self._fill_price(order, quote)
        shares = order.shares
        position = self._position_for(order.market, order.token)

        if order.side == Side.BUY:
            position.holding_cost = round(position.holding_cost + shares * price, 6)
            position.holding_shares = round(position.holding_shares + shares, 6)
            if position.holding_open_ts_ms is None:
                position.holding_open_ts_ms = order.created_ts_ms
        else:
            shares = min(shares, position.holding_shares)
            avg_price = position.holding_avg_price or ZERO
            position.holding_cost = round(position.holding_cost - shares * avg_price, 6)
            position.holding_shares = round(position.holding_shares - shares, 6)

        if position.holding_shares <= SHARES_EPSILON:
            position.holding_shares = ZERO
            position.holding_cost = ZERO
            position.holding_open_ts_ms = None

        logger.info(
            "fill %s %s %.6f @ %.3f holding=%.6f avg=%s",
            order.side.lower(),
            order.token.key,
            shares,
            price,
            position.holding_shares,
            position.holding_avg_price,
        )

    def _fill_price(self, order: _PaperOrder, quote: MarketQuoteEvent | None) -> float:
        if quote is None or not order.force:
            return order.price
        if order.side == Side.BUY:
            return quote.best_ask
        return quote.best_bid

    def _build_position_event(self, token_id: str) -> CurrentPositionEvent:
        token = self._tokens_by_token_id[token_id]
        order = self._orders_by_token_id.get(token_id)
        position = self._positions_by_token_id.get(token_id)
        quote = self._quotes_by_token_id.get(token_id)

        opening_shares = order.shares if order is not None and order.side == Side.BUY else ZERO
        closing_shares = order.shares if order is not None and order.side == Side.SELL else ZERO
        holding_shares = position.holding_shares if position is not None else ZERO
        holding_cost = position.holding_cost if position is not None else ZERO
        holding_avg_price = position.holding_avg_price if position is not None else None
        holding_open_ts_ms = position.holding_open_ts_ms if position is not None else None
        market = self._market_for_position_event(token_id, position, order, quote)
        price = self._position_price(
            order=order,
            quote=quote,
            holding_avg_price=holding_avg_price,
        )

        return CurrentPositionEvent(
            market=market,
            token=token,
            shares=round(holding_shares + opening_shares - closing_shares, 6),
            price=price,
            opening_shares=round(opening_shares, 6),
            holding_shares=round(holding_shares, 6),
            closing_shares=round(closing_shares, 6),
            holding_avg_price=holding_avg_price,
            holding_cost=round(holding_cost, 6),
            holding_open_ts_ms=holding_open_ts_ms,
        )

    def _market_for_position_event(
        self,
        token_id: str,
        position: _PaperPosition | None,
        order: _PaperOrder | None,
        quote: MarketQuoteEvent | None,
    ) -> Market:
        if position is not None:
            return position.market
        if order is not None:
            return order.market
        if quote is not None:
            return quote.market
        return self._markets_by_token_id[token_id]

    def _position_price(
        self,
        *,
        order: _PaperOrder | None,
        quote: MarketQuoteEvent | None,
        holding_avg_price: float | None,
    ) -> float:
        if order is not None:
            return order.price
        if holding_avg_price is not None:
            return holding_avg_price
        if quote is not None:
            return quote.mid
        return ZERO


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
