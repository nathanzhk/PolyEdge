from __future__ import annotations

import asyncio
import threading
import uuid
from dataclasses import dataclass, field

from enums import MarketOrderStatus, MarketOrderType, MarketTradeStatus, Role, Side
from events import MarketOrderEvent, MarketQuoteEvent, MarketTradeEvent
from markets.base import Token
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("PAPER")

_EVENT_SOURCE = "paper"
_QUOTE_STALE_MS = 2_000


@dataclass(slots=True)
class _PaperTrade:
    trade_id: str
    order_id: str
    market_id: str
    token_id: str
    side: Side
    role: Role
    price: float
    shares: float
    status: MarketTradeStatus
    created_ts_ms: int
    updated_ts_ms: int


@dataclass(slots=True)
class _PaperOrder:
    order_id: str
    market_id: str
    token_id: str
    side: Side
    role: Role
    type: MarketOrderType
    price: float
    shares: float
    status: MarketOrderStatus
    created_ts_ms: int
    updated_ts_ms: int
    matched_shares: float = 0.0
    trade_ids: list[str] = field(default_factory=list)

    @property
    def unmatched_shares(self) -> float:
        return round(max(self.shares - self.matched_shares, 0.0), 6)


class PaperExchangeSimulator:
    def __init__(
        self,
        *,
        submit_latency_s: float = 0.05,
        settle_latency_s: float = 0.25,
    ) -> None:
        self._loop = asyncio.get_running_loop()
        self._state_lock = threading.RLock()
        self._events: asyncio.Queue[MarketOrderEvent | MarketTradeEvent] = asyncio.Queue()
        self._orders_by_id: dict[str, _PaperOrder] = {}
        self._trades_by_id: dict[str, _PaperTrade] = {}
        self._latest_quotes: dict[str, MarketQuoteEvent] = {}
        self._pending_tasks: set[asyncio.Task[None]] = set()
        self._submit_latency_s = submit_latency_s
        self._settle_latency_s = settle_latency_s

    async def next_event(self) -> MarketOrderEvent | MarketTradeEvent:
        return await self._events.get()

    def submit_order(
        self,
        *,
        token: Token,
        side: Side,
        role: Role,
        shares: float,
        price: float,
    ) -> str | None:
        if shares <= 0:
            return None
        order_id = uuid.uuid4().hex
        now = now_ts_ms()
        order = _PaperOrder(
            order_id=order_id,
            market_id=token.market_id,
            token_id=token.id,
            side=side,
            role=role,
            type=MarketOrderType.GTC if role == Role.MAKER else MarketOrderType.FOK,
            price=round(price, 3),
            shares=round(shares, 6),
            status=MarketOrderStatus.LIVE,
            created_ts_ms=now,
            updated_ts_ms=now,
        )

        with self._state_lock:
            self._orders_by_id[order_id] = order

        if self._submit_latency_s > 0:
            import time

            time.sleep(self._submit_latency_s)

        if role == Role.TAKER:
            matched = self._fill_taker_order(order_id)
            if not matched:
                with self._state_lock:
                    self._orders_by_id.pop(order_id, None)
                logger.info("reject taker order without marketable quote: %s", order_id)
                return None

        return order_id

    def cancel_order(self, order_id: str) -> tuple[bool, str]:
        with self._state_lock:
            order = self._orders_by_id.get(order_id)
            if order is None:
                return False, "order not found"
            if order.unmatched_shares <= 0:
                return False, "matched"
            if order.status == MarketOrderStatus.CANCELED:
                return True, ""
            order.status = MarketOrderStatus.CANCELED
            order.updated_ts_ms = now_ts_ms()
            event = self._build_order_event(order)
        self._emit(event)
        return True, ""

    def get_order_by_id(self, order_id: str) -> MarketOrderEvent | None:
        with self._state_lock:
            order = self._orders_by_id.get(order_id)
            if order is None:
                return None
            return self._build_order_event(order)

    def get_trade_by_id(self, trade_id: str) -> MarketTradeEvent | None:
        with self._state_lock:
            trade = self._trades_by_id.get(trade_id)
            if trade is None:
                return None
            return self._build_trade_event(trade)

    def on_quote(self, quote: MarketQuoteEvent) -> None:
        matched_order_ids: list[str] = []
        with self._state_lock:
            self._latest_quotes[quote.token.id] = quote
            for order in self._orders_by_id.values():
                if order.role != Role.MAKER:
                    continue
                if order.token_id != quote.token.id:
                    continue
                if order.status == MarketOrderStatus.CANCELED or order.unmatched_shares <= 0:
                    continue
                if self._is_marketable(order, quote):
                    matched_order_ids.append(order.order_id)

        for order_id in matched_order_ids:
            self._fill_maker_order(order_id, quote)

    def close(self) -> None:
        for task in list(self._pending_tasks):
            task.cancel()

    def _fill_taker_order(self, order_id: str) -> bool:
        with self._state_lock:
            order = self._orders_by_id.get(order_id)
            if order is None:
                return False
            quote = self._latest_quotes.get(order.token_id)
            fill_price = self._pick_taker_fill_price(order, quote)
            if fill_price is None:
                return False
        self._fill_order(order_id, fill_price)
        return True

    def _fill_maker_order(self, order_id: str, quote: MarketQuoteEvent) -> None:
        with self._state_lock:
            order = self._orders_by_id.get(order_id)
            if order is None:
                return
            fill_price = order.price
            if not self._is_marketable(order, quote):
                return
        self._fill_order(order_id, fill_price)

    def _fill_order(self, order_id: str, fill_price: float) -> None:
        with self._state_lock:
            order = self._orders_by_id.get(order_id)
            if order is None:
                return
            remaining = order.unmatched_shares
            if remaining <= 0 or order.status == MarketOrderStatus.CANCELED:
                return

            ts_ms = now_ts_ms()
            order.matched_shares = round(order.matched_shares + remaining, 6)
            order.status = MarketOrderStatus.MATCHED
            order.updated_ts_ms = ts_ms

            trade = _PaperTrade(
                trade_id=uuid.uuid4().hex,
                order_id=order.order_id,
                market_id=order.market_id,
                token_id=order.token_id,
                side=order.side,
                role=order.role,
                price=round(fill_price, 3),
                shares=remaining,
                status=MarketTradeStatus.MATCHED,
                created_ts_ms=ts_ms,
                updated_ts_ms=ts_ms,
            )
            order.trade_ids.append(trade.trade_id)
            self._trades_by_id[trade.trade_id] = trade

            order_event = self._build_order_event(order)
            trade_event = self._build_trade_event(trade)

        self._emit(order_event)
        self._emit(trade_event)
        self._schedule_settlement(trade.trade_id)

    def _schedule_settlement(self, trade_id: str) -> None:
        def _spawn() -> None:
            task = asyncio.create_task(
                self._settle_trade(trade_id), name=f"paper-settle-{trade_id}"
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._on_task_done)

        self._loop.call_soon_threadsafe(_spawn)

    async def _settle_trade(self, trade_id: str) -> None:
        await asyncio.sleep(self._settle_latency_s)
        with self._state_lock:
            trade = self._trades_by_id.get(trade_id)
            if trade is None:
                return
            trade.status = MarketTradeStatus.MINED
            trade.updated_ts_ms = now_ts_ms()
            event = self._build_trade_event(trade)
        self._emit(event)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        self._pending_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("paper task failed: %s", task.get_name())

    def _pick_taker_fill_price(
        self,
        order: _PaperOrder,
        quote: MarketQuoteEvent | None,
    ) -> float | None:
        if quote is None:
            return order.price
        if quote.token.id != order.token_id:
            return None
        if (now_ts_ms() - quote.recv_ts_ms) > _QUOTE_STALE_MS:
            return None
        if order.side == Side.BUY:
            return quote.best_ask if quote.best_ask <= order.price else None
        return quote.best_bid if quote.best_bid >= order.price else None

    @staticmethod
    def _is_marketable(order: _PaperOrder, quote: MarketQuoteEvent) -> bool:
        if order.side == Side.BUY:
            return quote.best_ask <= order.price
        return quote.best_bid >= order.price

    @staticmethod
    def _build_order_event(order: _PaperOrder) -> MarketOrderEvent:
        return MarketOrderEvent(
            event_source=_EVENT_SOURCE,
            exch_ts_ms=order.updated_ts_ms,
            market_id=order.market_id,
            token_id=order.token_id,
            order_id=order.order_id,
            trade_ids=list(order.trade_ids),
            status=order.status,
            shares=order.shares,
            side=order.side,
            type=order.type,
            price=order.price,
            matched_shares=order.matched_shares,
        )

    @staticmethod
    def _build_trade_event(trade: _PaperTrade) -> MarketTradeEvent:
        return MarketTradeEvent(
            event_source=_EVENT_SOURCE,
            exch_ts_ms=trade.updated_ts_ms,
            market_id=trade.market_id,
            token_id=trade.token_id,
            order_id=trade.order_id,
            trade_id=trade.trade_id,
            status=trade.status,
            shares=trade.shares,
            side=trade.side,
            role=trade.role,
            price=trade.price,
        )

    def _emit(self, event: MarketOrderEvent | MarketTradeEvent) -> None:
        self._loop.call_soon_threadsafe(self._events.put_nowait, event)
