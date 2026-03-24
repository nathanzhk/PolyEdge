from __future__ import annotations

import asyncio
from dataclasses import dataclass

from enums import MarketTradeStatus, Side
from events import MarketTradeEvent
from utils.logger import get_logger

logger = get_logger("TRADE")

ZERO = 0.0
POSITION_EPSILON = 0.000001


@dataclass(slots=True, frozen=True)
class PositionSnapshot:
    token_id: str
    holding_shares: float = ZERO
    holding_cost: float = ZERO
    holding_open_ts_ms: int | None = None

    @property
    def holding_avg_price(self) -> float | None:
        if self.holding_shares <= POSITION_EPSILON:
            return None
        return round(self.holding_cost / self.holding_shares, 3)


@dataclass(slots=True)
class _Position:
    token_id: str
    holding_shares: float = ZERO
    holding_cost: float = ZERO
    holding_open_ts_ms: int | None = None

    @property
    def holding_avg_price(self) -> float | None:
        if self.holding_shares <= POSITION_EPSILON:
            return None
        return round(self.holding_cost / self.holding_shares, 3)

    def snapshot(self) -> PositionSnapshot:
        return PositionSnapshot(
            token_id=self.token_id,
            holding_shares=round(self.holding_shares, 6),
            holding_cost=round(self.holding_cost, 6),
            holding_open_ts_ms=self.holding_open_ts_ms,
        )


@dataclass(slots=True)
class _PositionTrade:
    trade_id: str
    token_id: str
    status: MarketTradeStatus
    side: Side
    shares: float
    price: float
    created_ts_ms: int
    updated_ts_ms: int
    applied_to_position: bool = False


class PositionManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._positions_by_token_id: dict[str, _Position] = {}
        self._trades_by_trade_id: dict[str, _PositionTrade] = {}

    async def position_for_token(self, token_id: str) -> PositionSnapshot:
        async with self._lock:
            position = self._positions_by_token_id.get(token_id)
            if position is None:
                return PositionSnapshot(token_id=token_id)
            return position.snapshot()

    async def handle_trade_event(self, event: MarketTradeEvent) -> None:
        async with self._lock:
            trade = self._trades_by_trade_id.get(event.trade_id)
            if trade is None:
                trade = _PositionTrade(
                    trade_id=event.trade_id,
                    token_id=event.token_id,
                    status=event.status,
                    side=event.side,
                    shares=event.shares,
                    price=event.price,
                    created_ts_ms=event.exch_ts_ms,
                    updated_ts_ms=event.exch_ts_ms,
                )
                self._trades_by_trade_id[event.trade_id] = trade
            else:
                trade.status = event.status
                trade.side = event.side
                trade.shares = event.shares
                trade.price = event.price
                trade.updated_ts_ms = max(trade.updated_ts_ms, event.exch_ts_ms)

            if event.status == MarketTradeStatus.CONFIRMED and not trade.applied_to_position:
                self._apply_confirmed_trade(trade)
                trade.applied_to_position = True

    def _apply_confirmed_trade(self, trade: _PositionTrade) -> None:
        position = self._positions_by_token_id.get(trade.token_id)
        if position is None:
            position = _Position(token_id=trade.token_id)
            self._positions_by_token_id[trade.token_id] = position

        if trade.side == Side.BUY:
            position.holding_cost = round(position.holding_cost + trade.shares * trade.price, 6)
            position.holding_shares = round(position.holding_shares + trade.shares, 6)
            position.holding_open_ts_ms = (
                min(position.holding_open_ts_ms, trade.created_ts_ms)
                if position.holding_open_ts_ms is not None
                else trade.created_ts_ms
            )
        else:
            holding_before = position.holding_shares
            reduce_shares = min(trade.shares, position.holding_shares)
            avg_price = position.holding_avg_price or ZERO
            position.holding_cost = round(position.holding_cost - reduce_shares * avg_price, 6)
            position.holding_shares = round(position.holding_shares - reduce_shares, 6)
            if trade.shares > reduce_shares:
                logger.warning(
                    "sell settled shares exceed holding: token=%s sell=%.6f holding=%.6f",
                    trade.token_id,
                    trade.shares,
                    holding_before,
                )

        if position.holding_shares <= POSITION_EPSILON:
            position.holding_shares = ZERO
            position.holding_cost = ZERO
            position.holding_open_ts_ms = None
