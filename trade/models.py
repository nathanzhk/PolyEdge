from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from models.market import Market, Token
from models.market_order import OrderSide

TradePurpose = Literal["increase", "reduce"]

OrderStatus = Literal["PENDING", "MATCHED", "INVALID"]


class ManagedOrderStatus(StrEnum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    SUBMIT_FAILED = "SUBMIT_FAILED"
    PENDING_MATCH = "PENDING_MATCH"
    MATCHED = "MATCHED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCEL_FAILED = "CANCEL_FAILED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


class ManagedTradeStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(slots=True)
class ManagedOrder:
    local_id: str
    signal_id: str
    purpose: TradePurpose
    market: Market
    token: Token
    side: OrderSide
    ordered_shares: float
    price: float
    as_maker: bool
    order_id: str | None
    created_ts_ms: int
    updated_ts_ms: int
    status: ManagedOrderStatus
    off_chain_pending_shares: float = 0.0
    off_chain_matched_shares: float = 0.0
    off_chain_invalid_shares: float = 0.0
    replace_count: int = 0
    cancel_attempts: int = 0
    last_error: str | None = None

    @property
    def shares(self) -> float:
        return self.ordered_shares

    @property
    def matched_shares(self) -> float:
        return self.off_chain_matched_shares

    @property
    def remaining_shares(self) -> float:
        return self.off_chain_pending_shares


@dataclass(slots=True)
class ManagedTrade:
    trade_id: str
    order_id: str
    market_id: str
    token_id: str
    shares: float
    raw_status: str
    status: ManagedTradeStatus
    created_ts_ms: int
    updated_ts_ms: int
    on_chain_pending_shares: float = 0.0
    on_chain_success_shares: float = 0.0
    on_chain_failure_shares: float = 0.0


@dataclass(slots=True)
class Position:
    token: Token
    shares: float = 0.0
    updated_ts_ms: int = 0
    source: Literal["local", "exchange"] = "local"


@dataclass(slots=True, frozen=True)
class TradeLatestState:
    open_orders: tuple[ManagedOrder, ...]
    positions: dict[str, Position]
    unknown_order_count: int = 0

    def position_shares(self, token: Token | None) -> float:
        if token is None:
            return 0.0
        position = self.positions.get(token.id)
        return 0.0 if position is None else position.shares

    def has_open_signal(self, signal_id: str) -> bool:
        return any(order.signal_id == signal_id for order in self.open_orders)
