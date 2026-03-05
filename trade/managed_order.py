from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from markets.base import Market, Token
from utils.enum import ManagedOrderStatus, ManagedTradeStatus, MarketTradeStatus, Side

TradePurpose = Literal["increase", "reduce"]


@dataclass(slots=True)
class ManagedOrder:
    local_id: str
    purpose: TradePurpose

    market: Market
    token: Token

    side: Side
    price: float
    as_maker: bool
    order_id: str | None
    created_ts_ms: int
    updated_ts_ms: int
    status: ManagedOrderStatus
    ordered_shares: float
    off_chain_pending_shares: float = 0.0
    off_chain_matched_shares: float = 0.0
    off_chain_invalid_shares: float = 0.0
    replace_count: int = 0
    cancel_attempts: int = 0
    last_error: str | None = None
    status_before_cancel: ManagedOrderStatus | None = None
    trades: dict[str, ManagedTrade] = field(default_factory=dict)

    @property
    def matching_shares(self) -> float:
        return self.off_chain_pending_shares

    @property
    def matched_shares(self) -> float:
        return self.off_chain_matched_shares

    @property
    def cancelled_shares(self) -> float:
        return self.off_chain_invalid_shares

    @property
    def is_settled(self) -> bool:
        return self.matched_shares == self.settled_shares + self.settle_failed_shares

    @property
    def settled_shares(self) -> float:
        return round(sum(trade.on_chain_success_shares for trade in self.trades.values()), 6)

    @property
    def has_settle_failed(self) -> bool:
        return self.settle_failed_shares > 0

    @property
    def settle_failed_shares(self) -> float:
        return round(sum(trade.on_chain_failure_shares for trade in self.trades.values()), 6)


@dataclass(slots=True)
class ManagedTrade:
    trade_id: str
    order_id: str
    market_id: str
    token_id: str
    shares: float
    raw_status: MarketTradeStatus
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
