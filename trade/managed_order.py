from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from markets.base import Market, Token
from utils.enum import ManagedOrderStatus, ManagedTradeStatus, MarketTradeStatus, Side
from utils.time import now_ts_ms

TradePurpose = Literal["increase", "reduce"]

_MATCHED_SHARES_RATE = 0.98
_PENDING_SHARES_RATE = 1 - _MATCHED_SHARES_RATE


@dataclass(slots=True)
class AuditRecord:
    ts_ms: int
    message: str


@dataclass(slots=True)
class ManagedOrder:
    local_id: str
    order_id: str | None

    market: Market
    token: Token
    side: Side
    price: float
    status: ManagedOrderStatus
    purpose: TradePurpose

    as_maker: bool

    created_ts_ms: int
    updated_ts_ms: int

    ordered_shares: float
    off_chain_pending_shares: float
    off_chain_matched_shares: float
    off_chain_invalid_shares: float

    trades: dict[str, ManagedTrade] = field(default_factory=dict)
    should_cancel: bool = False

    replace_count: int = 0
    cancel_attempts: int = 0
    audit_records: list[AuditRecord] = field(default_factory=list)

    @property
    def has_active_shares(self) -> bool:
        return self.off_chain_pending_shares > self.ordered_shares * _PENDING_SHARES_RATE

    @property
    def is_effectively_matched(self) -> bool:
        return self.off_chain_matched_shares > self.ordered_shares * _MATCHED_SHARES_RATE

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

    shares: float
    status: ManagedTradeStatus
    mkt_status: MarketTradeStatus

    created_ts_ms: int
    updated_ts_ms: int

    on_chain_pending_shares: float = 0.0
    on_chain_settled_shares: float = 0.0
    on_chain_failure_shares: float = 0.0


@dataclass(slots=True)
class Position:
    token: Token
    shares: float = 0.0
    updated_ts_ms: int = 0
    source: Literal["local", "exchange"] = "local"
