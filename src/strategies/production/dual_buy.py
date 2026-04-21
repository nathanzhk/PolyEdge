from __future__ import annotations

from dataclasses import dataclass

from events import CurrentPositionEvent, DesiredPositionEvent, MarketQuoteEvent, RuntimeStateEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("DUAL_BUY")


@dataclass(slots=True, frozen=True)
class DualBuyConfig:
    min_entry_s: int = 2
    stop_before_end_s: int = 30
    order_size: float = 5.0
    max_buy_ask: float = 0.49
    max_imbalance: float = 10.0
    max_shares_per_side: float = 80.0
    min_log_interval_s: int = 5


@dataclass(slots=True, frozen=True)
class _SideState:
    token: Token
    quote: MarketQuoteEvent
    position: CurrentPositionEvent | None

    @property
    def effective_shares(self) -> float:
        if self.position is None:
            return 0.0
        return self.position.effective_shares

    @property
    def resting_target_shares(self) -> float:
        if self.position is None:
            return 0.0
        return max(
            round(self.position.holding_shares + self.position.open_settling_shares, 6),
            0.0,
        )

    @property
    def opening_shares(self) -> float:
        if self.position is None:
            return 0.0
        return self.position.opening_shares

    @property
    def holding_cost(self) -> float:
        if self.position is None:
            return 0.0
        return self.position.holding_cost


class DualBuyStrategy:
    """Balanced dual-sided maker buys for BTC 5-minute markets."""

    def __init__(self, config: DualBuyConfig | None = None) -> None:
        self.config = config or DualBuyConfig()
        self._market_id: str | None = None
        self._last_log_bucket: int | None = None

    def evaluate(self, state: RuntimeStateEvent) -> list[DesiredPositionEvent]:
        market = state.market
        if market.id != self._market_id:
            self._market_id = market.id
            self._last_log_bucket = None
            logger.info("new market %s", market.slug)

        ts_ms = now_ts_ms()
        elapsed_s = _elapsed_s(market, ts_ms)
        remaining_s = _remaining_s(market, ts_ms)

        up = _SideState(market.yes_token, state.yes_token_quote, state.yes_token_position)
        down = _SideState(market.no_token, state.no_token_quote, state.no_token_position)

        self._log_round_state(market, up, down, elapsed_s, remaining_s)

        if elapsed_s < self.config.min_entry_s or remaining_s <= self.config.stop_before_end_s:
            return self._cancel_opening_orders(market, up, down)

        targets = [
            self._target_for_side(market, side=up, other=down),
            self._target_for_side(market, side=down, other=up),
        ]
        return [target for target in targets if target is not None]

    def _target_for_side(
        self,
        market: Market,
        *,
        side: _SideState,
        other: _SideState,
    ) -> DesiredPositionEvent | None:
        cfg = self.config

        # Keep an existing maker order alive/repriced, but do not stack another order.
        if side.opening_shares > 0:
            return _desired_position(market, side, side.effective_shares)

        if side.quote.best_ask <= 0 or side.quote.best_ask > cfg.max_buy_ask:
            return None
        if side.effective_shares >= cfg.max_shares_per_side:
            return None
        if side.effective_shares - other.effective_shares >= cfg.max_imbalance:
            return None

        next_target = min(
            side.effective_shares + cfg.order_size,
            cfg.max_shares_per_side,
        )
        logger.info(
            "buy target %s %.2f -> %.2f ask=%.2f other=%.2f imbalance=%.2f",
            side.token.key,
            side.effective_shares,
            next_target,
            side.quote.best_ask,
            other.effective_shares,
            side.effective_shares - other.effective_shares,
        )
        return _desired_position(market, side, next_target)

    def _cancel_opening_orders(
        self,
        market: Market,
        up: _SideState,
        down: _SideState,
    ) -> list[DesiredPositionEvent]:
        targets = []
        for side in (up, down):
            if side.opening_shares > 0:
                logger.info(
                    "cancel opening %s opening=%.2f keep=%.2f",
                    side.token.key,
                    side.opening_shares,
                    side.resting_target_shares,
                )
                targets.append(_desired_position(market, side, side.resting_target_shares))
        return targets

    def _log_round_state(
        self,
        market: Market,
        up: _SideState,
        down: _SideState,
        elapsed_s: int,
        remaining_s: float,
    ) -> None:
        bucket = elapsed_s // self.config.min_log_interval_s
        if bucket == self._last_log_bucket:
            return
        self._last_log_bucket = bucket

        total_cost = up.holding_cost + down.holding_cost
        up_pnl = up.effective_shares - total_cost
        down_pnl = down.effective_shares - total_cost
        worst_pnl = min(up_pnl, down_pnl)
        imbalance = up.effective_shares - down.effective_shares
        logger.info(
            "[%s %03ds/%03ds] UP %.2f cost %.2f | DOWN %.2f cost %.2f | "
            "cost %.2f pnl_if_up %.2f pnl_if_down %.2f worst %.2f imbalance %.2f",
            market.slug,
            elapsed_s,
            int(remaining_s),
            up.effective_shares,
            up.holding_cost,
            down.effective_shares,
            down.holding_cost,
            total_cost,
            up_pnl,
            down_pnl,
            worst_pnl,
            imbalance,
        )


def _desired_position(
    market: Market,
    side: _SideState,
    shares: float,
) -> DesiredPositionEvent:
    return DesiredPositionEvent(
        market=market,
        token=side.token,
        shares=round(shares, 6),
        best_bid=side.quote.best_bid,
        best_ask=side.quote.best_ask,
    )


def _elapsed_s(market: Market, ts_ms: int) -> int:
    return int((ts_ms - market.start_ts_ms) // 1000)


def _remaining_s(market: Market, ts_ms: int) -> float:
    return (market.end_ts_ms - ts_ms) / 1000
