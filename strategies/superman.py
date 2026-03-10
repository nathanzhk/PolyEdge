from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from markets.base import Market, Token
from markets.btc import DOWN_OUTCOME, UP_OUTCOME
from strategies.context import Position, PositionLatestState, StrategyContext
from strategies.target import ExecutionStyle, PositionTarget
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger
from utils.time import fmt_ts_ms

logger = get_logger("SUPERMAN")

SignalSide = Literal["up", "down"]
LifecycleState = Literal["observe", "entry_ordered", "hold", "exit_ordered", "done"]


@dataclass(slots=True, frozen=True)
class S2_90sConfig:
    observe_min_sec: float = 5.0
    observe_max_sec: float = 120.0
    maker_exit_sec: float = 90.0
    taker_exit_sec: float = 120.0

    min_btc_move: float = 15.0
    max_btc_move: float = 1000.0

    min_bid: float = 0.01
    max_ask: float = 0.65
    neutral_lo: float = 0.40
    neutral_hi: float = 0.60

    tp_price: float = 0.90
    entry_shares: float = 5.02
    min_maker_shares: float = 5.0
    matched_share_gap: float = 0.1


class SupermanStrategy:
    """S2_90s decision logic derived only from StrategyContext."""

    def __init__(self, config: S2_90sConfig | None = None) -> None:
        self.config = config or S2_90sConfig()

    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        market = context.market.market
        market_price = context.market.market_price
        crypto_price = context.market.crypto_price

        if market is None or market_price is None:
            return None

        ts_ms = max(
            market_price.ts_ms,
            crypto_price.ts_ms if crypto_price is not None else market_price.ts_ms,
        )
        btc_latest = crypto_price.price if crypto_price is not None else None
        btc_base = context.market.beat_price
        elapsed_s = _elapsed(market, ts_ms)
        position = _active_position(market, context.position, self.config.matched_share_gap)
        lifecycle = self._infer_lifecycle(position, elapsed_s)

        self._log_metrics(btc_base, market_price, btc_latest)

        if lifecycle == "observe":
            return self._observe_target(
                market,
                market_price,
                elapsed_s,
                ts_ms,
                btc_base,
                btc_latest,
            )
        if lifecycle == "entry_ordered":
            return self._entry_target(
                market,
                market_price,
                position,
                elapsed_s,
                ts_ms,
                btc_base,
                btc_latest,
            )
        if lifecycle == "hold":
            return self._hold_target(market, market_price, position, ts_ms)
        if lifecycle == "exit_ordered":
            return self._exit_target(market, market_price, position, ts_ms)
        return None

    def _infer_lifecycle(
        self,
        position: Position | None,
        elapsed_s: float,
    ) -> LifecycleState:
        if position is not None:
            if position.closing_shares > self.config.matched_share_gap:
                return "exit_ordered"
            if position.opening_shares > self.config.matched_share_gap:
                return "entry_ordered"
            if position.holding_shares > self.config.matched_share_gap:
                return "hold"
        if elapsed_s <= self.config.observe_max_sec:
            return "observe"
        return "done"

    def _observe_target(
        self,
        market: Market,
        market_price: MarketPriceEvent,
        elapsed_s: float,
        ts_ms: int,
        btc_base: float | None,
        btc_latest: float | None,
    ) -> PositionTarget | None:
        if elapsed_s < self.config.observe_min_sec:
            return None
        if elapsed_s > self.config.observe_max_sec:
            logger.info("SIGNAL: N/A")
            return None

        side = self._check_signal(btc_base, btc_latest, market_price)
        if side is None:
            return None

        price = _bid_for_side(market_price, side)
        logger.info("SIGNAL: %s", side.upper())
        return _long_position_target(
            market=market,
            token=_token_for_side(market, side),
            side=side,
            shares=self.config.entry_shares,
            price=price,
        )

    def _entry_target(
        self,
        market: Market,
        market_price: MarketPriceEvent,
        position: Position | None,
        elapsed_s: float,
        ts_ms: int,
        btc_base: float | None,
        btc_latest: float | None,
    ) -> PositionTarget | None:
        if position is None:
            return None

        side = _side_for_token(market, position.token)
        holding_shares = position.holding_shares
        exposure_shares = _exposure_shares(position)

        if holding_shares >= self.config.entry_shares - self.config.matched_share_gap:
            logger.info("MAKER BUY: ORDER FILLED")
            return _hold_position_target(
                market=market,
                token=position.token,
                side=side,
                shares=exposure_shares,
                price=_bid_for_side(market_price, side),
            )

        can_keep_entry_alive = elapsed_s <= self.config.observe_max_sec
        if not can_keep_entry_alive:
            if holding_shares > self.config.matched_share_gap:
                logger.info("WINDOW EXPIRED: PARTIAL FILL -> HOLD")
                return _hold_position_target(
                    market=market,
                    token=position.token,
                    side=side,
                    shares=exposure_shares,
                    price=_bid_for_side(market_price, side),
                )

            logger.info("WINDOW EXPIRED: CANCEL ENTRY ORDER")
            return _flat_position_target(
                market=market,
                token=position.token,
                side=side,
                price=_ask_for_side(market_price, side),
                execution_style=ExecutionStyle.PASSIVE,
            )

        new_side = self._check_signal(btc_base, btc_latest, market_price)
        if new_side is None:
            if holding_shares > self.config.matched_share_gap:
                logger.info("SIGNAL LOST: PARTIAL FILL -> HOLD")
                return _hold_position_target(
                    market=market,
                    token=position.token,
                    side=side,
                    shares=exposure_shares,
                    price=_bid_for_side(market_price, side),
                )

            logger.info("SIGNAL LOST: CANCEL ENTRY ORDER")
            return _flat_position_target(
                market=market,
                token=position.token,
                side=side,
                price=_ask_for_side(market_price, side),
                execution_style=ExecutionStyle.PASSIVE,
            )

        price = _bid_for_side(market_price, new_side)
        token = _token_for_side(market, new_side)
        return _long_position_target(
            market=market,
            token=token,
            side=new_side,
            shares=self.config.entry_shares,
            price=price,
        )

    def _hold_target(
        self,
        market: Market,
        market_price: MarketPriceEvent,
        position: Position | None,
        ts_ms: int,
    ) -> PositionTarget | None:
        if position is None:
            return None

        side = _side_for_token(market, position.token)
        holding_shares = position.holding_shares
        exposure_shares = _exposure_shares(position)
        curr_price = _ask_for_side(market_price, side)
        entry_price = position.holding_avg_price or _bid_for_side(market_price, side)
        pnl = _calc_unrealized_pnl(holding_shares, curr_price, entry_price)
        logger.info("HOLDING: %.6f SHARES (PNL=%+.2f)", holding_shares, pnl)

        if position.holding_open_ts_ms is not None:
            logger.info(
                "EXIT AT %s",
                fmt_ts_ms(
                    position.holding_open_ts_ms + int(self.config.maker_exit_sec * 1000),
                    fmt="datetime",
                ),
            )

        if curr_price >= self.config.tp_price:
            logger.info(
                "HOLDING: TAKE PROFIT @ ask=%.2f >= %.2f",
                curr_price,
                self.config.tp_price,
            )
            return _flat_position_target(
                market=market,
                token=position.token,
                side=side,
                price=curr_price,
                execution_style=self._exit_execution_style(holding_shares, exposure_shares),
            )

        if _elapsed_since_open(position, ts_ms) >= self.config.maker_exit_sec:
            logger.info("HOLDING: EXIT TIME")
            return _flat_position_target(
                market=market,
                token=position.token,
                side=side,
                price=curr_price,
                execution_style=self._exit_execution_style(holding_shares, exposure_shares),
            )

        return _hold_position_target(
            market=market,
            token=position.token,
            side=side,
            shares=exposure_shares,
            price=_bid_for_side(market_price, side),
        )

    def _exit_target(
        self,
        market: Market,
        market_price: MarketPriceEvent,
        position: Position | None,
        ts_ms: int,
    ) -> PositionTarget | None:
        if position is None:
            return None

        side = _side_for_token(market, position.token)
        holding_shares = position.holding_shares
        exposure_shares = _exposure_shares(position)
        force_exit = (
            _elapsed_since_open(position, ts_ms) >= self.config.taker_exit_sec
            or max(holding_shares, exposure_shares) < self.config.min_maker_shares
        )
        if force_exit:
            logger.warning("FORCE TAKER EXIT")
        execution_style = ExecutionStyle.URGENT if force_exit else ExecutionStyle.PASSIVE
        price = (
            _bid_for_side(market_price, side)
            if execution_style == ExecutionStyle.URGENT
            else _ask_for_side(market_price, side)
        )
        return _flat_position_target(
            market=market,
            token=position.token,
            side=side,
            price=price,
            execution_style=execution_style,
        )

    def _check_signal(
        self,
        btc_base: float | None,
        btc_latest: float | None,
        market_price: MarketPriceEvent,
    ) -> SignalSide | None:
        if btc_base is None or btc_latest is None:
            logger.debug("SKIP: missing btc data")
            return None

        btc_change = btc_latest - btc_base
        abs_change = abs(btc_change)
        mid_up = (market_price.bid_yes + market_price.ask_yes) / 2
        side: SignalSide = "up" if btc_change > 0 else "down"

        if abs_change < self.config.min_btc_move:
            logger.debug("SKIP: BTC MOVE %.2f < %.2f", abs_change, self.config.min_btc_move)
            return None
        if abs_change > self.config.max_btc_move:
            logger.debug("SKIP: BTC MOVE %.2f > %.2f", abs_change, self.config.max_btc_move)
            return None
        if mid_up < self.config.neutral_lo or mid_up > self.config.neutral_hi:
            logger.debug(
                "SKIP: MID %.2f OUTSIDE [%.2f, %.2f]",
                mid_up,
                self.config.neutral_lo,
                self.config.neutral_hi,
            )
            return None
        if (
            _bid_for_side(market_price, side) <= self.config.min_bid
            or _ask_for_side(market_price, side) > self.config.max_ask
        ):
            logger.debug("SKIP: bid/ask out of range")
            return None
        return side

    def _log_metrics(
        self,
        btc_base: float | None,
        market_price: MarketPriceEvent,
        btc_latest: float | None,
    ) -> None:
        if btc_latest is None:
            return

        if btc_base is None:
            baseline = "BASE=N/A"
        else:
            diff = btc_latest - btc_base
            pct = diff / btc_base * 100
            baseline = f"BASE={btc_base:.2f}({diff:+.2f}/{pct:+.3f}%)"

        logger.info("BTC %.2f [%s]", btc_latest, baseline)
        logger.info(
            "mid=%.2f bid_up=%.2f ask_up=%.2f bid_down=%.2f ask_down=%.2f",
            (market_price.bid_yes + market_price.ask_yes) / 2,
            market_price.bid_yes,
            market_price.ask_yes,
            market_price.bid_no,
            market_price.ask_no,
        )

    def _exit_execution_style(
        self,
        holding_shares: float,
        exposure_shares: float,
    ) -> ExecutionStyle:
        if max(holding_shares, exposure_shares) >= self.config.min_maker_shares:
            return ExecutionStyle.PASSIVE
        return ExecutionStyle.URGENT


def _active_position(
    market: Market,
    positions: PositionLatestState,
    matched_share_gap: float,
) -> Position | None:
    for token in (market.yes_token, market.no_token):
        position = positions.get_position(token)
        if position is not None and (
            position.opening_shares > matched_share_gap
            or position.holding_shares > matched_share_gap
            or position.closing_shares > matched_share_gap
        ):
            return position
    return None


def _exposure_shares(position: Position | None) -> float:
    if position is None:
        return 0.0
    return round(position.holding_shares + position.opening_shares - position.closing_shares, 6)


def _calc_unrealized_pnl(shares: float, sell_price: float, entry_price: float) -> float:
    return shares * (sell_price - entry_price)


def _elapsed(market: Market, ts_ms: int) -> float:
    return (ts_ms - market.start_ts_ms) / 1000


def _elapsed_since_open(position: Position, ts_ms: int) -> float:
    open_ts_ms = position.holding_open_ts_ms
    return (ts_ms - open_ts_ms) / 1000 if open_ts_ms else 0.0


def _token_for_side(market: Market, side: SignalSide) -> Token:
    if side == UP_OUTCOME.lower():
        return market.yes_token
    if side == DOWN_OUTCOME.lower():
        return market.no_token
    raise ValueError(f"invalid signal side: {side}")


def _side_for_token(market: Market, token: Token) -> SignalSide:
    if token.id == market.yes_token.id:
        return "up"
    if token.id == market.no_token.id:
        return "down"
    raise ValueError(f"token {token.id} is not in market {market.id}")


def _bid_for_side(market_price: MarketPriceEvent, side: SignalSide) -> float:
    return market_price.bid_yes if side == UP_OUTCOME.lower() else market_price.bid_no


def _ask_for_side(market_price: MarketPriceEvent, side: SignalSide) -> float:
    return market_price.ask_yes if side == UP_OUTCOME.lower() else market_price.ask_no


def _long_position_target(
    *,
    market: Market,
    token: Token,
    side: SignalSide,
    shares: float,
    price: float,
) -> PositionTarget:
    logger.info("TARGET LONG: %.6f %s @ %.2f", shares, side.upper(), price)
    return PositionTarget(
        market=market,
        token=token,
        shares=shares,
        price=price,
        style=ExecutionStyle.PASSIVE,
    )


def _hold_position_target(
    *,
    market: Market,
    token: Token,
    side: SignalSide,
    shares: float,
    price: float,
) -> PositionTarget:
    logger.info("TARGET HOLD: %.6f %s @ %.2f", shares, side.upper(), price)
    return PositionTarget(
        market=market,
        token=token,
        shares=max(0.0, shares),
        price=price,
        style=ExecutionStyle.PASSIVE,
    )


def _flat_position_target(
    *,
    market: Market,
    token: Token,
    side: SignalSide,
    price: float,
    execution_style: ExecutionStyle,
) -> PositionTarget:
    logger.info(
        "TARGET FLAT: %s SELL %s @ %.2f",
        execution_style.value.upper(),
        side.upper(),
        price,
    )
    return PositionTarget(
        market=market,
        token=token,
        shares=0.0,
        price=price,
        style=execution_style,
    )
