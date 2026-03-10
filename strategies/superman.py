from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from markets.base import Market, Token
from strategies.context import Position, StrategyContext
from strategies.target import ExecutionStyle, PositionTarget
from streams.market_price_event import MarketPriceEvent
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("SUPERMAN")


class PositionStatus(IntEnum):
    OBSERVE = 0
    OPENING = 1
    HOLDING = 2
    CLOSING = 3
    EXISTED = 4


@dataclass(slots=True, frozen=True)
class _Config:
    observe_min_sec: float = 5.0
    observe_max_sec: float = 120.0
    maker_exit_sec: float = 90.0
    taker_exit_sec: float = 120.0
    min_btc_move: float = 15.0
    max_btc_move: float = 1000.0
    neutral_lo: float = 0.40
    neutral_hi: float = 0.60
    entry_shares: float = 5.02


class SupermanStrategy:
    def __init__(self) -> None:
        self.config = _Config()

    def on_market(self, context: StrategyContext) -> PositionTarget | None:
        market_status = context.market

        market = market_status.market
        if market is None:
            return None

        market_price = market_status.market_price
        crypto_price = market_status.crypto_price
        if market_price is None or crypto_price is None:
            return None

        beat_btc = market_status.beat_price
        curr_btc = crypto_price.price

        if beat_btc is None or curr_btc is None:
            return None

        ts_ms = now_ts_ms()
        elapsed_s = _elapsed(market, ts_ms)

        active_position = self._get_active_position(context.position.positions)
        position_status = self._get_position_status(active_position, elapsed_s)

        if position_status == PositionStatus.OBSERVE:
            return self._observe(
                market,
                market_price,
                elapsed_s,
                beat_btc,
                curr_btc,
            )
        if active_position is not None:
            if position_status == PositionStatus.OPENING:
                return self._opening(
                    market, market_price, active_position, elapsed_s, beat_btc, curr_btc
                )
            elif position_status == PositionStatus.HOLDING:
                return self._holding(
                    market, market_price, active_position, elapsed_s, beat_btc, curr_btc
                )
            elif position_status == PositionStatus.CLOSING:
                return self._closing(
                    market, market_price, active_position, elapsed_s, beat_btc, curr_btc
                )
            else:
                return None

    def _observe(
        self,
        market: Market,
        mkt_price: MarketPriceEvent,
        elapsed_s: float,
        beat_btc: float,
        curr_btc: float,
    ) -> PositionTarget | None:
        if elapsed_s < self.config.observe_min_sec:
            return None
        if elapsed_s > self.config.observe_max_sec:
            logger.info("no signal")
            return None

        token = self._check_signal(market, beat_btc, curr_btc, mkt_price)
        if token is None:
            return None

        logger.info("signal: %s", token.key)
        return PositionTarget(
            market=market,
            token=token,
            shares=self.config.entry_shares,
            price=_bid_for_token(mkt_price, token),
            style=ExecutionStyle.PASSIVE,
        )

    def _opening(
        self,
        market: Market,
        mkt_price: MarketPriceEvent,
        position: Position,
        elapsed_s: float,
        beat_btc: float,
        curr_btc: float,
    ):
        if (
            position.holding_open_ts_ms is not None
            and now_ts_ms() - position.holding_open_ts_ms <= 3_000
        ) or elapsed_s <= self.config.observe_max_sec:
            token = self._check_signal(market, beat_btc, curr_btc, mkt_price)
            if token is not None:
                return PositionTarget(
                    market=market,
                    token=token,
                    shares=self.config.entry_shares,
                    price=_bid_for_token(mkt_price, token),
                    style=ExecutionStyle.PASSIVE,
                )
            else:
                return PositionTarget(
                    market=market,
                    token=position.token,
                    shares=0.0,
                    price=0.0,
                    style=ExecutionStyle.PASSIVE,
                )
        else:
            return PositionTarget(
                market=market,
                token=position.token,
                shares=0.0,
                price=0.0,
                style=ExecutionStyle.PASSIVE,
            )

    def _holding(
        self,
        market: Market,
        mkt_price: MarketPriceEvent,
        position: Position,
        elapsed_s: float,
        beat_btc: float,
        curr_btc: float,
    ):
        if elapsed_s > self.config.maker_exit_sec:
            return PositionTarget(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(mkt_price, position.token),
                style=ExecutionStyle.PASSIVE,
            )

    def _closing(
        self,
        market: Market,
        mkt_price: MarketPriceEvent,
        position: Position,
        elapsed_s: float,
        beat_btc: float,
        curr_btc: float,
    ):
        if elapsed_s < self.config.taker_exit_sec:
            return PositionTarget(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(mkt_price, position.token),
                style=ExecutionStyle.PASSIVE,
            )
        else:
            return PositionTarget(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(mkt_price, position.token),
                style=ExecutionStyle.URGENT,
            )

    def _check_signal(
        self, market: Market, btc_base: float, btc_curr: float, mkt_price: MarketPriceEvent
    ) -> Token | None:
        btc_diff = btc_curr - btc_base
        abs_diff = abs(btc_diff)
        mid_up = (mkt_price.bid_yes + mkt_price.ask_yes) / 2

        if abs_diff < self.config.min_btc_move:
            logger.debug("skip: btc move %.2f < %.2f", abs_diff, self.config.min_btc_move)
            return None
        if abs_diff > self.config.max_btc_move:
            logger.debug("skip: btc move %.2f > %.2f", abs_diff, self.config.max_btc_move)
            return None
        if mid_up < self.config.neutral_lo or mid_up > self.config.neutral_hi:
            logger.debug(
                "skip: mid %.2f outside [%.2f, %.2f]",
                mid_up,
                self.config.neutral_lo,
                self.config.neutral_hi,
            )
            return None

        return market.yes_token if btc_diff > 0 else market.no_token

    def _get_position_status(self, position: Position | None, elapsed_s: float) -> PositionStatus:
        if position is None:
            if elapsed_s < self.config.observe_max_sec:
                return PositionStatus.OBSERVE
            else:
                return PositionStatus.EXISTED
        else:
            if position.opening_shares > 0:
                return PositionStatus.OPENING
            elif position.closing_shares > 0:
                return PositionStatus.CLOSING
            else:
                return PositionStatus.HOLDING

    def _get_active_position(self, positions: list[Position] | None) -> Position | None:
        if positions is None or len(positions) == 0:
            return None
        for position in positions:
            if (
                position.opening_shares > 0
                or position.holding_shares > 0
                or position.closing_shares > 0
            ):
                return position
        return None


def _elapsed(market: Market, ts_ms: int) -> float:
    return (ts_ms - market.start_ts_ms) / 1000


def _bid_for_token(market_price: MarketPriceEvent, token: Token) -> float:
    return market_price.bid_yes if token.key == "up" else market_price.bid_no


def _ask_for_token(market_price: MarketPriceEvent, token: Token) -> float:
    return market_price.ask_yes if token.key == "up" else market_price.ask_no
