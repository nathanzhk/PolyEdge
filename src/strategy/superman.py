from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from events import CurrentPositionEvent, DesiredPositionEvent, MarketQuoteEvent, RuntimeStateEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("SUPERMAN")


class _PositionStatus(IntEnum):
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

    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:

        yes_quote = state.yes_token_quote
        no_quote = state.no_token_quote
        market = state.market

        btc_base = state.beat_price
        btc_curr = state.crypto_quote.mid
        btc_diff = btc_curr - btc_base

        ts_ms = now_ts_ms()
        elapsed_s = _elapsed_s(market, ts_ms)

        active_position = self._get_active_position(list(state.positions))
        position_status = self._get_position_status(active_position, elapsed_s)

        if position_status == _PositionStatus.OBSERVE:
            return self._observe(
                market,
                yes_quote,
                no_quote,
                btc_diff,
                elapsed_s,
            )
        if active_position is not None:
            if position_status == _PositionStatus.OPENING:
                return self._opening(
                    market,
                    yes_quote,
                    no_quote,
                    btc_diff,
                    active_position,
                    elapsed_s,
                )
            elif position_status == _PositionStatus.HOLDING:
                return self._holding(
                    market,
                    yes_quote,
                    no_quote,
                    active_position,
                    elapsed_s,
                )
            elif position_status == _PositionStatus.CLOSING:
                return self._closing(
                    market,
                    yes_quote,
                    no_quote,
                    active_position,
                    elapsed_s,
                )

    def _observe(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        btc_diff: float,
        elapsed_s: float,
    ) -> DesiredPositionEvent | None:
        if elapsed_s < self.config.observe_min_sec:
            logger.info("elapsed_s < %ds", self.config.observe_min_sec)
            return None
        if elapsed_s > self.config.observe_max_sec:
            logger.info("elapsed_s > %ds", self.config.observe_max_sec)
            return None

        token = self._check_signal(market, btc_diff, yes_quote)
        if token is None:
            return None

        logger.info("signal: %s", token.key)
        return DesiredPositionEvent(
            market=market,
            token=token,
            shares=self.config.entry_shares,
            price=_bid_for_token(yes_quote, no_quote, token),
        )

    def _opening(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        btc_diff: float,
        position: CurrentPositionEvent,
        elapsed_s: float,
    ):
        if (
            position.holding_open_ts_ms is not None
            and now_ts_ms() - position.holding_open_ts_ms <= 3_000
        ) or elapsed_s <= self.config.observe_max_sec:
            token = self._check_signal(market, btc_diff, yes_quote)
            if token is not None:
                return DesiredPositionEvent(
                    market=market,
                    token=token,
                    shares=self.config.entry_shares,
                    price=_bid_for_token(yes_quote, no_quote, token),
                )
            else:
                return DesiredPositionEvent(
                    market=market,
                    token=position.token,
                    shares=position.opening_shares + position.holding_shares,
                    price=position.holding_avg_price
                    or _bid_for_token(yes_quote, no_quote, position.token),
                )
        else:
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(yes_quote, no_quote, position.token),
            )

    def _holding(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        position: CurrentPositionEvent,
        elapsed_s: float,
    ):
        if elapsed_s > self.config.maker_exit_sec:
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(yes_quote, no_quote, position.token),
            )

    def _closing(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        position: CurrentPositionEvent,
        elapsed_s: float,
    ):
        if elapsed_s < self.config.taker_exit_sec:
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(yes_quote, no_quote, position.token),
            )
        else:
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                price=_ask_for_token(yes_quote, no_quote, position.token),
                force=True,
            )

    def _check_signal(
        self,
        market: Market,
        btc_diff: float,
        yes_quote: MarketQuoteEvent,
    ) -> Token | None:
        abs_diff = abs(btc_diff)
        if abs_diff < self.config.min_btc_move:
            logger.debug("skip: btc move %.2f < %.2f", abs_diff, self.config.min_btc_move)
            return None
        if abs_diff > self.config.max_btc_move:
            logger.debug("skip: btc move %.2f > %.2f", abs_diff, self.config.max_btc_move)
            return None

        mid_up = yes_quote.mid
        if mid_up < self.config.neutral_lo or mid_up > self.config.neutral_hi:
            logger.debug(
                "skip: mid %.2f outside [%.2f, %.2f]",
                mid_up,
                self.config.neutral_lo,
                self.config.neutral_hi,
            )
            return None

        return market.yes_token if btc_diff > 0 else market.no_token

    def _get_position_status(
        self,
        position: CurrentPositionEvent | None,
        elapsed_s: float,
    ) -> _PositionStatus:
        if position is None:
            if elapsed_s < self.config.observe_max_sec:
                return _PositionStatus.OBSERVE
            else:
                return _PositionStatus.EXISTED
        else:
            if position.opening_shares > 0:
                return _PositionStatus.OPENING
            elif position.closing_shares > 0:
                return _PositionStatus.CLOSING
            else:
                return _PositionStatus.HOLDING

    def _get_active_position(
        self,
        positions: list[CurrentPositionEvent] | None,
    ) -> CurrentPositionEvent | None:
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


def _elapsed_s(market: Market, ts_ms: int) -> int:
    return (ts_ms - market.start_ts_ms) // 1000


def _bid_for_token(
    yes_quote: MarketQuoteEvent,
    no_quote: MarketQuoteEvent,
    token: Token,
) -> float:
    return yes_quote.best_bid if token.id == yes_quote.token.id else no_quote.best_bid


def _ask_for_token(
    yes_quote: MarketQuoteEvent,
    no_quote: MarketQuoteEvent,
    token: Token,
) -> float:
    return yes_quote.best_ask if token.id == yes_quote.token.id else no_quote.best_ask
