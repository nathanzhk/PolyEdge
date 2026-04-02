from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from events import CurrentPositionEvent, DesiredPositionEvent, MarketQuoteEvent, RuntimeStateEvent
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("FLASH")


class _Momentum(IntEnum):
    DOWN = -1
    FLAT = 0
    UP = 1


@dataclass(slots=True, frozen=True)
class _Config:
    lookback_s: int = 3
    threshold: float = 3.0  # $/s velocity threshold
    entry_shares: float = 5.0
    min_entry_bid: float = 0.35  # don't buy below this price
    max_entry_ask: float = 0.65  # don't buy above this price
    min_entry_s: int = 5  # earliest entry (seconds into window)
    max_entry_s: int = 240  # latest entry — leave room for exit
    force_exit_s: int = 270  # force close before window ends


class FlashStrategy:
    def __init__(self) -> None:
        self.config = _Config()
        self._market_id: str = ""
        self._btc_by_s: dict[int, float] = {}

    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        yes_quote = state.yes_token_quote
        no_quote = state.no_token_quote
        market = state.market

        # Reset on new window
        if market.id != self._market_id:
            self._market_id = market.id
            self._btc_by_s.clear()

        ts_ms = now_ts_ms()
        elapsed_s = _elapsed_s(market, ts_ms)

        # Record BTC price (one per second)
        self._btc_by_s[elapsed_s] = state.crypto_quote.mid

        position = _get_active_position(state.positions)

        if position is not None and elapsed_s >= self.config.force_exit_s:
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                best_bid=_bid_for_token(yes_quote, no_quote, position.token),
                best_ask=_ask_for_token(yes_quote, no_quote, position.token),
                force=True,
            )

        if elapsed_s < self.config.min_entry_s:
            return None

        momentum = self._get_momentum(elapsed_s)

        if position is None:
            if elapsed_s > self.config.max_entry_s:
                return None
            return self._no_position(market, yes_quote, no_quote, momentum)

        if position.opening_shares > 0:
            return self._opening(market, yes_quote, no_quote, position, momentum)

        if position.closing_shares > 0:
            return self._closing(market, yes_quote, no_quote, position)

        return self._holding(market, yes_quote, no_quote, position, momentum)

    # ------------------------------------------------------------------
    # State handlers
    # ------------------------------------------------------------------

    def _no_position(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        momentum: _Momentum,
    ) -> DesiredPositionEvent | None:
        if momentum == _Momentum.UP:
            if yes_quote.best_ask > self.config.max_entry_ask:
                logger.debug(
                    "skip UP: ask %.2f > %.2f", yes_quote.best_ask, self.config.max_entry_ask
                )
                return None
            if yes_quote.best_bid < self.config.min_entry_bid:
                logger.debug(
                    "skip UP: bid %.2f < %.2f", yes_quote.best_bid, self.config.min_entry_bid
                )
                return None
            logger.info("enter UP")
            return DesiredPositionEvent(
                market=market,
                token=market.yes_token,
                shares=self.config.entry_shares,
                best_bid=yes_quote.best_bid,
                best_ask=yes_quote.best_ask,
                force=True,
            )
        if momentum == _Momentum.DOWN:
            if no_quote.best_ask > self.config.max_entry_ask:
                logger.debug(
                    "skip DOWN: ask %.2f > %.2f", no_quote.best_ask, self.config.max_entry_ask
                )
                return None
            if no_quote.best_bid < self.config.min_entry_bid:
                logger.debug(
                    "skip DOWN: bid %.2f < %.2f", no_quote.best_bid, self.config.min_entry_bid
                )
                return None
            logger.info("enter DOWN")
            return DesiredPositionEvent(
                market=market,
                token=market.no_token,
                shares=self.config.entry_shares,
                best_bid=no_quote.best_bid,
                best_ask=no_quote.best_ask,
                force=True,
            )
        return None

    def _opening(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        position: CurrentPositionEvent,
        momentum: _Momentum,
    ) -> DesiredPositionEvent | None:
        expected = _Momentum.UP if position.token.key == "up" else _Momentum.DOWN
        if momentum == expected:
            # Momentum still valid — keep entry, update price
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=self.config.entry_shares,
                best_bid=_bid_for_token(yes_quote, no_quote, position.token),
                best_ask=_ask_for_token(yes_quote, no_quote, position.token),
                force=True,
            )
        # Momentum gone or reversed — cancel entry
        logger.info("cancel entry %s, momentum=%s", position.token.key, momentum.name)
        return DesiredPositionEvent(
            market=market,
            token=position.token,
            shares=0.0,
            best_bid=_bid_for_token(yes_quote, no_quote, position.token),
            best_ask=_ask_for_token(yes_quote, no_quote, position.token),
        )

    def _closing(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        position: CurrentPositionEvent,
    ) -> DesiredPositionEvent | None:
        # Commit to exit — update price (maker)
        return DesiredPositionEvent(
            market=market,
            token=position.token,
            shares=0.0,
            best_bid=_bid_for_token(yes_quote, no_quote, position.token),
            best_ask=_ask_for_token(yes_quote, no_quote, position.token),
        )

    def _holding(
        self,
        market: Market,
        yes_quote: MarketQuoteEvent,
        no_quote: MarketQuoteEvent,
        position: CurrentPositionEvent,
        momentum: _Momentum,
    ) -> DesiredPositionEvent | None:
        expected = _Momentum.UP if position.token.key == "up" else _Momentum.DOWN

        if momentum == expected:
            # Momentum matches — hold
            return None

        if momentum == _Momentum.FLAT:
            # Momentum gone — maker exit (lock profit)
            logger.info("flat exit %s", position.token.key)
            return DesiredPositionEvent(
                market=market,
                token=position.token,
                shares=0.0,
                best_bid=_bid_for_token(yes_quote, no_quote, position.token),
                best_ask=_ask_for_token(yes_quote, no_quote, position.token),
            )

        # Momentum reversed — taker exit (stop loss)
        logger.info("reversal exit %s", position.token.key)
        return DesiredPositionEvent(
            market=market,
            token=position.token,
            shares=0.0,
            best_bid=_bid_for_token(yes_quote, no_quote, position.token),
            best_ask=_ask_for_token(yes_quote, no_quote, position.token),
            force=True,
        )

    # ------------------------------------------------------------------
    # Momentum
    # ------------------------------------------------------------------

    def _get_momentum(self, elapsed_s: int) -> _Momentum:
        prev_s = elapsed_s - self.config.lookback_s
        prev_price = self._btc_by_s.get(prev_s)
        curr_price = self._btc_by_s.get(elapsed_s)
        if prev_price is None or curr_price is None:
            return _Momentum.FLAT
        velocity = (curr_price - prev_price) / self.config.lookback_s
        if velocity > self.config.threshold:
            return _Momentum.UP
        if velocity < -self.config.threshold:
            return _Momentum.DOWN
        return _Momentum.FLAT


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _elapsed_s(market: Market, ts_ms: int) -> int:
    return (ts_ms - market.start_ts_ms) // 1000


def _get_active_position(
    positions: tuple[CurrentPositionEvent, ...],
) -> CurrentPositionEvent | None:
    for position in positions:
        if (
            position.opening_shares > 0
            or position.holding_shares > 0
            or position.closing_shares > 0
        ):
            return position
    return None


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
