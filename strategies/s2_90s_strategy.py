from __future__ import annotations

from models.btc_market import DOWN_OUTCOME, UP_OUTCOME
from models.market import Token
from strategies.context import StrategyContext
from streams.market_price_event import MarketPriceEvent
from trade.models import PositionTarget, StrategyDecision, TradeLatestState
from utils.logger import get_logger

logger = get_logger("S2_90S")

OBSERVE_MIN_SEC = 10
OBSERVE_MAX_SEC = 30
MAKER_EXIT_SEC = 90
TAKER_EXIT_SEC = 120

MIN_BTC_MOVE = 10.0
MAX_BTC_MOVE = 1000.0
BINANCE_BASELINE_SEC = 3

MIN_BID = 0.01
MAX_ASK = 0.65
NEUTRAL_LO = 0.40
NEUTRAL_HI = 0.60

TP_PRICE = 0.85
ENTRY_SHARES = 5.02
MIN_MAKER_SHARES = 5.0
MATCHED_SHARE_GAP = 0.1
REPLACE_PRICE_GAP = 0.05
ORDER_TTL_S = 2.0


class S2_90sStrategy:
    def __init__(self) -> None:
        self._window_id: str | None = None
        self._window_start_ms = 0
        self._window_end_ms = 0
        self._btc_tick_buffer: list[tuple[int, float]] = []
        self._last_btc_ts_ms: int | None = None
        self._reset_window_state()

    def on_market(self, context: StrategyContext) -> StrategyDecision:
        market_price = context.market.market_price
        crypto_price = context.market.crypto_price
        trade = context.trade
        if crypto_price is not None:
            self._on_btc(crypto_price.ts_ms, crypto_price.price)

        if market_price is None:
            return StrategyDecision(window_id=self._window_id)

        if market_price.market.id != self._window_id:
            self._on_new_window(market_price)
            return StrategyDecision(window_id=self._window_id)

        ts_ms = max(
            market_price.ts_ms,
            crypto_price.ts_ms if crypto_price is not None else market_price.ts_ms,
        )
        self._log_metrics(ts_ms, market_price)
        elapsed_s = self._elapsed(ts_ms)

        if self.state == "observe":
            return self._observe_decision(market_price, elapsed_s, ts_ms)

        if self.state == "entry_ordered":
            return self._entry_decision(market_price, trade, elapsed_s, ts_ms)

        if self.state == "hold":
            return self._hold_decision(market_price, trade, ts_ms)

        if self.state == "exit_ordered":
            return self._exit_decision(market_price, trade, ts_ms)

        return StrategyDecision(window_id=self._window_id)

    def _reset_window_state(self) -> None:
        self.state = "wait"
        self.btc_binance_base: float | None = None
        self._btc_binance_sum = 0.0
        self._btc_binance_count = 0
        self.btc_latest: float | None = None
        self.signal_side: str | None = None
        self.entry_price = 0.0
        self.entry_ts_ms = 0
        self.entry_token: Token | None = None
        self.exit_price = 0.0

    def _on_btc(self, ts_ms: int, price: float) -> None:
        if self._last_btc_ts_ms == ts_ms:
            return
        self._last_btc_ts_ms = ts_ms
        self._btc_tick_buffer.append((ts_ms, price))
        cutoff = ts_ms - (BINANCE_BASELINE_SEC + 1) * 1000
        while self._btc_tick_buffer and self._btc_tick_buffer[0][0] < cutoff:
            self._btc_tick_buffer.pop(0)

        if self.state == "wait":
            return
        elapsed = self._elapsed(ts_ms)
        if elapsed < 0:
            return
        self.btc_latest = price
        if elapsed <= BINANCE_BASELINE_SEC:
            self._btc_binance_sum += price
            self._btc_binance_count += 1

    def _on_new_window(self, market_price: MarketPriceEvent) -> None:
        market = market_price.market
        self._window_id = market.id
        self._window_start_ms = market.start_ts_ms
        self._window_end_ms = market.end_ts_ms
        self._reset_window_state()

        pre_cutoff = self._window_start_ms - BINANCE_BASELINE_SEC * 1000
        for ts_ms, price in self._btc_tick_buffer:
            if pre_cutoff <= ts_ms < self._window_start_ms:
                self._btc_binance_sum += price
                self._btc_binance_count += 1

        self.state = "observe"
        logger.info("=== BTC 5M %s ===", market.slug)

    def _observe_decision(
        self,
        market_price: MarketPriceEvent,
        elapsed_s: float,
        ts_ms: int,
    ) -> StrategyDecision:
        if elapsed_s < OBSERVE_MIN_SEC:
            return StrategyDecision(window_id=self._window_id)
        if elapsed_s > OBSERVE_MAX_SEC:
            logger.info("SIGNAL: N/A")
            self.state = "done"
            return StrategyDecision(window_id=self._window_id)

        side = self._check_signal(market_price)
        if side is None:
            return StrategyDecision(window_id=self._window_id)

        logger.info("SIGNAL: %s", side.upper())
        self.state = "entry_ordered"
        self.signal_side = side
        self.entry_token = self._token_for_side(market_price, side)
        self.entry_price = self._bid_for_side(market_price, side)
        self.entry_ts_ms = ts_ms
        return self._long_target_decision(market_price, side)

    def _entry_decision(
        self,
        market_price: MarketPriceEvent,
        trade: TradeLatestState | None,
        elapsed_s: float,
        ts_ms: int,
    ) -> StrategyDecision:
        entry_shares = trade.position_shares(self.entry_token) if trade is not None else 0.0
        if entry_shares > ENTRY_SHARES - MATCHED_SHARE_GAP:
            logger.info("MAKER BUY: ORDER FILLED")
            self.state = "hold"
            if self.entry_ts_ms == 0:
                self.entry_ts_ms = ts_ms
            return StrategyDecision(window_id=self._window_id)

        if elapsed_s > OBSERVE_MAX_SEC:
            logger.info("WINDOW EXPIRED: CANCEL ENTRY ORDER")
            self.state = "done"
            return self._flat_target_decision(market_price, post_only=True)

        side = self._check_signal(market_price)
        if side is None:
            logger.info("SIGNAL LOST: CANCEL ENTRY ORDER")
            self.state = "observe"
            return self._flat_target_decision(market_price, post_only=True)

        self.signal_side = side
        self.entry_token = self._token_for_side(market_price, side)
        return self._long_target_decision(market_price, side)

    def _hold_decision(
        self,
        market_price: MarketPriceEvent,
        trade: TradeLatestState | None,
        ts_ms: int,
    ) -> StrategyDecision:
        token = self.entry_token
        shares = trade.position_shares(token) if trade is not None else 0.0
        if token is None or self.signal_side is None or shares < MATCHED_SHARE_GAP:
            self.state = "done"
            return StrategyDecision(window_id=self._window_id)

        curr_price = self._ask_for_side(market_price, self.signal_side)
        logger.info("HOLDING: %.6f SHARES", shares)
        if curr_price >= TP_PRICE:
            logger.info("HOLDING: TAKE PROFIT @ ask=%.2f >= %.2f", curr_price, TP_PRICE)
            self.state = "exit_ordered"
            return self._flat_target_decision(market_price, post_only=True)

        if self._elapsed_since_entry(ts_ms) >= MAKER_EXIT_SEC:
            logger.info("HOLDING: EXIT TIME")
            self.state = "exit_ordered"
            return self._flat_target_decision(market_price, post_only=True)

        if self.signal_side is None:
            return StrategyDecision(window_id=self._window_id)
        return self._long_target_decision(market_price, self.signal_side, shares=shares)

    def _exit_decision(
        self,
        market_price: MarketPriceEvent,
        trade: TradeLatestState | None,
        ts_ms: int,
    ) -> StrategyDecision:
        token = self.entry_token
        shares = trade.position_shares(token) if trade is not None else 0.0
        if token is None or self.signal_side is None or shares < MATCHED_SHARE_GAP:
            logger.info("EXIT: POSITION CLOSED")
            self.state = "done"
            return StrategyDecision(window_id=self._window_id)

        if self._elapsed_since_entry(ts_ms) >= TAKER_EXIT_SEC:
            logger.warning("FORCE TAKER EXIT")
            return self._flat_target_decision(market_price, post_only=False)

        return self._flat_target_decision(market_price, post_only=True)

    def _long_target_decision(
        self,
        market_price: MarketPriceEvent,
        side: str,
        *,
        shares: float = ENTRY_SHARES,
    ) -> StrategyDecision:
        token = self._token_for_side(market_price, side)
        price = self._bid_for_side(market_price, side)
        self.entry_price = price
        self.entry_token = token
        target = PositionTarget(
            target_id=self._target_id(),
            market=market_price.market,
            token=token,
            shares=shares,
            price=price,
            post_only=True,
            ttl_s=ORDER_TTL_S,
            replace_price_gap=REPLACE_PRICE_GAP,
        )
        logger.info("TARGET LONG: %.6f %s @ %.2f", shares, side.upper(), price)
        return StrategyDecision.target(target)

    def _flat_target_decision(
        self,
        market_price: MarketPriceEvent,
        *,
        post_only: bool,
    ) -> StrategyDecision:
        if self.signal_side is None:
            return StrategyDecision(window_id=self._window_id)

        token = self._token_for_side(market_price, self.signal_side)
        price = (
            self._ask_for_side(market_price, self.signal_side)
            if post_only
            else self._bid_for_side(market_price, self.signal_side)
        )
        self.exit_price = price
        role = "MAKER" if post_only else "TAKER"
        target = PositionTarget(
            target_id=self._target_id(),
            market=market_price.market,
            token=token,
            shares=0.0,
            price=price,
            post_only=post_only,
            ttl_s=ORDER_TTL_S,
            replace_price_gap=REPLACE_PRICE_GAP,
            replace_on_ttl=post_only,
        )
        logger.info("TARGET FLAT: %s SELL %s @ %.2f", role, self.signal_side.upper(), price)
        return StrategyDecision.target(target)

    def _check_signal(self, market_price: MarketPriceEvent) -> str | None:
        if (
            self.btc_binance_base is None
            and self._elapsed(market_price.ts_ms) >= BINANCE_BASELINE_SEC
            and self._btc_binance_count > 0
        ):
            self.btc_binance_base = self._btc_binance_sum / self._btc_binance_count

        if self.btc_binance_base is None or self.btc_latest is None:
            logger.debug("SKIP: missing btc data")
            return None

        btc_change = self.btc_latest - self.btc_binance_base
        abs_change = abs(btc_change)
        mid_up = (market_price.bid_yes + market_price.ask_yes) / 2
        side = "up" if btc_change > 0 else "down"

        if abs_change < MIN_BTC_MOVE:
            logger.debug("SKIP: BTC MOVE %.2f < %.2f", abs_change, MIN_BTC_MOVE)
            return None
        if abs_change > MAX_BTC_MOVE:
            logger.debug("SKIP: BTC MOVE %.2f > %.2f", abs_change, MAX_BTC_MOVE)
            return None
        if mid_up < NEUTRAL_LO or mid_up > NEUTRAL_HI:
            logger.debug("SKIP: MID %.2f OUTSIDE [%.2f, %.2f]", mid_up, NEUTRAL_LO, NEUTRAL_HI)
            return None
        if (
            self._bid_for_side(market_price, side) <= MIN_BID
            or self._ask_for_side(market_price, side) > MAX_ASK
        ):
            logger.debug("SKIP: bid/ask out of range")
            return None
        return side

    def _log_metrics(self, ts_ms: int, market_price: MarketPriceEvent) -> None:
        if (
            self.btc_binance_base is None
            and self._elapsed(ts_ms) >= BINANCE_BASELINE_SEC
            and self._btc_binance_count > 0
        ):
            self.btc_binance_base = self._btc_binance_sum / self._btc_binance_count

        if self.btc_latest is None:
            return

        if self.btc_binance_base is None:
            baseline = "BIAN=N/A"
        else:
            diff = self.btc_latest - self.btc_binance_base
            pct = diff / self.btc_binance_base * 100
            baseline = f"BIAN={self.btc_binance_base:.2f}({diff:+.2f}/{pct:+.3f}%)"

        logger.info("BTC %.2f [%s]", self.btc_latest, baseline)
        logger.info(
            "mid=%.2f bid_up=%.2f ask_up=%.2f bid_down=%.2f ask_down=%.2f",
            (market_price.bid_yes + market_price.ask_yes) / 2,
            market_price.bid_yes,
            market_price.ask_yes,
            market_price.bid_no,
            market_price.ask_no,
        )

    def _target_id(self) -> str:
        return f"{self._window_id}:position"

    def _elapsed(self, ts_ms: int) -> float:
        return (ts_ms - self._window_start_ms) / 1000

    def _elapsed_since_entry(self, ts_ms: int) -> float:
        return (ts_ms - self.entry_ts_ms) / 1000 if self.entry_ts_ms else 0.0

    @staticmethod
    def _token_for_side(market_price: MarketPriceEvent, side: str) -> Token:
        if side == UP_OUTCOME.lower():
            return market_price.market.yes_token
        if side == DOWN_OUTCOME.lower():
            return market_price.market.no_token
        raise ValueError(f"invalid signal side: {side}")

    @staticmethod
    def _bid_for_side(market_price: MarketPriceEvent, side: str) -> float:
        return market_price.bid_yes if side == UP_OUTCOME.lower() else market_price.bid_no

    @staticmethod
    def _ask_for_side(market_price: MarketPriceEvent, side: str) -> float:
        return market_price.ask_yes if side == UP_OUTCOME.lower() else market_price.ask_no
