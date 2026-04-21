from __future__ import annotations

from dataclasses import dataclass

from events import DesiredPositionEvent, RuntimeStateEvent
from markets.base import Market
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("LATE_REVERSAL")


@dataclass(slots=True, frozen=True)
class _Config:
    window_sec: float = 10.0
    entry_shares: float = 5.0
    max_entry_ask: float = 0.80


class LateReversalStrategy:
    def __init__(self) -> None:
        self.config = _Config()
        self._last_market_id: str | None = None

    def evaluate(self, state: RuntimeStateEvent) -> list[DesiredPositionEvent]:
        market = state.market

        if market.id != self._last_market_id:
            self._last_market_id = market.id

        remaining_s = _remaining_s(market, now_ts_ms())

        if remaining_s > self.config.window_sec or remaining_s <= 0:
            return []

        prev_side = state.prev_side
        curr_side = state.curr_side

        if prev_side is None or curr_side is None:
            return []
        if prev_side == curr_side:
            return []

        # Reversal detected: prev_side != curr_side within last 10 seconds
        logger.info(
            "reversal detected: %s -> %s, remaining=%.1fs",
            prev_side,
            curr_side,
            remaining_s,
        )

        # Buy the side that just flipped to (curr_side)
        if curr_side == "UP":
            token = market.yes_token
        else:
            token = market.no_token

        return [
            DesiredPositionEvent(
                market=market,
                token=token,
                shares=self.config.entry_shares,
                best_bid=self.config.max_entry_ask,
                best_ask=self.config.max_entry_ask,
                force=True,
            )
        ]


def _remaining_s(market: Market, ts_ms: int) -> float:
    return (market.end_ts_ms - ts_ms) / 1000
