from __future__ import annotations

from typing import Protocol

from events import DesiredPositionEvent, RuntimeStateEvent
from utils.logger import get_logger
from utils.time import elapsed_ms_since

logger = get_logger("DEFAULT STRATEGY")


class Strategy(Protocol):
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        raise NotImplementedError


class DefaultStrategy:
    def evaluate(self, state: RuntimeStateEvent) -> DesiredPositionEvent | None:
        logger.debug(
            "yes=%.2fms no=%.2fms btc=%.2fms ohlcv=%.2fms",
            elapsed_ms_since(state.yes_token_quote.recv_mono_ns),
            elapsed_ms_since(state.no_token_quote.recv_mono_ns),
            elapsed_ms_since(state.crypto_quote.recv_mono_ns),
            elapsed_ms_since(state.crypto_ohlcv.recv_mono_ns),
        )
        logger.debug(
            "bid_yes=%.2f ask_yes=%.2f bid_no=%.2f ask_no=%.2f btc=%.2f diff=%+.2f",
            state.yes_token_quote.best_bid,
            state.yes_token_quote.best_ask,
            state.no_token_quote.best_bid,
            state.no_token_quote.best_ask,
            state.crypto_quote.mid,
            state.crypto_quote.mid - state.beat_price,
        )
        return None
