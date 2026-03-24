from __future__ import annotations

import asyncio
from typing import Any

from events import (
    CryptoOHLCVEvent,
    CryptoQuoteEvent,
    CurrentPositionEvent,
    MarketQuoteEvent,
    RuntimeStateEvent,
)
from markets.base import Market, Token
from utils.logger import get_logger
from utils.time import elapsed_ms_since, fmt_duration_s, now_ts_s

logger = get_logger("STATE")

_MAX_BEAT_OFFSET_MS = 200


class RuntimeState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last_signature: tuple[Any, ...] | None = None

        self._yes_token: Token
        self._no_token: Token
        self._market: Market | None = None

        self._yes_quote: MarketQuoteEvent | None = None
        self._no_quote: MarketQuoteEvent | None = None

        self._crypto_quote: CryptoQuoteEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

        self._beat_price: float | None = None
        self._beat_offset_ms: int | None = None

        self._positions_by_token_id: dict[str, CurrentPositionEvent] = {}

    async def update_market_quote(
        self,
        quote: MarketQuoteEvent,
    ) -> RuntimeStateEvent | None:
        async with self._lock:
            if self._market is None or quote.market.id != self._market.id:
                self._switch_market(quote.market)
            if quote.token.id == self._yes_token.id:
                self._yes_quote = quote
            if quote.token.id == self._no_token.id:
                self._no_quote = quote
            state_event = self._event_if_changed("market_quote")
        logger.debug(
            "latency=%.2fms outcome=%s best_bid=%.2f best_ask=%.2f",
            elapsed_ms_since(quote.recv_mono_ns),
            quote.token.key,
            quote.best_bid,
            quote.best_ask,
        )
        return state_event

    async def update_crypto_quote(
        self,
        quote: CryptoQuoteEvent,
    ) -> RuntimeStateEvent | None:
        async with self._lock:
            self._crypto_quote = quote
            self._record_beat_price(quote)
            state_event = self._event_if_changed("crypto_quote")
            beat_price = self._beat_price
        if beat_price is not None:
            logger.debug(
                "latency=%.2fms symbol=%s best_bid=%.2f best_ask=%.2f diff=%+.2f",
                elapsed_ms_since(quote.recv_mono_ns),
                quote.symbol,
                quote.best_bid,
                quote.best_ask,
                quote.mid - beat_price,
            )
        return state_event

    async def update_crypto_ohlcv(
        self,
        ohlcv: CryptoOHLCVEvent,
    ) -> RuntimeStateEvent | None:
        async with self._lock:
            self._crypto_ohlcv = ohlcv
            state_event = self._event_if_changed("crypto_ohlcv")
        logger.debug(
            "latency=%.2fms open=%.2f high=%.2f low=%.2f close=%.2f volume=%.2f",
            elapsed_ms_since(ohlcv.recv_mono_ns),
            ohlcv.open,
            ohlcv.high,
            ohlcv.low,
            ohlcv.close,
            ohlcv.volume,
        )
        return state_event

    async def update_current_position(
        self, position: CurrentPositionEvent
    ) -> RuntimeStateEvent | None:
        async with self._lock:
            self._positions_by_token_id[position.token.id] = position
            return self._event_if_changed("current_position")

    def _switch_market(self, market: Market) -> None:
        self._yes_token = market.yes_token
        self._no_token = market.no_token
        self._yes_quote = None
        self._no_quote = None
        self._market = market
        self._beat_price = None
        self._beat_offset_ms = None
        self._positions_by_token_id.clear()
        logger.info("%s", market.title)

    def _record_beat_price(self, quote: CryptoQuoteEvent) -> None:
        if self._market is None:
            return
        beat_offset_ms = quote.recv_ts_ms - self._market.start_ts_ms
        beat_offset_abs_ms = abs(beat_offset_ms)
        if beat_offset_abs_ms > _MAX_BEAT_OFFSET_MS:
            return
        if self._beat_offset_ms is None or beat_offset_abs_ms < abs(self._beat_offset_ms):
            self._beat_price = quote.mid
            self._beat_offset_ms = beat_offset_ms
            logger.info("beat=%.2f offset=%dms", self._beat_price, beat_offset_ms)

    def _event_if_changed(self, reason: str) -> RuntimeStateEvent | None:
        event = self._build_event(reason)
        if event is None:
            return None
        signature = _event_signature(event)
        if signature == self._last_signature:
            return None
        self._last_signature = signature
        _log_event(event)
        return event

    def _build_event(self, reason: str) -> RuntimeStateEvent | None:
        if (
            self._market is None
            or self._yes_quote is None
            or self._no_quote is None
            or self._crypto_quote is None
            or self._crypto_ohlcv is None
            or self._beat_price is None
        ):
            return None
        return RuntimeStateEvent(
            reason=reason,
            market=self._market,
            yes_token_quote=self._yes_quote,
            no_token_quote=self._no_quote,
            crypto_quote=self._crypto_quote,
            crypto_ohlcv=self._crypto_ohlcv,
            beat_price=self._beat_price,
            positions=tuple(self._positions_by_token_id.values()),
        )


def _event_signature(event: RuntimeStateEvent) -> tuple[Any, ...]:
    return (
        event.market.id,
        (
            event.yes_token_quote.best_bid,
            event.yes_token_quote.best_ask,
        ),
        (
            event.no_token_quote.best_bid,
            event.no_token_quote.best_ask,
        ),
        (
            event.crypto_quote.best_bid,
            event.crypto_quote.best_ask,
        ),
        (
            event.crypto_ohlcv.start_ts_ms,
            event.crypto_ohlcv.close_ts_ms,
        ),
        event.beat_price,
        tuple(
            (
                position.market.id,
                position.token.id,
                position.shares,
                position.price,
                position.opening_shares,
                position.holding_shares,
                position.closing_shares,
                position.holding_avg_price,
                position.holding_cost,
                position.holding_open_ts_ms,
            )
            for position in event.positions
        ),
    )


def _log_event(event: RuntimeStateEvent) -> None:
    logger.debug(
        "yes=%.2fms no=%.2fms btc=%.2fms ohlcv=%.2fms",
        elapsed_ms_since(event.yes_token_quote.recv_mono_ns),
        elapsed_ms_since(event.no_token_quote.recv_mono_ns),
        elapsed_ms_since(event.crypto_quote.recv_mono_ns),
        elapsed_ms_since(event.crypto_ohlcv.recv_mono_ns),
    )
    logger.info(
        "%s-%s | UP bid_%.2f ask_%.2f | DOWN bid_%.2f ask_%.2f | BTC $%.2f %s",
        fmt_duration_s(now_ts_s() - event.market.start_ts_s),
        fmt_duration_s(event.market.end_ts_s - now_ts_s()),
        event.yes_token_quote.best_bid,
        event.yes_token_quote.best_ask,
        event.no_token_quote.best_bid,
        event.no_token_quote.best_ask,
        event.crypto_quote.mid,
        _fmt_signed_usd(event.crypto_quote.mid - event.beat_price),
    )


def _fmt_signed_usd(value: float) -> str:
    return f"{'+' if value >= 0 else '-'}${abs(value):.2f}"
