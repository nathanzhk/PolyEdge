from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Literal

from events import (
    CryptoOHLCVEvent,
    CryptoQuoteEvent,
    CurrentPositionEvent,
    MarketQuoteEvent,
    RuntimeStateEvent,
)
from markets.base import Market
from utils.logger import get_logger
from utils.time import elapsed_ms_since, fmt_duration_s, now_ts_s

logger = get_logger("STATE")

Side = Literal["UP", "DOWN"] | None


class Indicators:
    __slots__ = ("prev_side", "curr_side")

    def __init__(self) -> None:
        self.prev_side: Side = None
        self.curr_side: Side = None

    def update(
        self,
        yes_mid: float,
        no_mid: float,
        btc_change: float | None,
    ) -> None:
        self.prev_side = self.curr_side

        if btc_change is None:
            self.curr_side = None
        elif yes_mid > no_mid and btc_change > 0:
            self.curr_side = "UP"
        elif no_mid > yes_mid and btc_change < 0:
            self.curr_side = "DOWN"
        else:
            self.curr_side = None

    def reset(self) -> None:
        self.prev_side = None
        self.curr_side = None


class RuntimeState:
    def __init__(self, market: Market) -> None:
        self._lock = asyncio.Lock()
        self._last_signature: tuple[Any, ...] | None = None

        self._yes_token = market.yes_token
        self._no_token = market.no_token
        self._market = market

        self.indicators = Indicators()

        self._yes_quote: MarketQuoteEvent | None = None
        self._no_quote: MarketQuoteEvent | None = None

        self._crypto_quote: CryptoQuoteEvent | None = None
        self._crypto_ohlcv: CryptoOHLCVEvent | None = None

        self._yes_position: CurrentPositionEvent | None = None
        self._no_position: CurrentPositionEvent | None = None

    async def update_market_quote(
        self,
        quote: MarketQuoteEvent,
    ) -> RuntimeStateEvent | None:
        async with self._lock:
            if quote.market.id != self._market.id:
                logger.debug("ignore quote for other market: %s", quote.market.id)
                return None
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
            state_event = self._event_if_changed("crypto_quote")
        logger.debug(
            "latency=%.2fms symbol=%s price=%.2f baseline=%s change=%s",
            elapsed_ms_since(quote.recv_mono_ns),
            quote.symbol,
            quote.mid,
            _fmt_optional_usd(quote.baseline),
            _fmt_optional_signed_usd(quote.change),
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
            if position.market.id != self._market.id:
                logger.debug("ignore position for other market: %s", position.market.id)
                return None
            if position.token.id == self._yes_token.id:
                self._yes_position = position
            elif position.token.id == self._no_token.id:
                self._no_position = position
            return self._event_if_changed("current_position")

    def _event_if_changed(self, reason: str) -> RuntimeStateEvent | None:
        event = self._build_event(reason)
        if event is None:
            return None
        signature = _event_signature(event)
        if signature == self._last_signature:
            return None
        self._last_signature = signature
        self.indicators.update(
            yes_mid=event.yes_token_quote.mid,
            no_mid=event.no_token_quote.mid,
            btc_change=event.crypto_quote.change,
        )
        event = dataclasses.replace(
            event,
            prev_side=self.indicators.prev_side,
            curr_side=self.indicators.curr_side,
        )
        _log_event(event)
        return event

    def _build_event(self, reason: str) -> RuntimeStateEvent | None:
        if (
            self._no_quote is None
            or self._yes_quote is None
            or self._crypto_quote is None
            or self._crypto_ohlcv is None
        ):
            return None
        return RuntimeStateEvent(
            reason=reason,
            market=self._market,
            yes_token_quote=self._yes_quote,
            no_token_quote=self._no_quote,
            crypto_quote=self._crypto_quote,
            crypto_ohlcv=self._crypto_ohlcv,
            yes_token_position=self._yes_position,
            no_token_position=self._no_position,
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
            event.crypto_quote.baseline,
            event.crypto_quote.change,
            event.crypto_quote.price,
        ),
        (
            event.crypto_ohlcv.start_ts_ms,
            event.crypto_ohlcv.close_ts_ms,
        ),
        (
            event.yes_token_position.opening_shares,
            event.yes_token_position.holding_shares,
            event.yes_token_position.closing_shares,
            event.yes_token_position.holding_cost,
            event.yes_token_position.holding_avg_price,
            event.yes_token_position.realized_pnl,
        )
        if event.yes_token_position is not None
        else (),
        (
            event.no_token_position.opening_shares,
            event.no_token_position.holding_shares,
            event.no_token_position.closing_shares,
            event.no_token_position.holding_cost,
            event.no_token_position.holding_avg_price,
            event.no_token_position.realized_pnl,
        )
        if event.no_token_position is not None
        else (),
    )


def _log_event(event: RuntimeStateEvent) -> None:
    logger.debug(
        "UP %.2fms | DN %.2fms | BTC %.2fms | OHLCV %.2fms",
        elapsed_ms_since(event.yes_token_quote.recv_mono_ns),
        elapsed_ms_since(event.no_token_quote.recv_mono_ns),
        elapsed_ms_since(event.crypto_quote.recv_mono_ns),
        elapsed_ms_since(event.crypto_ohlcv.recv_mono_ns),
    )
    logger.info(
        "[%s-%s] ▲ bid %.2f ask %.2f | ▼ bid %.2f ask %.2f | $%.2f %s",
        fmt_duration_s(now_ts_s() - event.market.start_ts_s),
        fmt_duration_s(event.market.end_ts_s - now_ts_s()),
        event.yes_token_quote.best_bid,
        event.yes_token_quote.best_ask,
        event.no_token_quote.best_bid,
        event.no_token_quote.best_ask,
        event.crypto_quote.mid,
        _fmt_optional_signed_usd(event.crypto_quote.change),
    )
    logger.info(
        "UP | open %.6f | open settling %.6f | close %.6f | close settling %.6f",
        event.yes_token_position.opening_shares,
        event.yes_token_position.open_settling_shares,
        event.yes_token_position.closing_shares,
        event.yes_token_position.close_settling_shares,
    ) if event.yes_token_position is not None else ...
    logger.info(
        "UP | hold %.6f @ %.2f %s | PnL %s",
        event.yes_token_position.holding_shares,
        event.yes_token_position.holding_avg_price,
        _fmt_signed_usd(
            event.yes_token_position.holding_shares * event.yes_token_quote.best_bid
            - event.yes_token_position.holding_cost
        ),
        _fmt_signed_usd(event.yes_token_position.realized_pnl),
    ) if event.yes_token_position is not None else ...
    logger.info(
        "DN | open %.6f | open settling %.6f | close %.6f | close settling %.6f",
        event.no_token_position.opening_shares,
        event.no_token_position.open_settling_shares,
        event.no_token_position.closing_shares,
        event.no_token_position.close_settling_shares,
    ) if event.no_token_position is not None else ...
    logger.info(
        "DN | hold %.6f @ %.2f %s | PnL %s",
        event.no_token_position.holding_shares,
        event.no_token_position.holding_avg_price,
        _fmt_signed_usd(
            event.no_token_position.holding_shares * event.no_token_quote.best_bid
            - event.no_token_position.holding_cost
        ),
        _fmt_signed_usd(event.no_token_position.realized_pnl),
    ) if event.no_token_position is not None else ...


def _fmt_signed_usd(value: float) -> str:
    return f"{'△ +' if value >= 0 else '▽ -'}${abs(value):.2f}"


def _fmt_optional_usd(value: float | None) -> str:
    if value is None:
        return "--"
    return f"${value:.2f}"


def _fmt_optional_signed_usd(value: float | None) -> str:
    if value is None:
        return "--"
    return _fmt_signed_usd(value)
