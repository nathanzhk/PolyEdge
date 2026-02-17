from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Self

from adapters.market_api import get_market_by_slug
from common.time import current_5m_window_s, current_15m_window_s
from domain.market import Market, Token

UP_OUTCOME = "Up"
DOWN_OUTCOME = "Down"


@dataclass(slots=True, frozen=True)
class _BTCMarket(Market):
    interval_s: ClassVar[int]
    slug_prefix: ClassVar[str]
    window_func: ClassVar[staticmethod[[], tuple[int, int]]]

    @classmethod
    def now(cls) -> Self:
        start_ts_s, end_ts_s = cls.window_func()
        return cls._from_window(start_ts_s, end_ts_s)

    @classmethod
    def next(cls) -> Self:
        _, current_end_ts_s = cls.window_func()
        start_ts_s = current_end_ts_s
        end_ts_s = start_ts_s + cls.interval_s
        return cls._from_window(start_ts_s, end_ts_s)

    @classmethod
    def _from_window(cls, start_ts_s: int, end_ts_s: int) -> Self:
        slug = f"{cls.slug_prefix}-{start_ts_s}"

        metadata = get_market_by_slug(slug)
        if metadata is None:
            raise ValueError(f"market not found: {slug}")

        market_id = metadata["id"]
        return cls(
            id=market_id,
            slug=slug,
            title=metadata["title"],
            start_ts_s=start_ts_s,
            end_ts_s=end_ts_s,
            start_ts_ms=start_ts_s * 1000,
            end_ts_ms=end_ts_s * 1000,
            yes_token=Token(
                id=metadata["tokens"][UP_OUTCOME],
                outcome=UP_OUTCOME,
                market_id=market_id,
            ),
            no_token=Token(
                id=metadata["tokens"][DOWN_OUTCOME],
                outcome=DOWN_OUTCOME,
                market_id=market_id,
            ),
            fee_rate=metadata["fee_rate"],
        )

    @property
    def up_token(self) -> Token:
        return self.yes_token

    @property
    def down_token(self) -> Token:
        return self.no_token


@dataclass(slots=True, frozen=True)
class BTC5mMarket(_BTCMarket):
    interval_s: ClassVar[int] = 5 * 60
    slug_prefix: ClassVar[str] = "btc-updown-5m"
    window_func: ClassVar[staticmethod[[], tuple[int, int]]] = staticmethod(current_5m_window_s)


@dataclass(slots=True, frozen=True)
class BTC15mMarket(_BTCMarket):
    interval_s: ClassVar[int] = 15 * 60
    slug_prefix: ClassVar[str] = "btc-updown-15m"
    window_func: ClassVar[staticmethod[[], tuple[int, int]]] = staticmethod(current_15m_window_s)
