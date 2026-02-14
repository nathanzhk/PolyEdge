from dataclasses import dataclass
from typing import ClassVar

from adapters.market_api import get_market_by_slug
from common import time


@dataclass(slots=True, frozen=True)
class Token:
    id: str
    outcome: str


@dataclass(slots=True, frozen=True)
class Market:
    id: str
    slug: str
    title: str
    start_ts_s: int
    end_ts_s: int
    start_ts_ms: int
    end_ts_ms: int
    yes_token: Token
    no_token: Token
    fee_rate: float


@dataclass(slots=True, frozen=True)
class Btc5mMarket(Market):
    interval_s: ClassVar[int] = 5 * 60
    slug_prefix: ClassVar[str] = "btc-updown-5m"
    up_outcome: ClassVar[str] = "Up"
    down_outcome: ClassVar[str] = "Down"

    @classmethod
    def now(cls) -> "Btc5mMarket":
        start_ts_s, end_ts_s = time.current_5m_window_s()
        return cls._from_window(start_ts_s, end_ts_s)

    @classmethod
    def next(cls) -> "Btc5mMarket":
        _, current_end_ts_s = time.current_5m_window_s()
        start_ts_s = current_end_ts_s
        end_ts_s = start_ts_s + cls.interval_s
        return cls._from_window(start_ts_s, end_ts_s)

    @classmethod
    def _from_window(cls, start_ts_s: int, end_ts_s: int) -> "Btc5mMarket":
        slug = f"{cls.slug_prefix}-{start_ts_s}"

        metadata = get_market_by_slug(slug)
        if metadata is None:
            raise ValueError(f"market not found: {slug}")

        return cls(
            id=metadata["id"],
            slug=slug,
            title=metadata["title"],
            start_ts_s=start_ts_s,
            end_ts_s=end_ts_s,
            start_ts_ms=start_ts_s * 1000,
            end_ts_ms=end_ts_s * 1000,
            yes_token=Token(
                id=metadata["tokens"][cls.up_outcome],
                outcome=cls.up_outcome,
            ),
            no_token=Token(
                id=metadata["tokens"][cls.down_outcome],
                outcome=cls.down_outcome,
            ),
            fee_rate=metadata["fee_rate"],
        )

    @property
    def up_token(self) -> Token:
        return self.yes_token

    @property
    def down_token(self) -> Token:
        return self.no_token
