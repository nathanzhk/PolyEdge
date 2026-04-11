from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self


@dataclass(slots=True, frozen=True)
class Token:
    id: str
    outcome: str
    market_id: str

    @property
    def key(self) -> str:
        return self.outcome.lower()


@dataclass(slots=True, frozen=True)
class Market(ABC):
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

    @classmethod
    @abstractmethod
    def curr_market(cls) -> Self:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def next_market(cls) -> Self:
        raise NotImplementedError
