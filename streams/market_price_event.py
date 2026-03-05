from dataclasses import dataclass

from markets.base import Market


@dataclass(slots=True, frozen=True)
class MarketPriceEvent:
    ts_ms: int
    market: Market
    bid_yes: float
    ask_yes: float
    bid_no: float
    ask_no: float
