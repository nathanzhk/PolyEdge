from dataclasses import dataclass

from markets import Market


@dataclass(slots=True, frozen=True)
class MarketQuoteEvent:
    ts_ms: int
    market: Market
    bid_yes: float
    ask_yes: float
    bid_no: float
    ask_no: float
