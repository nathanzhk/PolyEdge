from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CryptoPriceEvent:
    ts_ms: int
    symbol: str
    price: float
    best_bid: float
    best_ask: float
