from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CryptoOHLCVEvent:
    ts_ms: int
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
