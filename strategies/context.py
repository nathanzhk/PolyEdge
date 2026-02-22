from dataclasses import dataclass

from indicators.indicator import MacdValue
from models.market import Market
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from trade.models import TradeLatestState


@dataclass(slots=True, frozen=True)
class MarketLatestState:
    market: Market | None
    beat_price: float | None
    market_price: MarketPriceEvent | None
    crypto_price: CryptoPriceEvent | None
    crypto_ohlcv: CryptoOHLCVEvent | None


@dataclass(slots=True, frozen=True)
class IndicatorLatestState:
    crypto_macd: MacdValue | None


@dataclass(slots=True, frozen=True)
class StrategyContext:
    market: MarketLatestState
    indicators: IndicatorLatestState
    trade: TradeLatestState | None = None
