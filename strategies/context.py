from dataclasses import dataclass

from indicators.indicator import MacdValue
from markets.base import Market, Token
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent


@dataclass(slots=True)
class Position:
    token: Token
    opening_shares: float
    holding_shares: float
    closing_shares: float


@dataclass(slots=True, frozen=True)
class MarketLatestState:
    market: Market | None
    beat_price: float | None
    market_price: MarketPriceEvent | None
    crypto_price: CryptoPriceEvent | None
    crypto_ohlcv: CryptoOHLCVEvent | None


@dataclass(slots=True, frozen=True)
class PositionLatestState:
    positions: list[Position]

    def get_position(self, token: Token) -> Position | None:
        if token is None:
            return None
        for position in self.positions:
            if position.token.id == token.id:
                return position
        return None


@dataclass(slots=True, frozen=True)
class IndicatorLatestState:
    crypto_macd: MacdValue | None


@dataclass(slots=True, frozen=True)
class StrategyContext:
    market: MarketLatestState
    position: PositionLatestState
    indicators: IndicatorLatestState
