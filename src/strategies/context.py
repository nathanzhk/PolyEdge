from dataclasses import dataclass

from events.crypto_ohlcv import CryptoOHLCVEvent
from events.crypto_price import CryptoPriceEvent
from events.market_price import MarketPriceEvent
from indicators.indicator import MacdValue
from markets.base import Market, Token


@dataclass(slots=True)
class Position:
    token: Token
    opening_shares: float
    holding_shares: float
    closing_shares: float
    holding_avg_price: float | None = None
    holding_cost: float = 0.0
    holding_open_ts_ms: int | None = None


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
