from dataclasses import dataclass

from indicators.indicator import MacdValue
from markets.base import Market, Token
from streams.crypto_ohlcv_event import CryptoOHLCVEvent
from streams.crypto_price_event import CryptoPriceEvent
from streams.market_price_event import MarketPriceEvent
from trade.managed_order import ManagedOrder, Position


@dataclass(slots=True, frozen=True)
class MarketLatestState:
    market: Market | None
    beat_price: float | None
    market_price: MarketPriceEvent | None
    crypto_price: CryptoPriceEvent | None
    crypto_ohlcv: CryptoOHLCVEvent | None


@dataclass(slots=True, frozen=True)
class PositionLatestState:
    open_orders: tuple[ManagedOrder, ...]
    positions: dict[str, Position]
    unknown_order_count: int = 0

    def position_shares(self, token: Token | None) -> float:
        if token is None:
            return 0.0
        position = self.positions.get(token.id)
        return 0.0 if position is None else position.shares


@dataclass(slots=True, frozen=True)
class IndicatorLatestState:
    crypto_macd: MacdValue | None


@dataclass(slots=True, frozen=True)
class StrategyContext:
    market: MarketLatestState
    position: PositionLatestState
    indicators: IndicatorLatestState
