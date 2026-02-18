from execution.trade_intent import TradeIntent
from strategies.context import StrategyContext
from utils.logger import get_logger
from utils.time import now_ts_ms

logger = get_logger("STRATEGY")


class LoggingStrategy:
    def on_market(self, context: StrategyContext) -> TradeIntent | None:

        ts_ms = now_ts_ms()

        market_price = context.market.market_price
        crypto_price = context.market.crypto_price
        crypto_ohlcv = context.market.crypto_ohlcv
        crypto_macd = context.indicators.crypto_macd

        if market_price is not None:
            logger.info(
                "market %s bid_yes=%.2f ask_yes=%.2f bid_no=%.2f ask_no%.2f",
                market_price.market.slug,
                market_price.bid_yes,
                market_price.ask_yes,
                market_price.bid_no,
                market_price.ask_no,
            )

        if crypto_price is not None and crypto_ohlcv is not None:
            logger.info(
                "btc %sms price=%s open=%s high=%s low=%s close=%s volume=%s",
                ts_ms - crypto_price.ts_ms,
                crypto_price.price,
                crypto_ohlcv.open,
                crypto_ohlcv.high,
                crypto_ohlcv.low,
                crypto_ohlcv.close,
                crypto_ohlcv.volume,
            )

        if crypto_macd is not None:
            logger.info(
                "test indicator value1=%s value2=%s",
                crypto_macd.value,
                crypto_macd.histogram,
            )
        return None
