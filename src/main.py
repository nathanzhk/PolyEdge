import asyncio
import logging

from clients.polymarket_clob import MakerTradeClient, TakerTradeClient
from execution.engine import ExecutionEngine
from feeds.crypto_quote import CryptoQuoteStream
from feeds.market_quote import MarketQuoteStream
from feeds.market_trade import MarketTradeStream
from infra.env import Env
from infra.logger import configure_logging, get_logger
from markets.btc import BTC5mMarket
from runtime.runner import Runner
from strategies.strategy import DefaultStrategy

logger = get_logger("MAIN")


async def run() -> None:
    Env.load()
    configure_logging()

    market_logger = get_logger("MARKET STATE")
    market_logger.setLevel(logging.INFO)

    logger.info("initializing trade clients")
    maker = await asyncio.to_thread(MakerTradeClient)
    taker = await asyncio.to_thread(TakerTradeClient)
    logger.info("trade clients initialized")

    logger.info("starting market quote, market trade, and crypto quote streams")
    runner = Runner(
        market_quote_stream=MarketQuoteStream(BTC5mMarket, interval_ms=10),
        market_trade_stream=MarketTradeStream(maker.get_credentials()),
        crypto_quote_stream=CryptoQuoteStream("btcusdt", interval_ms=10),
        strategy=DefaultStrategy(),
        execution_engine=ExecutionEngine(maker, taker),
    )
    await runner.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
