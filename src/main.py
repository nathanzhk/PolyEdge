import asyncio
import logging

from clients import MakerTradeClient, TakerTradeClient
from execution import ExecutionEngine
from feeds import CryptoPriceStream, MarketQuoteStream, MarketTradeStream
from infra import Env, configure_logging, get_logger
from markets import BTC5mMarket
from runtime import Runner
from strategies import DefaultStrategy

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

    logger.info("starting market quote, market trade, and crypto price streams")
    runner = Runner(
        market_quote_stream=MarketQuoteStream(BTC5mMarket, interval_ms=10),
        market_trade_stream=MarketTradeStream(maker.get_credentials()),
        crypto_price_stream=CryptoPriceStream("btcusdt", interval_ms=10),
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
