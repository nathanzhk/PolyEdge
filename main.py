import asyncio
import logging

from dotenv import load_dotenv

from models.btc_market import BTC5mMarket
from runtime.runner import Runner
from strategies.logging_strategy import LoggingStrategy
from streams.crypto_stream import CryptoPriceStream
from streams.market_price_stream import MarketPriceStream
from streams.market_trade_stream import MarketTradeStream
from trade.execution_engine import ExecutionEngine
from trade.trade_client import MakerTradeClient, TakerTradeClient
from utils.logger import configure_logging, get_logger

logger = get_logger("MAIN")


async def run() -> None:
    load_dotenv()
    configure_logging()

    market_logger = get_logger("MARKET STATE")
    market_logger.setLevel(logging.INFO)

    logger.info("initializing trade clients")
    maker = await asyncio.to_thread(MakerTradeClient)
    taker = await asyncio.to_thread(TakerTradeClient)
    logger.info("trade clients initialized")

    logger.info("starting market price, market trade, and crypto price streams")
    runner = Runner(
        market_price_stream=MarketPriceStream(BTC5mMarket, interval_ms=10),
        market_trade_stream=MarketTradeStream(maker.credentials),
        crypto_price_stream=CryptoPriceStream("btcusdt", interval_ms=10),
        strategy=LoggingStrategy(),
        execution_engine=ExecutionEngine(maker, taker),
    )
    await runner.run()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
