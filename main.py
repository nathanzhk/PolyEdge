import asyncio

from dotenv import load_dotenv

from execution.trade_client import MakerTradeClient
from execution.trader import TradeClientExecutor
from models.btc_market import BTC5mMarket
from models.market import Market
from runtime.runner import Runner
from strategies.logging_strategy import LoggingStrategy
from streams.crypto_stream import CryptoPriceStream
from streams.market_stream import MarketStream
from utils.logger import configure_logging


async def run() -> None:
    load_dotenv()
    configure_logging()

    maker = MakerTradeClient()
    market: Market = BTC5mMarket.curr_market()
    maker.warm_up(market)

    runner = Runner(
        market_stream=MarketStream(BTC5mMarket),
        crypto_price_stream=CryptoPriceStream("btcusdt"),
        strategy=LoggingStrategy(),
        trader=TradeClientExecutor(maker),
    )
    await runner.run()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
