import asyncio

from strategies.strategy import DefaultStrategy

from app import TradingApp
from markets.btc import BTC5mMarket
from utils.env import Env
from utils.logger import configure_logging, get_logger

logger = get_logger("MAIN")


async def run() -> None:
    Env.load()
    configure_logging()

    await TradingApp(
        market=BTC5mMarket,
        symble="BTCUSDT",
        strategy=DefaultStrategy(),
    ).run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
