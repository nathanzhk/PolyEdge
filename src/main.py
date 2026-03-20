import asyncio

from app import Runtime
from markets.btc import BTC5mMarket
from strategy.strategy import DefaultStrategy
from utils.env import Env
from utils.logger import configure_logging, get_logger

logger = get_logger("MAIN")


async def run() -> None:
    Env.load()
    configure_logging()

    runtime = Runtime(
        market=BTC5mMarket,
        symbol="BTCUSDT",
        strategy=DefaultStrategy(),
    )
    await runtime.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
