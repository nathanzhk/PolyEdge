import asyncio
import logging

from app import Runtime
from markets.btc import BTC5mMarket
from strategy.flash import FlashStrategy
from utils.env import Env
from utils.logger import configure_logging, get_logger

logger = get_logger("MAIN")


async def run() -> None:
    Env.load()
    configure_logging()

    bus_logger = get_logger("BUS")
    bus_logger.setLevel(logging.INFO)
    state_logger = get_logger("STATE")
    state_logger.setLevel(logging.INFO)

    runtime = Runtime(
        market=BTC5mMarket,
        symbol="BTCUSDT",
        strategy=FlashStrategy(),
    )
    await runtime.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
