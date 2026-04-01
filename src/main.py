import asyncio
import logging

from app import Runtime
from markets.btc import BTC5mMarket
from strategy.superman import SupermanStrategy
from utils.env import Env
from utils.logger import configure_logging, get_logger

logger = get_logger("MAIN")


async def run() -> None:
    Env.load()
    configure_logging()

    bus_logger = get_logger("BUS")
    state_logger = get_logger("STATE")
    maker_logger = get_logger("MAKER")
    taker_logger = get_logger("TAKER")
    for info_logger in {bus_logger, state_logger, maker_logger, taker_logger}:
        info_logger.setLevel(logging.INFO)

    runtime = Runtime(
        market=BTC5mMarket,
        symbol="BTCUSDT",
        strategy=SupermanStrategy(),
    )
    await runtime.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
