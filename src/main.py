import argparse
import asyncio
import contextlib
import logging
import sys
from pathlib import Path

from app import Runtime
from markets.btc import BTC5mMarket
from strategy.strategy import DefaultStrategy
from utils.env import Env
from utils.logger import configure_logging, get_logger, set_log_file
from utils.time import sleep_until

logger = get_logger("MAIN")

_DEFAULT_PREWARM_S = 5
_DEFAULT_SHUTDOWN_S = 5


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("supervisor", "worker"),
        default="supervisor",
        help="supervisor starts one worker per market; worker runs one fixed market",
    )
    parser.add_argument(
        "--market-start-ts",
        type=int,
        default=None,
        help="market start timestamp in seconds; worker mode only",
    )
    parser.add_argument(
        "--prewarm-s",
        type=int,
        default=_DEFAULT_PREWARM_S,
        help="seconds before the next market start to launch its worker",
    )
    parser.add_argument(
        "--worker-grace-s",
        type=int,
        default=_DEFAULT_SHUTDOWN_S,
        help="seconds after market end before a worker exits",
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    Env.load()
    configure_logging()

    bus_logger = get_logger("BUS")
    bus_logger.setLevel(logging.INFO)
    state_logger = get_logger("STATE")
    state_logger.setLevel(logging.INFO)

    if args.mode == "worker":
        await run_worker(args.market_start_ts, worker_grace_s=args.worker_grace_s)
    else:
        await run_supervisor(prewarm_s=args.prewarm_s, worker_grace_s=args.worker_grace_s)


async def run_worker(market_start_ts: int | None, *, worker_grace_s: int) -> None:
    market = (
        BTC5mMarket.curr_market()
        if market_start_ts is None
        else BTC5mMarket.from_start_ts(market_start_ts)
    )
    set_log_file(market.slug)
    logger.info("start worker for %s", market.slug)
    logger.info("%s", market.title)

    runtime = Runtime(
        market=market,
        symbol="BTCUSDT",
        strategy=DefaultStrategy(),
    )
    runtime_task = asyncio.create_task(
        runtime.run(),
        name=f"runtime-{market.slug}",
    )
    stop_task = asyncio.create_task(
        sleep_until(market.end_ts_s + worker_grace_s),
        name=f"stop-{market.slug}",
    )

    done, pending = await asyncio.wait(
        {runtime_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()

    if runtime_task in done:
        await asyncio.gather(*pending, return_exceptions=True)
        await runtime_task
        return

    logger.info("market ended; stop worker for %s", market.slug)
    runtime_task.cancel()
    await asyncio.gather(runtime_task, return_exceptions=True)


async def run_supervisor(*, prewarm_s: int, worker_grace_s: int) -> None:
    set_log_file("supervisor")
    logger.info("start supervisor")
    running: dict[int, asyncio.subprocess.Process] = {}
    try:
        while True:
            await _cleanup_workers(running)

            curr_market = BTC5mMarket.curr_market()
            await _ensure_worker(running, curr_market, worker_grace_s=worker_grace_s)

            next_market = BTC5mMarket.next_market()
            await sleep_until(next_market.start_ts_s - prewarm_s)
            await _ensure_worker(running, next_market, worker_grace_s=worker_grace_s)

            await sleep_until(next_market.start_ts_s)
    finally:
        await _terminate_workers(running)


async def _ensure_worker(
    running: dict[int, asyncio.subprocess.Process],
    market: BTC5mMarket,
    *,
    worker_grace_s: int,
) -> None:
    proc = running.get(market.start_ts_s)
    if proc is not None and proc.returncode is None:
        return

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        "worker",
        "--market-start-ts",
        str(market.start_ts_s),
        "--worker-grace-s",
        str(worker_grace_s),
    ]
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=Path(__file__).resolve().parents[1])
    running[market.start_ts_s] = proc
    logger.info("started worker pid=%s market=%s", proc.pid, market.slug)


async def _cleanup_workers(running: dict[int, asyncio.subprocess.Process]) -> None:
    for start_ts_s, proc in list(running.items()):
        if proc.returncode is None:
            continue
        returncode = await proc.wait()
        if returncode == 0:
            logger.info("worker exited start_ts=%s", start_ts_s)
        else:
            logger.error("worker exited start_ts=%s returncode=%s", start_ts_s, returncode)
        del running[start_ts_s]


async def _terminate_workers(running: dict[int, asyncio.subprocess.Process]) -> None:
    for proc in running.values():
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
    for proc in running.values():
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                await proc.wait()


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
