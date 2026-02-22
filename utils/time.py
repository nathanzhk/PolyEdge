import asyncio
import time
from datetime import datetime

WINDOW_5M_S = 5 * 60
WINDOW_15M_S = 15 * 60
DATE_TIME_MS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def now_ts_s() -> int:
    return time.time_ns() // 1_000_000_000


def now_ts_ms() -> int:
    return time.time_ns() // 1_000_000


def format_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1_000)
    return dt.strftime(DATE_TIME_MS_FORMAT)[:-3]


async def sleep_until(ts_s: int) -> None:
    delay_s = ts_s - now_ts_s()
    if delay_s > 0:
        await asyncio.sleep(delay_s)


def current_5m_window_s() -> tuple[int, int]:
    return _current_window_s(WINDOW_5M_S)


def current_15m_window_s() -> tuple[int, int]:
    return _current_window_s(WINDOW_15M_S)


def _current_window_s(window_s: int) -> tuple[int, int]:
    current_ts_s = now_ts_s()
    start_ts_s = current_ts_s - (current_ts_s % window_s)
    end_ts_s = start_ts_s + window_s
    return start_ts_s, end_ts_s
