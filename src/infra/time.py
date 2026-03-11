import asyncio
import time
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Los_Angeles")
MARKET_TZ = ZoneInfo("America/New_York")
TimestampTimezone = Literal["local", "market"]

WINDOW_5M_S = 5 * 60
WINDOW_15M_S = 15 * 60

DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_TIME_MS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
TimestampFormat = Literal["date", "time", "datetime", "datetime_ms"]


def now_ts_s() -> int:
    return time.time_ns() // 1_000_000_000


def now_ts_ms() -> int:
    return time.time_ns() // 1_000_000


def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def fmt_ts_ms(
    ts_ms: int, *, tz: TimestampTimezone = "local", fmt: TimestampFormat = "datetime"
) -> str:
    return fmt_ts_s(ts_ms / 1_000, tz=tz, fmt=fmt)


def fmt_ts_s(
    ts_s: int | float, *, tz: TimestampTimezone = "local", fmt: TimestampFormat = "datetime"
) -> str:
    match tz:
        case "local":
            tzinfo = LOCAL_TZ
        case "market":
            tzinfo = MARKET_TZ
        case _:
            raise ValueError(f"Unsupported timestamp timezone: {tz}")

    dt = datetime.fromtimestamp(ts_s, tz=tzinfo)

    match fmt:
        case "date":
            return dt.strftime(DATE_FORMAT)
        case "time":
            return dt.strftime(TIME_FORMAT)
        case "datetime":
            return dt.strftime(DATE_TIME_FORMAT)
        case "datetime_ms":
            return dt.strftime(DATE_TIME_MS_FORMAT)[:-3]
        case _:
            raise ValueError(f"Unsupported timestamp format: {fmt}")


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
