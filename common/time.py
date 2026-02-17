import time

WINDOW_5M_S = 5 * 60
WINDOW_15M_S = 15 * 60


def now_ts_s() -> int:
    return time.time_ns() // 1_000_000_000


def now_ts_ms() -> int:
    return time.time_ns() // 1_000_000


def current_5m_window_s() -> tuple[int, int]:
    return current_window_s(WINDOW_5M_S)


def current_5m_window_ms() -> tuple[int, int]:
    start_ts_s, end_ts_s = current_5m_window_s()
    return start_ts_s * 1000, end_ts_s * 1000


def current_15m_window_s() -> tuple[int, int]:
    return current_window_s(WINDOW_15M_S)


def current_15m_window_ms() -> tuple[int, int]:
    start_ts_s, end_ts_s = current_15m_window_s()
    return start_ts_s * 1000, end_ts_s * 1000


def current_window_s(window_s: int) -> tuple[int, int]:
    current_ts_s = now_ts_s()
    start_ts_s = current_ts_s - (current_ts_s % window_s)
    end_ts_s = start_ts_s + window_s
    return start_ts_s, end_ts_s
