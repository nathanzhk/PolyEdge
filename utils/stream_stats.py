from __future__ import annotations

import logging
from time import perf_counter_ns

from utils.env import env_bool

DEFAULT_REPORT_INTERVAL_S = 5.0
NS_PER_SECOND = 1_000_000_000


class StreamStats:
    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        report_interval_s: float = DEFAULT_REPORT_INTERVAL_S,
    ) -> None:
        self.enabled = env_bool("ENABLE_STATS")
        self._name = name
        self._logger = logger
        self._report_interval_ns = int(report_interval_s * NS_PER_SECOND)
        now_ns = perf_counter_ns()
        self._window_started_at_ns = now_ns
        self._next_report_at_ns = now_ns + self._report_interval_ns
        self._reset()

    def record_raw(self) -> None:
        if not self.enabled:
            return
        self._raw += 1

    def record_pong(self) -> None:
        if not self.enabled:
            return
        self._pong += 1
        self._maybe_report()

    def record_bucket_drop(self) -> None:
        if not self.enabled:
            return
        self._bucket_drop += 1
        self._maybe_report()

    def record_parse_drop(self) -> None:
        if not self.enabled:
            return
        self._parse_drop += 1
        self._maybe_report()

    def record_filter_drop(self) -> None:
        if not self.enabled:
            return
        self._filter_drop += 1
        self._maybe_report()

    def record_build_drop(self) -> None:
        if not self.enabled:
            return
        self._build_drop += 1
        self._maybe_report()

    def record_event(self) -> None:
        if not self.enabled:
            return
        self._event += 1
        self._maybe_report()

    def _maybe_report(self) -> None:
        now_ns = perf_counter_ns()
        if now_ns < self._next_report_at_ns:
            return
        self._report(now_ns)
        self._reset()
        self._window_started_at_ns = now_ns
        self._next_report_at_ns = now_ns + self._report_interval_ns

    def _report(self, now_ns: int) -> None:
        window_s = (now_ns - self._window_started_at_ns) / NS_PER_SECOND
        if window_s <= 0:
            return
        tracked_raw = max(0, self._raw - self._pong)
        dropped = self._bucket_drop + self._parse_drop + self._filter_drop + self._build_drop
        drop_pct = dropped / tracked_raw * 100 if tracked_raw else 0.0
        raw_interval_ms = window_s * 1000 / self._raw if self._raw else 0.0
        event_interval_ms = window_s * 1000 / self._event if self._event else 0.0
        self._logger.info(
            (
                "%s raw stats: raw=%d raw/s=%.1f row/intv=%.3fms "
                "event=%d event/s=%.1f event/intv=%.3fms"
            ),
            self._name,
            self._raw,
            self._raw / window_s,
            raw_interval_ms,
            self._event,
            self._event / window_s,
            event_interval_ms,
        )
        self._logger.info(
            (
                "%s raw stats: drop=%.1f%% pong=%d bucket_drop=%d parse_drop=%d "
                "filter_drop=%d build_drop=%d"
            ),
            self._name,
            drop_pct,
            self._pong,
            self._bucket_drop,
            self._parse_drop,
            self._filter_drop,
            self._build_drop,
        )

    def _reset(self) -> None:
        self._raw = 0
        self._pong = 0
        self._bucket_drop = 0
        self._parse_drop = 0
        self._filter_drop = 0
        self._build_drop = 0
        self._event = 0
