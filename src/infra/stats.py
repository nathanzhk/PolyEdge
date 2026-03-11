from __future__ import annotations

import logging
from time import perf_counter_ns

from infra import Env

NS_PER_S = 1_000_000_000
NS_PER_MS = 1_000_000

DEFAULT_REPORT_INTERVAL_S = 5.0


class LatencyStats:
    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        report_interval_s: float = DEFAULT_REPORT_INTERVAL_S,
    ) -> None:
        self.enabled = Env.ENABLE_STATS
        self._name = name
        self._logger = logger
        self._samples_ms: list[float] = []
        self._report_interval_ns = int(report_interval_s * NS_PER_S)
        now_ns = perf_counter_ns()
        self._window_started_at_ns = now_ns
        self._next_report_at_ns = now_ns + self._report_interval_ns

    def record_ns(self, started_at_ns: int) -> None:
        if not self.enabled:
            return
        self._record_ms((perf_counter_ns() - started_at_ns) / NS_PER_MS)

    def _record_ms(self, elapsed_ms: float) -> None:
        self._samples_ms.append(elapsed_ms)
        now_ns = perf_counter_ns()
        if now_ns < self._next_report_at_ns:
            return
        self._report(now_ns)
        self._samples_ms.clear()
        self._window_started_at_ns = now_ns
        self._next_report_at_ns = now_ns + self._report_interval_ns

    def _report(self, now_ns: int) -> None:
        if not self._samples_ms:
            return
        samples = sorted(self._samples_ms)
        count = len(samples)
        window_ns = now_ns - self._window_started_at_ns
        if window_ns <= 0:
            return
        window_s = window_ns / NS_PER_S
        window_ms = window_ns / NS_PER_MS
        msg_interval_ms = window_ms / count
        avg_ms = sum(samples) / count
        p95_ms = samples[_percentile_index(count, 0.95)]
        p99_ms = samples[_percentile_index(count, 0.99)]
        max_ms = samples[-1]
        self._logger.info(
            (
                "%s latency stats: cnt=%d cnt/s=%.1f intv=%.3fms "
                "avg=%.3fms p95=%.3fms p99=%.3fms max=%.3fms"
            ),
            self._name,
            count,
            count / window_s,
            msg_interval_ms,
            avg_ms,
            p95_ms,
            p99_ms,
            max_ms,
        )


class StreamStats:
    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        report_interval_s: float = DEFAULT_REPORT_INTERVAL_S,
    ) -> None:
        self.enabled = Env.ENABLE_STATS
        self._name = name
        self._logger = logger
        self._report_interval_ns = int(report_interval_s * NS_PER_S)
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
        window_ns = now_ns - self._window_started_at_ns
        if window_ns <= 0:
            return
        window_s = window_ns / NS_PER_S
        window_ms = window_ns / NS_PER_MS
        tracked_raw = max(0, self._raw - self._pong)
        dropped = self._bucket_drop + self._parse_drop + self._filter_drop + self._build_drop
        drop_pct = dropped / tracked_raw * 100 if tracked_raw else 0.0
        raw_interval_ms = window_ms / self._raw if self._raw else 0.0
        event_interval_ms = window_ms / self._event if self._event else 0.0
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


def _percentile_index(count: int, percentile: float) -> int:
    return min(count - 1, max(0, int((count - 1) * percentile)))
