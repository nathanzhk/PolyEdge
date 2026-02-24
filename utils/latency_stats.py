from __future__ import annotations

import logging
from time import perf_counter_ns

DEFAULT_REPORT_INTERVAL_S = 5.0
NS_PER_SECOND = 1_000_000_000
NS_PER_MS = 1_000_000


class LatencyStats:
    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        report_interval_s: float = DEFAULT_REPORT_INTERVAL_S,
    ) -> None:
        self._name = name
        self._logger = logger
        self._samples_ms: list[float] = []
        self._report_interval_ns = int(report_interval_s * NS_PER_SECOND)
        now_ns = perf_counter_ns()
        self._window_started_at_ns = now_ns
        self._next_report_at_ns = now_ns + self._report_interval_ns

    def record_ns(self, started_at_ns: int) -> None:
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
        window_s = (now_ns - self._window_started_at_ns) / NS_PER_SECOND
        msg_interval_ms = window_s * 1000 / count
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


def _percentile_index(count: int, percentile: float) -> int:
    return min(count - 1, max(0, int((count - 1) * percentile)))
