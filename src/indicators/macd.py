from __future__ import annotations

from dataclasses import dataclass

from indicators.indicator import MacdValue


@dataclass(slots=True)
class _Ema:
    period: int
    _count: int = 0
    _sum: float = 0.0
    _value: float | None = None

    def update(self, price: float) -> float | None:
        self._count += 1

        if self._value is None:
            self._sum += price
            if self._count < self.period:
                return None
            self._value = self._sum / self.period
            return self._value

        multiplier = 2.0 / (self.period + 1.0)
        self._value = ((price - self._value) * multiplier) + self._value
        return self._value


class MacdIndicator:
    def __init__(
        self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
    ) -> None:
        if fast_period <= 0:
            raise ValueError("fast_period must be positive")
        if slow_period <= 0:
            raise ValueError("slow_period must be positive")
        if signal_period <= 0:
            raise ValueError("signal_period must be positive")
        if fast_period >= slow_period:
            raise ValueError("fast_period must be less than slow_period")

        self._fast_ema = _Ema(fast_period)
        self._slow_ema = _Ema(slow_period)
        self._signal_ema = _Ema(signal_period)

    def calc(self, close: float) -> MacdValue | None:
        fast = self._fast_ema.update(close)
        slow = self._slow_ema.update(close)
        if fast is None or slow is None:
            return None

        value = fast - slow
        signal = self._signal_ema.update(value)
        if signal is None:
            return None

        return MacdValue(
            value=value,
            signal=signal,
            histogram=value - signal,
        )
