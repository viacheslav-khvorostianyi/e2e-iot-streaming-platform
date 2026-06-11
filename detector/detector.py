from collections import deque

import numpy as np
import structlog

log = structlog.get_logger()


class IQRDetector:
    """
    Sliding-window IQR outlier detector, one window per household.

    Upper Tukey fence: Q3 + sigma * IQR.
    Evaluate-then-append: the current value is compared against the fence
    derived from the existing window before being added, so an outlier
    cannot inflate its own detection threshold.
    """

    def __init__(self, window_size: int, min_window: int, sigma: float) -> None:
        self._window_size = window_size
        self._min_window = min_window
        self._sigma = sigma
        self._windows: dict[str, deque[float]] = {}

    def _get_window(self, household: str) -> deque[float]:
        if household not in self._windows:
            self._windows[household] = deque(maxlen=self._window_size)
        return self._windows[household]

    def detect(self, household: str, value: float) -> bool:
        window = self._get_window(household)

        is_peak = False
        if len(window) >= self._min_window:
            arr = np.array(window, dtype=np.float64)
            q1, q3 = np.percentile(arr, [25, 75])
            iqr = q3 - q1
            upper_fence = q3 + self._sigma * iqr

            if value > upper_fence:
                is_peak = True
                log.debug(
                    "peak_detected",
                    household=household,
                    value=round(value, 6),
                    upper_fence=round(upper_fence, 6),
                    q1=round(q1, 6),
                    q3=round(q3, 6),
                    iqr=round(iqr, 6),
                )

        window.append(value)
        return is_peak
