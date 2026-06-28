import threading
import time
from collections import deque

from domain import PeakEvent

StampedEvent = tuple[float, PeakEvent]


class PeakStore:
    def __init__(self, maxlen: int) -> None:
        self._events: deque[StampedEvent] = deque(maxlen=maxlen)
        self._total_seen = 0
        self._lock = threading.Lock()

    def add(self, event: PeakEvent) -> None:
        with self._lock:
            self._events.append((time.time(), event))
            self._total_seen += 1

    def snapshot(self) -> tuple[list[StampedEvent], int]:
        """Return the retained window and the lifetime count seen so far.

        The deque is bounded, so ``len(window)`` caps at ``maxlen``; the
        monotonic counter keeps the true lifetime total regardless of eviction.
        """
        with self._lock:
            return list(self._events), self._total_seen
