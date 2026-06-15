import re
from dataclasses import dataclass

_TYPE_RE = re.compile(r"^([a-zA-Z]+)")


@dataclass(frozen=True)
class PeakEvent:
    room: str
    datetime: str
    value_kwh: float
    upper_fence: float


def parse_peak(payload: dict) -> PeakEvent:
    return PeakEvent(
        room=payload["room"],
        datetime=payload["datetime"],
        value_kwh=float(payload["value_kwh"]),
        upper_fence=float(payload["upper_fence"]),
    )


def level(event: PeakEvent) -> float:
    return event.value_kwh - event.upper_fence


def building_type(room: str) -> str:
    match = _TYPE_RE.match(room)
    return match.group(1) if match else "other"
