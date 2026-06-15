import re
from dataclasses import dataclass

from pydantic import BaseModel

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


def severity(event: PeakEvent) -> float:
    return event.value_kwh / event.upper_fence if event.upper_fence else 0.0


def building_type(room: str) -> str:
    match = _TYPE_RE.match(room)
    return match.group(1) if match else "other"


class RecentPeak(BaseModel):
    room: str
    datetime: str
    value_kwh: float
    upper_fence: float
    level_kwh: float
    severity: float


class TimelinePoint(BaseModel):
    t: int
    count: int


class PeaksResponse(BaseModel):
    total: int
    peaks_per_min: int
    max_level: float
    per_room: dict[str, int]
    by_type: dict[str, int]
    by_hour: list[int]
    timeline: list[TimelinePoint]
    recent: list[RecentPeak]


def to_recent(event: PeakEvent) -> RecentPeak:
    return RecentPeak(
        room=event.room,
        datetime=event.datetime,
        value_kwh=round(event.value_kwh, 4),
        upper_fence=round(event.upper_fence, 4),
        level_kwh=round(level(event), 4),
        severity=round(severity(event), 2),
    )
