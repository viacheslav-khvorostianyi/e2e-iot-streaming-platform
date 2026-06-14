from pydantic import BaseModel

from domain import PeakEvent, level


class RecentPeak(BaseModel):
    room: str
    datetime: str
    value_kwh: float
    upper_fence: float
    level_kwh: float


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
    )
