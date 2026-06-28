from pydantic import BaseModel

from domain import PeakEvent


class RecentPeak(BaseModel):
    household: str
    room: str
    datetime: str
    level_kwh: float


class TimelinePoint(BaseModel):
    t: int
    count: int


class PeaksResponse(BaseModel):
    total: int
    peaks_per_min: int
    max_level: float
    per_room: dict[str, int]
    per_household: dict[str, int]
    by_type: dict[str, int]
    by_hour: list[int]
    timeline: list[TimelinePoint]
    recent: list[RecentPeak]


def to_recent(event: PeakEvent) -> RecentPeak:
    return RecentPeak(
        household=event.household,
        room=event.room,
        datetime=event.datetime,
        level_kwh=round(event.level, 4),
    )
