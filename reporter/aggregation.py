from collections import Counter
from datetime import datetime

from domain import building_type, level
from schemas import PeaksResponse, to_recent
from store import StampedEvent

TIMELINE_BUCKET_SEC = 5
RECENT_LIMIT = 50


def aggregate(snapshot: list[StampedEvent], total_seen: int) -> PeaksResponse:
    events = [event for _, event in snapshot]
    levels = [level(event) for event in events]
    per_room = Counter(event.room for event in events)
    by_type = Counter(building_type(event.room) for event in events)

    buckets: dict[int, int] = {}
    by_hour = [0] * 24
    for timestamp, _ in snapshot:
        bucket = int(timestamp // TIMELINE_BUCKET_SEC * TIMELINE_BUCKET_SEC)
        buckets[bucket] = buckets.get(bucket, 0) + 1
        by_hour[datetime.fromtimestamp(timestamp).hour] += 1

    now = snapshot[-1][0] if snapshot else 0.0
    peaks_per_min = sum(1 for timestamp, _ in snapshot if timestamp >= now - 60)

    return PeaksResponse(
        total=total_seen,
        peaks_per_min=peaks_per_min,
        max_level=round(max(levels, default=0.0), 4),
        per_room=dict(sorted(per_room.items())),
        by_type=dict(sorted(by_type.items())),
        by_hour=by_hour,
        timeline=[
            {"t": bucket * 1000, "count": count}
            for bucket, count in sorted(buckets.items())
        ],
        recent=[to_recent(event) for _, event in snapshot[-RECENT_LIMIT:]][::-1],
    )
