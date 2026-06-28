import time
from collections import Counter
from datetime import datetime, timezone

from domain import building_type, level
from schemas import PeaksResponse, TimelinePoint, to_recent
from store import StampedEvent

TIMELINE_BUCKET_SEC = 5
RECENT_LIMIT = 50
PEAKS_WINDOW_SEC = 60
MS_PER_SEC = 1000
LEVEL_ROUND_DIGITS = 4


def aggregate(snapshot: list[StampedEvent], total_seen: int) -> PeaksResponse:
    events = [event for _, event in snapshot]
    per_room = Counter(event.room for event in events)
    by_type = Counter(building_type(event.room) for event in events)

    buckets: Counter[int] = Counter()
    by_hour = [0] * 24
    for timestamp, _ in snapshot:
        bucket = int(timestamp // TIMELINE_BUCKET_SEC) * TIMELINE_BUCKET_SEC
        buckets[bucket] += 1
        by_hour[datetime.fromtimestamp(timestamp, tz=timezone.utc).hour] += 1

    now = time.time()
    peaks_per_min = sum(1 for ts, _ in snapshot if ts >= now - PEAKS_WINDOW_SEC)

    return PeaksResponse(
        total=total_seen,
        peaks_per_min=peaks_per_min,
        max_level=round(max((level(e) for e in events), default=0.0), LEVEL_ROUND_DIGITS),
        per_room=dict(sorted(per_room.items())),
        by_type=dict(sorted(by_type.items())),
        by_hour=by_hour,
        timeline=[
            TimelinePoint(t=bucket * MS_PER_SEC, count=count)
            for bucket, count in sorted(buckets.items())
        ],
        recent=[to_recent(event) for _, event in snapshot[-RECENT_LIMIT:]][::-1],
    )