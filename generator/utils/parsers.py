from datetime import UTC, datetime
import re

from models import HouseholdReading


_ROOM_NAMES = (
    "kitchen|livingroom|bedroom|warehouse|office|production|lobby|cafeteria|hall"
)
COLUMN_RE = re.compile(
    rf"^DE_KN_((?:residential|industrial|public)\d+)_({_ROOM_NAMES})_(grid_import)$"
)
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"


def parse_ts(raw: str) -> str | None:
    try:
        dt = datetime.strptime(raw, TIMESTAMP_FMT).replace(tzinfo=UTC)
        return dt.isoformat()
    except ValueError:
        return None


def parse_reading(column: str, value: str, iso_ts: str) -> HouseholdReading | None:
    """One CSV cell -> HouseholdReading; None if the column isn't a meter or the value is bad."""
    m = COLUMN_RE.match(column)
    if not m or not value:
        return None
    try:
        value = float(value)
    except ValueError:
        return None
    return HouseholdReading(
        household=m.group(1),
        room=m.group(2),
        feed=m.group(3),
        utc_timestamp=iso_ts,
        value_kwh=value,
    )
