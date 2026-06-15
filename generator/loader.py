import csv
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from models import HouseholdReading

_ROOM_NAMES = "kitchen|livingroom|bedroom|warehouse|office|production|lobby|cafeteria|hall"
COLUMN_RE = re.compile(
    rf'^DE_KN_((?:residential|industrial|public)\d+)_({_ROOM_NAMES})_(grid_import)$'
)
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_ts(raw: str) -> str | None:
    try:
        dt = datetime.strptime(raw, TIMESTAMP_FMT).replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return None


@contextmanager
def _open_source(source: str):
    f = open(source, newline="", encoding="utf-8")
    try:
        yield f
    finally:
        f.close()


def iter_readings(data_source: str) -> Iterator[tuple[HouseholdReading, str]]:
    """Yield one HouseholdReading per non-null cell from a local CSV file."""
    with _open_source(data_source) as lines:
        for row in csv.DictReader(lines):
            iso_ts = _parse_ts(row["utc_timestamp"])
            if iso_ts is None:
                continue
            for col, val in row.items():
                m = COLUMN_RE.match(col)
                if not m or not val:
                    continue
                try:
                    value = float(val)
                except ValueError:
                    continue
                household = m.group(1)
                room = m.group(2)
                feed = m.group(3)
                yield HouseholdReading(
                    household=household,
                    room=room,
                    feed=feed,
                    utc_timestamp=iso_ts,
                    value_kwh=value,
                ), f"{household}:{room}"
