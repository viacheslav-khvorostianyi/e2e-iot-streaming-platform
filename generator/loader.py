from collections.abc import Iterator
import csv
from pathlib import Path

from models import HouseholdReading
from utils.parsers import parse_reading, parse_ts


def readings_from_csv(data_source: str) -> Iterator[tuple[HouseholdReading, str]]:
    """Yield one HouseholdReading per non-null cell from a local CSV file."""
    with Path(data_source).open(newline="", encoding="utf-8") as lines:
        for row in csv.DictReader(lines):
            iso_ts = parse_ts(row["utc_timestamp"])
            if iso_ts is None:
                continue
            for column, value in row.items():
                reading = parse_reading(column=column, value=value, iso_ts=iso_ts)
                if reading is None:
                    continue
                yield reading, f"{reading.household}:{reading.room}"
