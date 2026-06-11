import json
from dataclasses import asdict, dataclass


@dataclass
class HouseholdReading:
    household: str
    feed: str
    utc_timestamp: str  # ISO-8601 UTC
    value_kwh: float


def to_json_bytes(reading: HouseholdReading) -> bytes:
    return json.dumps(asdict(reading)).encode("utf-8")
