from dataclasses import asdict, dataclass
import json


@dataclass
class HouseholdReading:
    household: str
    room: str
    feed: str
    utc_timestamp: str  # ISO-8601 UTC
    value_kwh: float

    def to_json_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")
