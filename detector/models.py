import json
from dataclasses import asdict, dataclass


@dataclass
class PeakEvent:
    household: str     # building identifier
    room: str          # room within the building
    datetime: str      # ISO-8601 UTC timestamp of the reading
    value_kwh: float   # reading that exceeded the upper Tukey fence
    upper_fence: float # Q3 + sigma * IQR at detection time


def to_json_bytes(event: PeakEvent) -> bytes:
    return json.dumps(asdict(event)).encode("utf-8")
