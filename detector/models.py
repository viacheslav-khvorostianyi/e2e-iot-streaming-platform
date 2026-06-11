import json
from dataclasses import asdict, dataclass


@dataclass
class PeakEvent:
    room: str       # household identifier
    datetime: str   # ISO-8601 UTC timestamp of the reading
    level: float    # value_kwh that exceeded the upper Tukey fence


def to_json_bytes(event: PeakEvent) -> bytes:
    return json.dumps(asdict(event)).encode("utf-8")
