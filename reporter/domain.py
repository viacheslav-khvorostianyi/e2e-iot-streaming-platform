from dataclasses import dataclass
import re


_TYPE_RE = re.compile(r"^([a-zA-Z]+)")


@dataclass(frozen=True)
class PeakEvent:
    household: str
    room: str
    datetime: str
    level: float


def parse_peak(payload: dict) -> PeakEvent:
    return PeakEvent(
        household=payload["household"],
        room=payload["room"],
        datetime=payload["datetime"],
        level=float(payload["level"]),
    )


def building_type(household: str) -> str:
    match = _TYPE_RE.match(household)
    return match.group(1) if match else "other"
