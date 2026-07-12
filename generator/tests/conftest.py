"""Shared CSV helpers for generator tests."""

import csv
from pathlib import Path


FIELDNAMES = [
    "utc_timestamp",
    "cet_cest_timestamp",
    "DE_KN_residential1_kitchen_grid_import",
    "DE_KN_residential1_bedroom_grid_import",
    "DE_KN_residential2_livingroom_grid_import",
    "DE_KN_industrial1_warehouse_grid_import",
    "DE_KN_public1_lobby_grid_import",
]

VALID_TS = "2026-07-12T14:00:00Z"


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] = FIELDNAMES) -> str:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def valid_row(**overrides) -> dict:
    row = {
        "utc_timestamp": VALID_TS,
        "cet_cest_timestamp": "2026-07-12T15:00:00+0100",
        "DE_KN_residential1_kitchen_grid_import": "0.412",
        "DE_KN_residential1_bedroom_grid_import": "0.120",
        "DE_KN_residential2_livingroom_grid_import": "0.305",
        "DE_KN_industrial1_warehouse_grid_import": "131.500",
        "DE_KN_public1_lobby_grid_import": "9.800",
    }
    row.update(overrides)
    return row
