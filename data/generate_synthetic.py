#!/usr/bin/env python3
"""
Generate synthetic household grid_import readings with injected anomalies.
Output is a CSV compatible with the existing loader format.
Data spans [now - rows minutes, now] so the dashboard always looks current.

Usage:
    python data/generate_synthetic.py
    python data/generate_synthetic.py --rows 50000 --anomaly-rate 0.03
    python data/generate_synthetic.py --output data/household/synthetic.csv --seed 0
    python data/generate_synthetic.py --start-date 2026-01-01  # fixed start for reproducibility
"""
import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOUSEHOLDS = [
    "industrial1", "industrial2", "industrial3",
    "public1", "public2",
    "residential1", "residential2", "residential3",
    "residential4", "residential5", "residential6",
]

ROOMS: dict[str, list[str]] = {
    "industrial": ["warehouse", "office", "production"],
    "public":     ["lobby", "cafeteria", "hall"],
    "residential": ["kitchen", "livingroom", "bedroom"],
}

# (mean_kwh, std_kwh) per household+room — each room has a distinct consumption profile
PROFILES: dict[str, tuple[float, float]] = {
    # industrial: warehouse > production > office
    "industrial1_warehouse":   (130.0, 60.0),
    "industrial1_office":      ( 35.0, 15.0),
    "industrial1_production":  ( 90.0, 45.0),
    "industrial2_warehouse":   (100.0, 48.0),
    "industrial2_office":      ( 28.0, 12.0),
    "industrial2_production":  ( 72.0, 36.0),
    "industrial3_warehouse":   (115.0, 55.0),
    "industrial3_office":      ( 32.0, 14.0),
    "industrial3_production":  ( 80.0, 40.0),
    # public: cafeteria > lobby > hall
    "public1_lobby":           ( 10.0,  5.0),
    "public1_cafeteria":       ( 18.0,  8.0),
    "public1_hall":            (  7.0,  3.5),
    "public2_lobby":           (  8.0,  4.0),
    "public2_cafeteria":       ( 14.0,  6.0),
    "public2_hall":            (  5.5,  2.8),
    # residential: kitchen > livingroom > bedroom
    "residential1_kitchen":    ( 0.40, 0.28),
    "residential1_livingroom": ( 0.28, 0.18),
    "residential1_bedroom":    ( 0.12, 0.08),
    "residential2_kitchen":    ( 0.48, 0.32),
    "residential2_livingroom": ( 0.34, 0.22),
    "residential2_bedroom":    ( 0.15, 0.10),
    "residential3_kitchen":    ( 0.42, 0.30),
    "residential3_livingroom": ( 0.30, 0.20),
    "residential3_bedroom":    ( 0.13, 0.09),
    "residential4_kitchen":    ( 0.52, 0.36),
    "residential4_livingroom": ( 0.38, 0.25),
    "residential4_bedroom":    ( 0.16, 0.11),
    "residential5_kitchen":    ( 0.36, 0.24),
    "residential5_livingroom": ( 0.25, 0.16),
    "residential5_bedroom":    ( 0.11, 0.07),
    "residential6_kitchen":    ( 0.45, 0.31),
    "residential6_livingroom": ( 0.32, 0.21),
    "residential6_bedroom":    ( 0.14, 0.09),
}


def _household_type(household: str) -> str:
    for t in ("industrial", "public", "residential"):
        if household.startswith(t):
            return t
    raise ValueError(f"unknown household type: {household}")


def _build_cols() -> list[str]:
    cols = []
    for hh in HOUSEHOLDS:
        htype = _household_type(hh)
        for room in ROOMS[htype]:
            cols.append(f"DE_KN_{hh}_{room}_grid_import")
    return cols


GRID_IMPORT_COLS = _build_cols()


def _normal(col_key: str, rng: random.Random) -> float:
    mean, std = PROFILES[col_key]
    return max(0.0, rng.gauss(mean, std))


def _anomaly(col_key: str, rng: random.Random) -> float:
    mean, std = PROFILES[col_key]
    # Detector default SIGMA=1.5 → upper Tukey fence ≈ mean + 2.70·std.
    # Floor of 2.8 keeps injected anomalies clear of that fence.
    return mean + rng.uniform(2.8, 4.0) * std


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=10_000, help="Number of 1-minute rows (default: 10000)")
    parser.add_argument("--anomaly-rate", type=float, default=0.02, help="Fraction of readings injected as anomalies (default: 0.02)")
    parser.add_argument("--output", default="data/household/synthetic_data.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--start-date", default=None, help="Override start date (ISO format, e.g. 2026-01-01). Default: now minus rows minutes.")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.start_date:
        start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    else:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start = now - timedelta(minutes=args.rows)

    cet_offset = timedelta(hours=1)
    fieldnames = ["utc_timestamp", "cet_cest_timestamp", *GRID_IMPORT_COLS]
    anomaly_count = 0

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(args.rows):
            ts = start + timedelta(minutes=i)
            cet = ts + cet_offset
            row: dict[str, str] = {
                "utc_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "cet_cest_timestamp": cet.strftime("%Y-%m-%dT%H:%M:%S+0100"),
            }
            for col in GRID_IMPORT_COLS:
                # col = DE_KN_{household}_{room}_grid_import
                parts = col.split("_")
                # parts: ['DE', 'KN', household_type, household_num, room, 'grid', 'import']
                # easier: derive key from col name
                col_key = col[len("DE_KN_"):].removesuffix("_grid_import")
                if rng.random() < args.anomaly_rate:
                    row[col] = f"{_anomaly(col_key, rng):.3f}"
                    anomaly_count += 1
                else:
                    row[col] = f"{_normal(col_key, rng):.3f}"
            writer.writerow(row)

    total = args.rows * len(GRID_IMPORT_COLS)
    n_households = len(HOUSEHOLDS)
    n_rooms = len(GRID_IMPORT_COLS) // n_households
    print(f"rows:         {args.rows:,}")
    print(f"meters:       {len(GRID_IMPORT_COLS)}  ({n_households} households × {n_rooms} rooms)")
    print(f"readings:     {total:,}")
    print(f"anomalies:    {anomaly_count} ({100 * anomaly_count / total:.1f}%)")
    print(f"time range:   {start.isoformat()} → {(start + timedelta(minutes=args.rows - 1)).isoformat()}")
    print(f"output:       {out_path}")


if __name__ == "__main__":
    main()
