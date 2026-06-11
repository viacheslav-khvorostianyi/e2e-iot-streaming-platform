#!/usr/bin/env python3
"""
Generate synthetic household grid_import readings with injected anomalies.
Output is a CSV compatible with the existing loader format.

Usage:
    python data/generate_synthetic.py
    python data/generate_synthetic.py --rows 50000 --anomaly-rate 0.03
    python data/generate_synthetic.py --output data/household/synthetic.csv --seed 0
"""
import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

GRID_IMPORT_COLS = [
    "DE_KN_industrial1_grid_import",
    "DE_KN_industrial2_grid_import",
    "DE_KN_industrial3_grid_import",
    "DE_KN_public1_grid_import",
    "DE_KN_public2_grid_import",
    "DE_KN_residential1_grid_import",
    "DE_KN_residential2_grid_import",
    "DE_KN_residential3_grid_import",
    "DE_KN_residential4_grid_import",
    "DE_KN_residential5_grid_import",
    "DE_KN_residential6_grid_import",
]

# (mean, std) per column in kWh/min, chosen to produce realistic household profiles
PROFILES: dict[str, tuple[float, float]] = {
    "DE_KN_industrial1_grid_import":  (110.0, 55.0),
    "DE_KN_industrial2_grid_import":  ( 85.0, 40.0),
    "DE_KN_industrial3_grid_import":  ( 95.0, 50.0),
    "DE_KN_public1_grid_import":      ( 14.0,  7.0),
    "DE_KN_public2_grid_import":      ( 11.0,  5.5),
    "DE_KN_residential1_grid_import": (  0.30, 0.22),
    "DE_KN_residential2_grid_import": (  0.38, 0.28),
    "DE_KN_residential3_grid_import": (  0.33, 0.24),
    "DE_KN_residential4_grid_import": (  0.42, 0.30),
    "DE_KN_residential5_grid_import": (  0.28, 0.20),
    "DE_KN_residential6_grid_import": (  0.36, 0.26),
}


def _normal(col: str, rng: random.Random) -> float:
    mean, std = PROFILES[col]
    return max(0.0, rng.gauss(mean, std))


def _anomaly(col: str, rng: random.Random) -> float:
    mean, std = PROFILES[col]
    # For a Gaussian, Q3 ≈ mean + 0.674·std and IQR ≈ 1.349·std, so the
    # detection fence at sigma=0.5 is ≈ mean + 1.35·std.  Place anomalies
    # at mean + (2.5 … 4.0)·std so they are clearly above the fence.
    return mean + rng.uniform(2.5, 4.0) * std


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=10_000, help="Number of 1-minute rows (default: 10000)")
    parser.add_argument("--anomaly-rate", type=float, default=0.02, help="Fraction of readings injected as anomalies (default: 0.02)")
    parser.add_argument("--output", default="data/household/synthetic_data.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["utc_timestamp", "cet_cest_timestamp", *GRID_IMPORT_COLS]
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    cet_offset = timedelta(hours=1)

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
                if rng.random() < args.anomaly_rate:
                    row[col] = f"{_anomaly(col, rng):.3f}"
                    anomaly_count += 1
                else:
                    row[col] = f"{_normal(col, rng):.3f}"
            writer.writerow(row)

    total = args.rows * len(GRID_IMPORT_COLS)
    print(f"rows:      {args.rows:,}")
    print(f"readings:  {total:,}  ({len(GRID_IMPORT_COLS)} households × {args.rows:,} rows)")
    print(f"anomalies: {anomaly_count} ({100 * anomaly_count / total:.1f}%)")
    print(f"output:    {out_path}")


if __name__ == "__main__":
    main()
