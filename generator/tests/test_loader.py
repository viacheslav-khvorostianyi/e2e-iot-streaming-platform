from conftest import FIELDNAMES, valid_row, write_csv
import pytest

from loader import readings_from_csv


def test_yields_one_reading_per_meter_cell(tmp_path):
    source = write_csv(tmp_path / "readings.csv", [valid_row()])

    readings = list(readings_from_csv(source))

    assert len(readings) == 5


def test_bad_value_cell_is_skipped_others_survive(tmp_path):
    # 5 meter cells, 1 unparsable -> 4 readings
    source = write_csv(
        tmp_path / "readings.csv",
        [valid_row(DE_KN_residential1_kitchen_grid_import="not-a-number")],
    )

    readings = list(readings_from_csv(source))

    assert len(readings) == 4
    assert all(r.room != "kitchen" for r, _ in readings)


def test_empty_cell_is_skipped(tmp_path):
    source = write_csv(
        tmp_path / "readings.csv",
        [valid_row(DE_KN_public1_lobby_grid_import="")],
    )

    readings = list(readings_from_csv(source))

    assert len(readings) == 4
    assert all(r.household != "public1" for r, _ in readings)


def test_row_with_bad_timestamp_is_skipped_entirely(tmp_path):
    source = write_csv(
        tmp_path / "readings.csv",
        [valid_row(utc_timestamp="not-a-date"), valid_row()],
    )

    readings = list(readings_from_csv(source))

    assert len(readings) == 5  # only the second row's cells


def test_reading_fields_and_key(tmp_path):
    source = write_csv(tmp_path / "readings.csv", [valid_row()])

    by_key = {key: reading for reading, key in readings_from_csv(source)}

    reading = by_key["residential1:kitchen"]
    assert reading.household == "residential1"
    assert reading.room == "kitchen"
    assert reading.feed == "grid_import"
    assert reading.value_kwh == pytest.approx(0.412)
    assert reading.utc_timestamp == "2026-07-12T14:00:00+00:00"


def test_non_meter_columns_are_ignored(tmp_path):
    # cet_cest_timestamp holds a non-empty value in every row and must never
    # be yielded as a reading; same for any unknown numeric column
    source = write_csv(
        tmp_path / "readings.csv",
        [valid_row(DE_KN_residential1_kitchen_pv="1.234")],
        fieldnames=[*FIELDNAMES, "DE_KN_residential1_kitchen_pv"],
    )

    readings = list(readings_from_csv(source))

    assert len(readings) == 5
    assert all(r.feed == "grid_import" for r, _ in readings)
