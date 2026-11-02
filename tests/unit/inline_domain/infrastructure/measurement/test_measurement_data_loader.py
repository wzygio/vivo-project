from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.shared.measurement_data_loader import (
    RAW_MEASUREMENT_COLUMNS,
    load_raw_measurements,
)


def _database() -> SimpleNamespace:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        for table_name, id_column, time_column in (
            ("spc_tzbjx_array", "sheet_id", "sheet_start_time"),
            ("spc_tzbjx_oled", "glass_id", "glass_start_time"),
            ("spc_tzbjx_tsp", "glass_id", "glass_start_time"),
        ):
            connection.execute(
                text(
                    f"CREATE TABLE eda.{table_name} ("
                    "product_spec TEXT, lot_id TEXT, "
                    f"{id_column} TEXT, {time_column} TEXT, "
                    "step_id TEXT, param_name TEXT, site_name TEXT, "
                    "unit_id TEXT, param_value TEXT)"
                )
            )
        connection.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_productspec ("
                "productspecname TEXT, productcode TEXT)"
            )
        )
        connection.execute(
            text("INSERT INTO mdw.dwr_mes_productspec VALUES ('SPEC-M678', 'M678')")
        )
        connection.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_array VALUES "
                "('SPEC-M678', 'LOT-A', 'SHEET-A', '2026-08-01 08:00:00', "
                "'11620', 'TDSUM', 'S1', 'EQ-A', '3')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_oled VALUES "
                "('SPEC-M678', 'LOT-O', 'GLASS-O', '2026-08-02 09:00:00', "
                "'21320', 'DSUM_L', 'S2', 'EQ-O', '4')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_tsp VALUES "
                "('SPEC-M678', 'LOT-T', 'GLASS-T', '2026-08-03 10:00:00', "
                "'43620', 'TDSUM', 'S3', 'EQ-T', '5')"
            )
        )
        connection.execute(text("ALTER TABLE eda.spc_tzbjx_oled ADD COLUMN ppid TEXT"))
        connection.execute(text("CREATE TABLE eda.IMP_SPC_TZBJX_MQC (ppid TEXT, prodcode TEXT)"))
    return SimpleNamespace(engine=engine)


@pytest.fixture
def database():
    db = _database()
    try:
        yield db
    finally:
        db.engine.dispose()


def test_load_raw_measurements_unifies_three_factories_and_preserves_lot(database) -> None:
    result = load_raw_measurements(
        database,
        start_date="2026-08-01",
        end_date="2026-08-10",
        prod_code="M678",
    )

    assert list(result.columns) == RAW_MEASUREMENT_COLUMNS
    assert set(result["factory"]) == {"ARRAY", "OLED", "TP"}
    assert set(result["lot_id"]) == {"LOT-A", "LOT-O", "LOT-T"}
    assert set(result["sheet_id"]) == {"SHEET-A", "GLASS-O", "GLASS-T"}


def _insert_oled(database, *, sheet, ppid, spec="OLED-MQC-THK", step="2120A", date="2026-08-02 09:00:00"):
    with database.engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO eda.spc_tzbjx_oled "
            "(product_spec, lot_id, glass_id, glass_start_time, step_id, param_name, site_name, unit_id, param_value, ppid) "
            "VALUES (:spec, 'LOT-MQC', :sheet, :date, :step, 'SE_EV8_T1', '1', 'EQ-MQC', '42', :ppid)"
        ), dict(spec=spec, sheet=sheet, date=date, step=step, ppid=ppid))


def _load(database, product):
    return load_raw_measurements(database, "2026-08-01", "2026-08-10", product)


def test_2120a_uses_ppid_product_even_when_spec_is_unknown_and_mapping_is_duplicated(database):
    with database.engine.begin() as connection:
        connection.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('OLED-MQC-THK', 'MQC')"))
        connection.execute(text(
            "INSERT INTO eda.IMP_SPC_TZBJX_MQC VALUES ('135', 'M678'), ('135', 'M678'), ('136', 'M678')"
        ))
    _insert_oled(database, sheet="MQC-MAPPED", ppid="135")
    _insert_oled(database, sheet="NO-SPEC", ppid="136", spec="UNKNOWN")

    result = _load(database, "M678")
    mapped = result[result["step_id"].eq("2120A")]

    assert set(mapped["sheet_id"]) == {"MQC-MAPPED", "NO-SPEC"}
    assert len(mapped) == 2
    assert mapped["prod_code"].tolist() == ["M678", "M678"]
    assert mapped["param_value"].tolist() == [42, 42]
    assert list(result.columns) == RAW_MEASUREMENT_COLUMNS
    assert _load(database, "MQC").empty


def test_2120a_never_falls_back_to_product_spec_or_leaks_into_wrong_product(database):
    with database.engine.begin() as connection:
        connection.execute(text("INSERT INTO eda.IMP_SPC_TZBJX_MQC VALUES ('135', 'Z553')"))
    _insert_oled(database, sheet="REDIRECTED", ppid="135", spec="SPEC-M678")
    _insert_oled(database, sheet="UNMAPPED", ppid="missing", spec="SPEC-M678")
    _insert_oled(database, sheet="NULL-PPID", ppid=None, spec="SPEC-M678")

    assert _load(database, "M678").query("step_id == '2120A'").empty
    redirected = _load(database, "Z553")
    assert redirected["sheet_id"].tolist() == ["REDIRECTED"]
    assert redirected["prod_code"].tolist() == ["Z553"]


def test_other_stations_keep_spec_mapping_and_2120a_keeps_date_window(database):
    with database.engine.begin() as connection:
        connection.execute(text("INSERT INTO eda.IMP_SPC_TZBJX_MQC VALUES ('135', 'Z553')"))
    _insert_oled(database, sheet="OTHER-STATION", ppid="135", spec="SPEC-M678", step="21200")
    _insert_oled(database, sheet="TOO-EARLY", ppid="135", date="2026-07-31 23:59:59")
    _insert_oled(database, sheet="TOO-LATE", ppid="135", date="2026-08-11 00:00:00")
    _insert_oled(database, sheet="IN-WINDOW", ppid="135", date="2026-08-10 23:59:59")

    original = _load(database, "M678")
    assert "OTHER-STATION" in set(original["sheet_id"])
    assert _load(database, "Z553")["sheet_id"].tolist() == ["IN-WINDOW"]
