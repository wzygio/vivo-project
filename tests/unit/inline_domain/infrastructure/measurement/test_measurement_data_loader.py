from __future__ import annotations

from types import SimpleNamespace

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
    return SimpleNamespace(engine=engine)


def test_load_raw_measurements_unifies_three_factories_and_preserves_lot() -> None:
    result = load_raw_measurements(
        _database(),
        start_date="2026-08-01",
        end_date="2026-08-10",
        prod_code="M678",
    )

    assert list(result.columns) == RAW_MEASUREMENT_COLUMNS
    assert set(result["factory"]) == {"ARRAY", "OLED", "TP"}
    assert set(result["lot_id"]) == {"LOT-A", "LOT-O", "LOT-T"}
    assert set(result["sheet_id"]) == {"SHEET-A", "GLASS-O", "GLASS-T"}
