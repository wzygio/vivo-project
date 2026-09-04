from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import create_engine, text

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.infrastructure.aoi_tt.particle_size_loader import (
    PARTICLE_SIZE_COUNT_COLUMNS,
    load_particle_size_counts,
)
from src.shared_kernel.data_forward import DataForwardPolicy


def _database() -> SimpleNamespace:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        connection.execute(
            text(
                "CREATE TABLE eda.spc_tzbjx_array ("
                "product_spec TEXT, sheet_id TEXT, sheet_start_time TEXT, "
                "step_id TEXT, param_name TEXT, param_value INTEGER)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE eda.ARRAY_DEFECT_T ("
                "glass_id TEXT, step_id TEXT, item51 TEXT, item119 TEXT, "
                "glass_start_time TEXT)"
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
        # 同一 Sheet 在 SPC 中有多条测量行；产品映射不去重会把每个 defect 放大 2 倍。
        connection.execute(
            text(
                "INSERT INTO eda.spc_tzbjx_array VALUES "
                "('SPEC-M678', 'SHEET-1', '2026-08-01 08:00:00', '11620', 'TDSUM', 4),"
                "('SPEC-M678', 'SHEET-1', '2026-08-01 08:00:00', '11620', 'OTHER', 9),"
                "('SPEC-M678', 'SHEET-2', '2026-08-02 08:00:00', '11620', 'TDSUM', 1)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.ARRAY_DEFECT_T VALUES "
                "('SHEET-1', '11620', 'AOI', 'O', '2026-08-01 08:01:00'),"
                "('SHEET-1', '11620', 'AOI', ' o ', '2026-08-01 08:02:00'),"
                "('SHEET-1', '11620', 'AOI', 'L', '2026-08-01 08:03:00'),"
                "('SHEET-1', '11620', 'AOI', 'M', '2026-08-01 08:04:00'),"
                "('SHEET-1', '11620', 'RS',  'O', '2026-08-01 08:05:00'),"
                "('SHEET-2', '11620', 'AOI', 'L', '2026-08-02 08:01:00')"
            )
        )
    return SimpleNamespace(engine=engine)


def test_load_particle_size_counts_uses_unique_sheet_product_mapping() -> None:
    query = AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-08-01",
        end_date="2026-08-10",
    )

    result = load_particle_size_counts(
        _database(),
        query,
        data_forward_policy=DataForwardPolicy(enabled=False),
    )

    assert list(result.columns) == PARTICLE_SIZE_COUNT_COLUMNS
    assert result.to_dict("records") == [
        {
            "factory": "ARRAY",
            "prod_code": "M678",
            "start_time": result.loc[0, "start_time"],
            "sheet_id": "SHEET-1",
            "step_id": "11620",
            "particle_size": "L",
            "particle_qty": 1,
        },
        {
            "factory": "ARRAY",
            "prod_code": "M678",
            "start_time": result.loc[1, "start_time"],
            "sheet_id": "SHEET-1",
            "step_id": "11620",
            "particle_size": "O",
            "particle_qty": 2,
        },
        {
            "factory": "ARRAY",
            "prod_code": "M678",
            "start_time": result.loc[2, "start_time"],
            "sheet_id": "SHEET-2",
            "step_id": "11620",
            "particle_size": "L",
            "particle_qty": 1,
        },
    ]


def test_load_particle_size_counts_maps_source_time_to_display_time() -> None:
    query = AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-08-05",
        end_date="2026-08-10",
    )

    result = load_particle_size_counts(
        _database(),
        query,
        data_forward_policy=DataForwardPolicy(enabled=True, offset_days=4),
    )

    assert not result.empty
    assert result["start_time"].min().date().isoformat() == "2026-08-05"


def test_load_particle_size_counts_returns_empty_outside_array_tdsum() -> None:
    query = AoiTtQueryConfig(
        prod_code="M678",
        start_date="2026-08-01",
        end_date="2026-08-10",
        factory="OLED",
    )

    result = load_particle_size_counts(
        SimpleNamespace(engine=None),
        query,
        data_forward_policy=DataForwardPolicy(enabled=False),
    )

    assert result.empty
    assert list(result.columns) == PARTICLE_SIZE_COUNT_COLUMNS
