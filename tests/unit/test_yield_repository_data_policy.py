from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError
from sqlalchemy import bindparam, create_engine, text

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy

from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.infrastructure import data_loader
from src.yield_domain.infrastructure.report_data_adapter import YieldReportDataAdapter
from src.yield_domain.infrastructure.repositories.yield_repository import (
    PanelRepository,
    build_yield_snapshot_path,
)


def test_yield_query_rejects_static_data_policy_fields() -> None:
    with pytest.raises(ValidationError):
        YieldQueryConfig(
            start_date="2026-07-01",
            end_date="2026-07-01",
            product_code="M678",
            work_order_types=["ESLC"],
            target_defect_groups=["Array_Line"],
        )


def test_work_order_policy_participates_in_snapshot_identity(tmp_path) -> None:
    first_policy = YieldDataPolicy(
        work_order_types=("ESLC", "P"),
        target_defect_groups=("Array_Line",),
    )
    second_policy = YieldDataPolicy(
        work_order_types=("LCFG",),
        target_defect_groups=("Array_Line",),
    )

    first_path = build_yield_snapshot_path(tmp_path, "M678", first_policy)
    second_path = build_yield_snapshot_path(tmp_path, "M678", second_policy)

    assert first_path != second_path
    assert first_path.parent == tmp_path / "yield_domain" / "yield"
    assert first_policy.signature in first_path.name
    assert second_policy.signature in second_path.name


def test_z622_report_includes_additional_work_orders_only_for_configured_lots(
    tmp_path, monkeypatch,
) -> None:
    config = ConfigLoader.load_config("Z622")
    policy = YieldDataPolicy.from_app_config(config)
    standard_only = YieldDataPolicy(
        work_order_types=policy.work_order_types,
        target_defect_groups=policy.target_defect_groups,
    )
    assert build_yield_snapshot_path(tmp_path, "Z622", policy) != (
        build_yield_snapshot_path(tmp_path, "Z622", standard_only)
    )

    monkeypatch.setattr(
        ConfigLoader, "get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=False),
    )
    fixture_sql = """
    WITH dwt_warehousing_pnl(panel_id, prod_id, first_ship_date, first_ship_time,
                             last_flag, sub_prod_type) AS (VALUES
        ('L3ZB6BE030101', 'Z_SPEC', '20261001', '20261001100000', 'Y', 'XMSY'),
        ('L3ZB6BE040101', 'Z_SPEC', '20261002', '20261002100000', 'Y', 'XPSY'),
        ('L3ZB6BE050101', 'Z_SPEC', '20261001', '20261001100000', 'Y', 'XMSY'),
        ('L3ZB6BE060101', 'Z_SPEC', '20261001', '20261001100000', 'Y', 'P'),
        ('L3ZB6BE030102', 'Z_SPEC', '20260930', '20260930100000', 'Y', 'XMSY'),
        ('L3ZB6BE040102', 'Z_SPEC', '20261003', '20261003100000', 'Y', 'XPSY'),
        ('L3ZB6BE030103', 'Z_SPEC', '20261001', '20261001100000', 'N', 'XMSY'),
        ('L3ZB6BE040103', 'Z_SPEC', '20261001', '20261001100000', 'Y', 'OTHER'),
        ('L3ZB6BE040104', 'M_SPEC', '20261001', '20261001100000', 'Y', 'XPSY'),
        ('L3ZB6BE060102', 'M_SPEC', '20261001', '20261001100000', 'Y', 'P')
    ), dwr_mes_productspec(productspecname, productcode) AS (VALUES
        ('Z_SPEC', 'Z622'), ('M_SPEC', 'M626')
    ), dws_dft_warehousing_d(panel_id, defect_code) AS (SELECT NULL, NULL WHERE FALSE),
    imp_ct_dft_group(defect_code, defect_desc, defect_group) AS (
        SELECT NULL, NULL, NULL WHERE FALSE
    ), spot_glass_batch_info(productcode, lot, glass_id, timekey) AS (
        SELECT NULL, NULL, NULL, NULL WHERE FALSE
    )
    """
    read_sql = pd.read_sql

    def read_fixture(query, engine, *, params):
        assert (params["start_date"], params["end_date"]) == ("20261001", "20261002")
        fixture_query = query.text.replace("mdw.spot_glass_batch_info", "spot_glass_batch_info")
        sql = text(fixture_sql + fixture_query)
        expanding = [
            bindparam(key, expanding=True) for key, value in params.items()
            if isinstance(value, list)
        ]
        return read_sql(sql.bindparams(*expanding), engine, params=params)

    monkeypatch.setattr(data_loader.pd, "read_sql", read_fixture)
    engine = create_engine("sqlite://")
    try:
        adapter = YieldReportDataAdapter(SimpleNamespace(engine=engine), tmp_path)
        query = YieldQueryConfig(
            start_date="2026-10-01", end_date="2026-10-02", product_code="Z622",
        )
        z622 = adapter.read_panel(query, policy)
        m626 = adapter.read_panel(
            query.model_copy(update={"product_code": "M626"}),
            YieldDataPolicy.from_app_config(ConfigLoader.load_config("M626")),
        )
    finally:
        engine.dispose()

    assert sorted(z622["panel_id"]) == ["L3ZB6BE030101", "L3ZB6BE040101", "L3ZB6BE060101"]
    assert m626["panel_id"].tolist() == ["L3ZB6BE060102"]


def test_repository_applies_injected_defect_group_policy_to_snapshot_data(
    tmp_path, monkeypatch,
) -> None:
    # This fixture deliberately models a four-day display shift; do not depend
    # on the user's mutable production configuration.
    monkeypatch.setattr(
        ConfigLoader, "get_data_forward_policy",
        lambda: DataForwardPolicy(enabled=True, offset_days=4),
    )
    snapshot_path = tmp_path / "yield_snapshot.parquet"
    pd.DataFrame(
        [
            {
                "warehousing_time": "2026-06-27",
                "panel_id": "PANEL-1",
                "defect_code": "LINE",
                "defect_desc": "亮线",
                "defect_group": "Array_Line",
            },
            {
                "warehousing_time": "2026-06-27",
                "panel_id": "PANEL-2",
                "defect_code": "OTHER",
                "defect_desc": "非目标缺陷",
                "defect_group": "Other_Group",
            },
        ]
    ).to_parquet(snapshot_path, index=False)

    policy = YieldDataPolicy(
        work_order_types=("ESLC", "P"),
        target_defect_groups=("Array_Line",),
    )
    repository = PanelRepository(
        snapshot_path=snapshot_path,
        data_policy=policy,
        db_manager=SimpleNamespace(engine=None),
    )
    query = YieldQueryConfig(
        start_date="2026-06-01",
        end_date="2026-07-01",
        product_code="M678",
    )

    result = repository.get_panel_details(query)

    assert result["warehousing_time"].eq(pd.Timestamp("2026-07-01")).all()
    assert result.loc[result["panel_id"] == "PANEL-1", "defect_group"].item() == "Array_Line"
    non_target = result.loc[result["panel_id"] == "PANEL-2"].iloc[0]
    assert pd.isna(non_target["defect_code"])
    assert pd.isna(non_target["defect_desc"])
    assert pd.isna(non_target["defect_group"])


def test_repository_persists_raw_defects_before_applying_policy(
    tmp_path, monkeypatch
) -> None:
    snapshot_path = tmp_path / "yield_snapshot.parquet"

    def fake_read_sql(sql_query, engine, params=None) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "batch_no": "2026/07/01",
                    "lot_id": "LOT-1",
                    "sheet_id": "SHEET-1",
                    "panel_id": "PANEL-1",
                    "warehousing_time": "2026-06-27",
                    "prod_code": "M678",
                    "defect_code": "OTHER",
                    "defect_desc": "非目标缺陷",
                    "defect_group": "Other_Group",
                }
            ]
        )

    monkeypatch.setattr(data_loader.pd, "read_sql", fake_read_sql)
    policy = YieldDataPolicy(
        work_order_types=("ESLC", "P"),
        target_defect_groups=("Array_Line",),
    )
    repository = PanelRepository(
        snapshot_path=snapshot_path,
        data_policy=policy,
        db_manager=SimpleNamespace(engine=object()),
    )
    query = YieldQueryConfig(
        start_date="2026-06-01",
        end_date="2026-07-01",
        product_code="M678",
    )

    result = repository.get_panel_details(query)

    assert pd.isna(result.loc[0, "defect_group"])
    persisted = pd.read_parquet(snapshot_path)
    assert persisted.loc[0, "warehousing_time"] == pd.Timestamp("2026-06-27")
    assert persisted.loc[0, "defect_code"] == "OTHER"
    assert persisted.loc[0, "defect_desc"] == "非目标缺陷"
    assert persisted.loc[0, "defect_group"] == "Other_Group"
