from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.infrastructure import data_loader
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
    assert first_policy.signature in first_path.name
    assert second_policy.signature in second_path.name


def test_repository_applies_injected_defect_group_policy_to_snapshot_data(
    tmp_path,
) -> None:
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

    def fake_read_sql(sql_query, engine) -> pd.DataFrame:
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
