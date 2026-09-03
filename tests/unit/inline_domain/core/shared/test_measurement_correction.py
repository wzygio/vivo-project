"""Unit tests for SPC raw-measurement value corrections."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
)
from src.inline_domain.core.shared.measurement_correction import (
    apply_spc_value_corrections,
)


def _measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # 规则1：M673 + PPA + site 96（下边界）→ -5
            {"prod_code": "M673", "param_name": "PPA_X", "site_name": "96", "param_value": 12.5},
            # 规则1：M673 + PPA（小写）+ site 114（上边界），正常值同样修正
            {"prod_code": "M673", "param_name": "ppa y", "site_name": "114", "param_value": 3.0},
            # 规则2：M673 + PPA 但 site 越界（95）→ -1
            {"prod_code": "M673", "param_name": "PPA_X", "site_name": "95", "param_value": 10.0},
            # 规则2：M673 + PPA 但 site 越界（115）→ -1
            {"prod_code": "M673", "param_name": "PPA_X", "site_name": "115", "param_value": 10.0},
            # 不命中：param_name 不含 PPA
            {"prod_code": "M673", "param_name": "TDSUM", "site_name": "100", "param_value": 10.0},
            # 规则2：其它产品的 PPA → -1
            {"prod_code": "M678", "param_name": "PPA_X", "site_name": "100", "param_value": 10.0},
            # 规则2：site 非数值的 PPA → -1
            {"prod_code": "M673", "param_name": "PPA_X", "site_name": "N/A", "param_value": 10.0},
        ]
    )


def test_correction_shifts_m673_ppa_sites_96_to_114() -> None:
    result = apply_spc_value_corrections(_measurements())

    assert result.loc[0, "param_value"] == 7.5
    assert result.loc[1, "param_value"] == -2.0
    assert result.loc[2, "param_value"] == 9.0
    assert result.loc[3, "param_value"] == 9.0
    assert result.loc[4, "param_value"] == 10.0
    assert result.loc[5, "param_value"] == 9.0
    assert result.loc[6, "param_value"] == 9.0


def test_correction_does_not_mutate_input() -> None:
    source = _measurements()
    apply_spc_value_corrections(source)
    assert source.loc[0, "param_value"] == 12.5


def test_correction_passes_through_empty_frame() -> None:
    empty = pd.DataFrame(columns=["prod_code", "param_name", "site_name", "param_value"])
    assert apply_spc_value_corrections(empty).empty


def test_snapshot_repository_applies_corrector_before_writing_snapshot(tmp_path) -> None:
    raw = _measurements()

    def loader(_db, _start_date: str, _end_date: str, _prod_code: str) -> pd.DataFrame:
        return raw.copy()

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=tmp_path,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=loader,
        measurement_corrector=apply_spc_value_corrections,
    )

    returned = repository.get_measurements(prod_code="M673", end_date="2026-08-13")
    assert returned.loc[0, "param_value"] == 7.5

    snapshot = pd.read_parquet(tmp_path / "inline_measurements_M673.parquet")
    assert snapshot.loc[0, "param_value"] == 7.5
    assert snapshot.loc[2, "param_value"] == 9.0
    assert snapshot.loc[4, "param_value"] == 10.0
