"""Characterization tests for the unified scope-driven decoration entry.

Replaces the per-module wrapper tests (former test_spc_data_decoration.py):
the same behaviours must hold through ``prepare_decorated_data(scope=...)``.
"""

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.shared import decorated_data
from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
from src.inline_domain.core.shared.sheet_oos_decoration import OOS_DECORATION_FILE_NAME


def _raw_measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_id": "S1",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 8.0,
                "data_type": "SPC",
            },
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_id": "S1",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "site_name": "P2",
                "unit_id": "3CEE01-PPA",
                "param_value": 0.0,
                "data_type": "SPC",
            },
        ]
    )


def _spec_limits() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "usl": 6.0,
                "lsl": -6.0,
                "ucl": 3.0,
                "lcl": -3.0,
                "target": 0.0,
            }
        ]
    )


def _write_flag_workbook(product_dir: Path, file_name: str, flag: object) -> None:
    """预写用户决策台账（<产品>__flags）；旧产品 sheet 的 flag 永远不再生效。"""
    product_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "flag": flag,
            }
        ]
    ).to_excel(product_dir / file_name, index=False, sheet_name="Z571__flags")


def test_prepare_decorated_data_clips_points_and_recomputes_sheet_features(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"

    result = prepare_decorated_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        scope="spc",
        product_dir=product_dir,
    )

    assert not (product_dir / "spc_sheet_oos_detail.xlsx").exists()
    assert result.sheet_oos_decoration_result.decoration_path.exists()
    assert result.sheet_oos_decoration_result.decoration_sheet == "Z571"
    assert result.raw_measurements_df["param_value"].max() < 6.0
    assert result.sheet_features_df["sheet_max"].iloc[0] < 6.0


def test_prepare_decorated_data_respects_flag_false_for_real_values(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    _write_flag_workbook(product_dir, OOS_DECORATION_FILE_NAME, False)

    result = prepare_decorated_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        scope="spc",
        product_dir=product_dir,
    )

    assert result.raw_measurements_df["param_value"].max() == 8.0
    assert result.sheet_features_df["sheet_max"].iloc[0] == 8.0


def test_prepare_decorated_data_removes_delete_flagged_sheet_from_report(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    _write_flag_workbook(product_dir, OOS_DECORATION_FILE_NAME, "Delete")

    result = prepare_decorated_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        scope="spc",
        product_dir=product_dir,
    )

    assert result.raw_measurements_df.empty
    assert result.sheet_features_df.empty
    assert result.sheet_oos_decoration_result.decoration_df["flag"].tolist() == ["Delete"]
    assert result.sheet_oos_decoration_result.decoration_sheet == "Z571"
    persisted = pd.read_excel(product_dir / OOS_DECORATION_FILE_NAME, sheet_name="Z571")
    assert persisted["flag"].tolist() == ["Delete"]


def test_ctq_scope_uses_ctq_workbook(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources"
    # CTQ 工作簿释放真实值；同名 SPC 工作簿若被误用会删除该片
    _write_flag_workbook(product_dir, "ctq_sheet_oos_decoration.xlsx", False)
    _write_flag_workbook(product_dir, OOS_DECORATION_FILE_NAME, "Delete")

    result = prepare_decorated_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        scope="ctq",
        product_dir=product_dir,
    )

    assert result.raw_measurements_df["param_value"].max() == 8.0
    assert result.sheet_oos_decoration_result.decoration_path.name == (
        "ctq_sheet_oos_decoration.xlsx"
    )


def test_unknown_scope_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown decoration scope"):
        prepare_decorated_data(
            raw_measurements_df=_raw_measurements(),
            spec_df=_spec_limits(),
            prod_code="Z571",
            scope="banana",
            product_dir=tmp_path / "resources",
        )


def test_prepare_decorated_data_threads_gate_params_to_core(
    monkeypatch, tmp_path: Path
) -> None:
    """prepare_decorated_data 把 scope/prod_code/product_revision/decision_signature
    透传到 core prepare_sheet_oos_decoration（启用刷新门控）。"""
    from src.inline_domain.core.shared.sheet_oos_decoration import (
        SheetOosDecorationResult,
    )

    recorded: dict[str, object] = {}

    def fake_core(**kwargs):
        recorded.update(kwargs)
        raw_df = kwargs["raw_measurements_df"]
        return SheetOosDecorationResult(
            raw_measurements_df=raw_df,
            decoration_df=pd.DataFrame(),
            decoration_path=tmp_path / "spc_sheet_oos_decoration.xlsx",
            decoration_sheet=str(kwargs.get("decoration_sheet_name") or "Sheet1"),
        )

    monkeypatch.setattr(decorated_data, "prepare_sheet_oos_decoration", fake_core)

    prepare_decorated_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        scope="spc",
        product_dir=tmp_path / "resources",
        product_revision="rev-9",
        decision_signature="sig-9",
    )

    assert recorded["scope"] == "spc"
    assert recorded["prod_code"] == "Z571"
    assert recorded["product_revision"] == "rev-9"
    assert recorded["decision_signature"] == "sig-9"
