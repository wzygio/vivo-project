from pathlib import Path

import pandas as pd

from src.spc_domain.application import spc_data_decoration
from src.spc_domain.application.spc_data_decoration import prepare_decorated_spc_data
from src.spc_domain.core.cpm_sheet_oos_decoration import OOS_DECORATION_FILE_NAME


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


def test_prepare_decorated_spc_data_clips_points_and_recomputes_sheet_features(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources" / "Z571"

    result = prepare_decorated_spc_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        product_dir=product_dir,
    )

    assert result.original_sheet_features_df["sheet_max"].iloc[0] == 8.0
    assert result.sheet_oos_decoration_result.detail_path.exists()
    assert result.sheet_oos_decoration_result.decoration_path.exists()
    assert result.sheet_oos_decoration_result.detail_df["sheet_id"].tolist() == ["S1"]
    assert result.raw_measurements_df["param_value"].max() < 6.0
    assert result.sheet_features_df["sheet_max"].iloc[0] < 6.0


def test_prepare_decorated_spc_data_applies_configured_clip_rules(monkeypatch, tmp_path: Path) -> None:
    raw_measurements = _raw_measurements()
    raw_measurements.loc[raw_measurements["param_value"] == 8.0, "param_value"] = 6.2
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_spc_sheet_oos_clip_rules",
        staticmethod(
            lambda: [
                {
                    "param_name_contains": "PPA",
                    "lower_offset": -0.5,
                    "upper_offset": 0.5,
                }
            ]
        ),
    )

    result = prepare_decorated_spc_data(
        raw_measurements_df=raw_measurements,
        spec_df=_spec_limits(),
        prod_code="Z571",
        product_dir=tmp_path / "resources" / "Z571",
        persist_files=False,
    )

    assert result.raw_measurements_df["param_value"].max() == 6.2
    assert result.sheet_features_df["sheet_max"].iloc[0] == 6.2


def test_prepare_decorated_spc_data_respects_flag_false_for_real_values(tmp_path: Path) -> None:
    product_dir = tmp_path / "resources" / "Z571"
    product_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "factory": "OLED",
                "prod_code": "Z571",
                "step_id": "21200",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": "2026-07-01 08:00:00",
                "sheet_max": 8.0,
                "sheet_min": 0.0,
                "sheet_mean": 4.0,
                "usl": 6.0,
                "lsl": -6.0,
                "oos_type": "USL",
                "flag": False,
            }
        ]
    ).to_excel(product_dir / OOS_DECORATION_FILE_NAME, index=False)

    result = prepare_decorated_spc_data(
        raw_measurements_df=_raw_measurements(),
        spec_df=_spec_limits(),
        prod_code="Z571",
        product_dir=product_dir,
    )

    assert result.raw_measurements_df["param_value"].max() == 8.0
    assert result.sheet_features_df["sheet_max"].iloc[0] == 8.0
