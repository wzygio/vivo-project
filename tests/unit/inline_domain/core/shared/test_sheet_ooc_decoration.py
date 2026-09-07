from __future__ import annotations

import pandas as pd

from src.inline_domain.core.shared.sheet_ooc_decoration import (
    build_aoi_rs_ooc_detail,
    build_aoi_tt_ooc_detail,
    build_sheet_ooc_detail,
)


def test_sheet_ooc_uses_control_limits_and_excludes_oos_rows() -> None:
    features = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "step_id": "100",
                "param_name": "CD",
                "sheet_id": "OOC",
                "sheet_start_time": "2026-09-01 08:00:00",
                "sheet_max": 56.0,
                "sheet_min": 55.0,
                "sheet_mean": 55.5,
                "usl": 60.0,
                "lsl": 40.0,
                "ucl": 54.0,
                "lcl": 46.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "step_id": "100",
                "param_name": "CD",
                "sheet_id": "OOS",
                "sheet_start_time": "2026-09-01 09:00:00",
                "sheet_max": 61.0,
                "sheet_min": 58.0,
                "sheet_mean": 59.0,
                "usl": 60.0,
                "lsl": 40.0,
                "ucl": 54.0,
                "lcl": 46.0,
            },
        ]
    )

    result = build_sheet_ooc_detail(features)

    assert result["sheet_id"].tolist() == ["OOC"]
    assert result["ooc_type"].tolist() == ["UCL"]


def test_aoi_tt_ooc_uses_ucl_and_excludes_usl_rows() -> None:
    details = pd.DataFrame(
        [
            {"factory": "ARRAY", "prod_code": "M626", "step_id": "100", "tt_name": "TT", "sheet_id": "OOC", "lot_id": "L1", "start_time": "2026-09-01", "tt_qty": 4.0},
            {"factory": "ARRAY", "prod_code": "M626", "step_id": "100", "tt_name": "TT", "sheet_id": "OOS", "lot_id": "L1", "start_time": "2026-09-02", "tt_qty": 6.0},
        ]
    )
    specs = pd.DataFrame([{"step_id": "100", "tt_name": "TT", "ucl": 3.0, "usl": 5.0}])

    result = build_aoi_tt_ooc_detail(details, specs)

    assert result["sheet_id"].tolist() == ["OOC"]
    assert result["ooc_type"].tolist() == ["UCL"]


def test_missing_real_control_limits_and_aoi_rs_produce_no_ooc() -> None:
    features = pd.DataFrame([{"factory": "ARRAY", "prod_code": "M626"}])

    assert build_sheet_ooc_detail(features).empty
    assert build_aoi_tt_ooc_detail(features, pd.DataFrame()).empty
    assert build_aoi_rs_ooc_detail().empty
