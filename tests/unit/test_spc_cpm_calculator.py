import math

import numpy as np
import pandas as pd

from src.spc_domain.core.cpm_calculator import (
    build_lot_cpm_report,
    calculate_cpk,
    calculate_cpm,
    derive_lot_id,
)


def test_calculate_cpm_uses_target_adjusted_denominator() -> None:
    cpm = calculate_cpm(mean_value=50.0, std_value=1.0, usl=55.0, lsl=45.0, target=50.0)

    assert cpm == 10.0 / 6.0


def test_calculate_cpm_penalizes_mean_drift_from_target() -> None:
    cpm = calculate_cpm(mean_value=51.0, std_value=1.0, usl=55.0, lsl=45.0, target=50.0)

    assert cpm == 10.0 / (6.0 * math.sqrt(2.0))


def test_calculate_cpm_returns_nan_for_single_sided_specs() -> None:
    cpm = calculate_cpm(mean_value=50.0, std_value=1.0, usl=55.0, lsl=np.nan, target=50.0)

    assert math.isnan(cpm)


def test_calculate_cpk_uses_nearest_spec_distance() -> None:
    centered_cpk = calculate_cpk(mean_value=50.0, std_value=1.0, usl=55.0, lsl=45.0)
    drifted_cpk = calculate_cpk(mean_value=54.0, std_value=1.0, usl=55.0, lsl=45.0)

    assert centered_cpk == 5.0 / 3.0
    assert drifted_cpk == 1.0 / 3.0


def test_derive_lot_id_uses_first_nine_chars() -> None:
    assert derive_lot_id("ABCDEFGHIJK") == "ABCDEFGHI"
    assert derive_lot_id("SHORT") == ""


def test_build_lot_cpm_report_groups_by_lot_and_indicator() -> None:
    sheet_features = pd.DataFrame(
        [
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000101",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 49.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000102",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 50.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
            {
                "prod_code": "P1",
                "factory": "ARRAY",
                "sheet_id": "LOT00000103",
                "step_id": "S1",
                "param_name": "THK",
                "sheet_mean": 51.0,
                "usl": 55.0,
                "lsl": 45.0,
                "target": 50.0,
            },
        ]
    )

    report = build_lot_cpm_report(sheet_features)

    assert len(report) == 1
    row = report.iloc[0]
    assert row["lot_id"] == "LOT000001"
    assert row["sheet_count"] == 3
    assert row["lot_mean"] == 50.0
    assert row["lot_std"] == 1.0
    assert row["cpm"] == 10.0 / 6.0
    assert row["cpk"] == 5.0 / 3.0
