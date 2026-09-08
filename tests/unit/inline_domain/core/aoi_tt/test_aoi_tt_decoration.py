import pandas as pd
import pytest

from src.inline_domain.core.aoi_tt.aoi_tt_decoration import apply_aoi_tt_decoration


def test_decoration_clips_at_ucl_and_preserves_decisions() -> None:
    details = pd.DataFrame([
        {"prod_code": "M678", "step_id": "11620", "tt_name": "TDSUM",
         "sheet_id": str(index), "tt_qty": value}
        for index, value in enumerate([2.0, 3.0, 4.0, 9.0, 4.0, 9.0])
    ])
    specs = pd.DataFrame([{"step_id": "11620", "tt_name": "TDSUM", "usl": 5, "ucl": "3"}])
    decisions = details.iloc[[4, 5]].copy()
    decisions["flag"] = [False, "Delete"]

    result = apply_aoi_tt_decoration(details, specs, decisions)

    assert result["sheet_id"].tolist() == ["0", "1", "2", "3", "4"]
    assert result["tt_qty"].iloc[:2].tolist() == [2.0, 3.0]
    assert result["tt_qty"].iloc[2:4].between(3 * 0.85, 3 * 0.95).all()
    assert result["tt_qty"].iloc[4] == 4.0
    assert result.columns.tolist() == details.columns.tolist()
    assert details["tt_qty"].tolist() == [2.0, 3.0, 4.0, 9.0, 4.0, 9.0]
    pd.testing.assert_frame_equal(result, apply_aoi_tt_decoration(details, specs, decisions))


@pytest.mark.parametrize("ucl", [None, float("nan"), "invalid"])
def test_missing_ucl_does_not_fall_back_to_usl(ucl) -> None:
    details = pd.DataFrame([{"step_id": "11620", "tt_name": "TDSUM", "tt_qty": 9.0}])
    specs = pd.DataFrame([{"step_id": "11620", "tt_name": "TDSUM", "usl": 5}])
    if ucl is not None:
        specs["ucl"] = ucl

    result = apply_aoi_tt_decoration(details, specs, pd.DataFrame())

    pd.testing.assert_frame_equal(result, details)
