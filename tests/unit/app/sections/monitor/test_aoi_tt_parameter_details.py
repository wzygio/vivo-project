"""Matrix detail follows the same TT admission rules as the report page."""

from datetime import date

import pandas as pd
import pytest

from app.sections.inline_domain.monitor import alert_matrix_detail


@pytest.mark.parametrize("names, expected", [
    (["CD1", "OVL1_X", "SE_L1T", "4PP_Rs", "PPA_X", "TPPOS_X_DIFF"], set()),
    (["CD1", "OVL1_X", "SE_L1T", "4PP_Rs", "PPA_X", "TPPOS_X_DIFF",
      "TDSUM", "DSUM_L", "DSUM_O", "TOTAL_O_L"],
     {"TDSUM", "DSUM_L", "DSUM_O", "TOTAL_O_L"}),
])
def test_matrix_tt_details_exclude_non_tt_workbook_rows(monkeypatch, tmp_path, names, expected):
    frame = pd.DataFrame([
        dict(factory="ARRAY", prod_code="M678", step_id="11620", tt_name=name,
             sheet_id=f"S{index}", start_time="2026-08-20 10:00:00", tt_qty=5, usl=3, flag=False)
        for index, name in enumerate(names)
    ])
    before = frame.copy(deep=True)
    reads = []

    def fake_load(resource_dir, **kwargs):
        reads.append((resource_dir, kwargs))
        return frame

    monkeypatch.setattr(alert_matrix_detail, "scope_decoration_path",
                        lambda scope: tmp_path / "tt-decisions.xlsx")
    monkeypatch.setattr(alert_matrix_detail, "load_sheet_oos_decoration", fake_load)

    result = alert_matrix_detail._load_sheet_oos_alerts_display(
        "aoi_tt", "M678", date(2026, 8, 25),
    )

    assert set(result["TT名称"]) == expected
    assert len(result) == len(expected)
    assert "flag" not in result.columns
    assert len(reads) == 1
    assert reads[0][0] == tmp_path
    assert reads[0][1]["file_name"] == "tt-decisions.xlsx"
    assert reads[0][1]["sheet_name"] == "M678"
    pd.testing.assert_frame_equal(frame, before)
