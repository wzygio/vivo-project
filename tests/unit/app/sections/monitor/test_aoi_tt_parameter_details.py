"""Matrix detail follows the same TT admission rules as the report page."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.sections.inline_domain.monitor import alert_matrix_detail
from app.sections.shared import alert_results
from src.inline_domain.infrastructure.monitor import excel_alarm_store
from src.inline_domain.infrastructure.shared import resource_paths
from src.shared_kernel.config import ConfigLoader


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
    path = tmp_path / "tt-results.xlsx"
    frame.to_excel(path, sheet_name="M678", index=False)
    published = path.read_bytes()
    alert_results._cached_alert_result.clear()
    excel_alarm_store.read_cached_alarm_workbook.clear()
    reads = []
    original_read = pd.read_excel

    def tracked_read(*args, **kwargs):
        reads.append((args, kwargs))
        return original_read(*args, **kwargs)

    monkeypatch.setattr(resource_paths, "scope_decoration_path", lambda scope: path)
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(pd, "read_excel", tracked_read)

    result = alert_matrix_detail._load_sheet_oos_alerts_display(
        "aoi_tt", "M678", date(2026, 8, 25),
    )

    assert set(result["TT名称"]) == expected
    assert len(result) == len(expected)
    assert "flag" not in result.columns
    # The report consumer obtains the exact same saved selection from the shared cache.
    report_result = alert_results.read_alert_result("aoi_tt_sheet_oos", "M678", date(2026, 8, 25))
    pd.testing.assert_frame_equal(result, report_result["alerts"])
    assert len(reads) == 1
    assert Path(reads[0][0][0]) == path.resolve()
    assert reads[0][1]["sheet_name"] is None
    pd.testing.assert_frame_equal(frame, before)
    assert path.read_bytes() == published
