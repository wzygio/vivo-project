"""SPC alerts read saved capability records and updates must be durable."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from app.sections.inline_domain.spc.spc_dashboard import build_weekly_cpk_alerts, build_weekly_cpm_alerts
from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore
from src.inline_domain.infrastructure.spc import capability_decoration_repository as repository


REFERENCE = date(2026, 8, 17)


def computed(metric, value=.9, hours=72):
    start = pd.Timestamp("2026-08-10 05:00")
    return pd.DataFrame([dict(
        prod_code="M626", factory="ARRAY", step_id="1B990", param_name="CD",
        period_type="week", period_label="2026-W33", period_sort=33,
        period_start=start, period_end=start + pd.Timedelta(hours=hours), **{metric: value},
    )])


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_raw_calculation_cannot_create_page_alerts(metric):
    build = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    assert build(computed(metric), reference_date=REFERENCE).empty


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_same_week_saved_lifecycle_matches_page_and_monitor(metric, tmp_path):
    path = tmp_path / "capability.xlsx"
    sheet = "M626" if metric == "cpk" else "M626_cpm"
    reader = (CpkLatestExcelStore if metric == "cpk" else CpmLatestExcelStore)(path)
    build = build_weekly_cpk_alerts if metric == "cpk" else build_weekly_cpm_alerts
    for value, hours, expected in [(.9, 18, 0), (.9, 48, 1), (1.5, 72, 0), (.8, 72, 1), (None, 72, 0)]:
        result = prepare_capability_decoration(
            computed(metric, value, hours), tmp_path, workbook_path=path,
            sheet_name=sheet, metric=metric, reference_date=REFERENCE, strict_persistence=True,
        )
        if value is None:
            assert result.period_capability_df[metric].isna().all()
        else:
            assert result.period_capability_df[metric].tolist() == [value]
        detail = reader.read_product("M626")
        alerts = build(detail, reference_date=REFERENCE)
        assert len(alerts) == expected
        if expected:
            assert alerts[f"{metric.upper()}值"].tolist() == [value]
        if hours >= 48:
            assert len(pd.read_excel(path, sheet_name=sheet)) == 1


def test_strict_update_failure_preserves_file_and_raises(tmp_path, monkeypatch):
    path = tmp_path / "capability.xlsx"
    initial = prepare_capability_decoration(
        computed("cpk"), tmp_path, workbook_path=path, sheet_name="M626",
        reference_date=REFERENCE,
    )
    before = path.read_bytes()
    monkeypatch.setattr(repository, "replace_workbook_sheets", lambda *args: SimpleNamespace(
        written=False, error="locked workbook",
    ))
    with pytest.raises(RuntimeError, match="persist"):
        prepare_capability_decoration(
            computed("cpk", 1.5), tmp_path, workbook_path=path, sheet_name="M626",
            reference_date=REFERENCE, strict_persistence=True,
        )
    assert initial.decoration_path.read_bytes() == before
