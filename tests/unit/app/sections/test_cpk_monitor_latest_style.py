import pandas as pd

from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
from src.inline_domain.core.monitor.cpk_summary import CPK_SUMMARY_COLUMNS, build_cpk_summary_from_records
from app.sections.inline_domain.monitor import cpk_monitor_dashboard as ui


def test_cpk_fixed_period_chart_and_no_detail_based_denominator(monkeypatch):
    charts, headings = [], []
    monkeypatch.setattr(ui.st, "markdown", headings.append)
    monkeypatch.setattr(ui.st, "dataframe", lambda *a, **k: None)
    monkeypatch.setattr(ui.st, "caption", lambda *a, **k: None)
    monkeypatch.setattr(ui.st, "altair_chart", lambda chart, **k: charts.append(chart))
    monkeypatch.setattr(ui.st, "metric", lambda *a, **k: (_ for _ in ()).throw(AssertionError("incomplete denominator")))
    monkeypatch.setattr(ui.st, "query_params", {})
    summary = build_cpk_summary_from_records(pd.DataFrame(columns=CPK_SUMMARY_COLUMNS), end_date=pd.Timestamp("2026-09-08"), week_count=4)
    detail = pd.DataFrame([dict(prod_code="M626", factory="ARRAY", step_id="1100", param_name="P", period_label="2026-W36", period_type="week", cpk_corrected=.8, flag=False)])
    view = CpkMonitorViewModel(summary, detail, pd.DataFrame([{"source": "excel"}]))
    ui.render_cpk_monitor_results(view)
    assert charts[0].to_dict()["encoding"]["x"]["scale"]["domain"][-4:] == ["W34", "W35", "W36", "W37"]
    assert "#### CPK 数据更新状态（管理员）" not in headings
    monkeypatch.setattr(ui.st, "query_params", {"admin": "true"})
    ui.render_cpk_monitor_results(view)
    assert "#### CPK 数据更新状态（管理员）" in headings
