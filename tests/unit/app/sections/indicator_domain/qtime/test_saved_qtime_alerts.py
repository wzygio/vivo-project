from datetime import date

import pandas as pd

from app.sections.indicator_domain.qtime import dashboard
from app.sections.shared import alert_results


def test_saved_qtime_alert_display_keeps_scope_and_uses_shared_consumer(monkeypatch):
    calls, displayed, captions = [], [], []

    def read(row, product, reference, **selection):
        calls.append((row, product, reference, selection))
        return {"state": "alert", "records": pd.DataFrame([{"lot_id": product}]),
                "metadata": {"total_lots": 3}}

    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(dashboard, "render_qtime_alert_center", lambda alerts, **kwargs: displayed.append((alerts, kwargs)))
    monkeypatch.setattr(dashboard.st, "caption", captions.append)
    dashboard.render_saved_qtime_alerts(
        ["M678", "M626"], shop="ARRAY", step_descriptions=("PATH",), reference_date=date(2026, 10, 27),
    )
    assert len(calls) == 2
    assert all(call[0] == "qtime_sheet_oos" for call in calls)
    assert calls[0][3] == {"shops": ("ARRAY",), "step_descriptions": ("PATH",),
                           "start_date": "2026-09-01", "end_date": "2026-10-27"}
    assert displayed[0][0].lot_id.tolist() == ["M678", "M626"]
    assert displayed[0][1] == {"total_lots": 6}


def test_saved_qtime_error_never_claims_no_alerts_or_invokes_report_service(monkeypatch):
    warnings, displayed = [], []
    monkeypatch.setattr(alert_results, "read_alert_result", lambda *_a, **_k: {"state": "error"})
    monkeypatch.setattr(dashboard.st, "warning", warnings.append)
    monkeypatch.setattr(dashboard, "render_qtime_alert_center", lambda *_a, **_k: displayed.append(True))
    dashboard.render_saved_qtime_alerts(["M678"], shop="ARRAY", step_descriptions=("PATH",))
    assert len(warnings) == 1
    assert displayed == []
