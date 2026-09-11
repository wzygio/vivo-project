"""CPK charts use the preserved ledger, including flags and closed-week boundaries."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.charts.inline_domain.cpk_ledger_chart import build_cpk_ledger_figure
from app.sections.inline_domain.monitor import alert_matrix_detail as detail
from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk


def ledger():
    rows = []
    for factory, week, value, flag in [
        ("ARRAY", "2025-W51", 1.1, True),
        ("ARRAY", "2026-W01", 0.8, False),
        ("ARRAY", "2026-W02", 0.6, False),  # current week must not appear
        ("TP", "2026-W01", 1.5, False),  # same step/parameter, another factory
    ]:
        rows.append(dict(prod_code="P1", factory=factory, step_id="100", param_name="CD",
                         period_type="week", period_label=week, cpk_corrected=value, flag=flag))
    rows.append({**rows[0], "period_type": "month", "period_label": "2025-12"})
    return normalize_latest_cpk(pd.DataFrame(rows), "P1")


def load_bundle(monkeypatch):
    frame = ledger()
    before = frame.copy(deep=True)
    monkeypatch.setattr(detail, "cpk_latest_store", lambda: SimpleNamespace(read_product=lambda product: frame))
    def forbidden(*args, **kwargs):
        raise AssertionError("CPK ledger charts must not query raw SPC")
    monkeypatch.setattr(detail, "_load_spc_view", forbidden)
    bundle = detail._make_spc_cpk_loader(None)("P1", date(2026, 1, 5))
    pd.testing.assert_frame_equal(frame, before)
    return bundle


def test_history_matches_alert_identity_and_excludes_open_week_and_month(monkeypatch):
    bundle = load_bundle(monkeypatch)
    assert len(bundle["alerts_df"]) == 1
    history = bundle["history_df"]
    assert history["period_label"].tolist() == ["2025-W51", "2026-W01"]
    assert history["factory"].tolist() == ["ARRAY", "ARRAY"]
    assert history["cpk"].tolist() == [1.1, 0.8]


def test_chart_preserves_missing_weeks_and_flag_status(monkeypatch):
    figure = build_cpk_ledger_figure(load_bundle(monkeypatch)["history_df"])
    trace = figure.data[0]
    assert list(trace.x) == ["2025-W51", "2025-W52", "2026-W01"]
    assert list(trace.y) == [1.1, None, 0.8]
    assert trace.connectgaps is False
    assert trace.marker.color[0] != trace.marker.color[2]
    assert figure.layout.shapes[0].y0 == 1.33


def test_production_ledger_bundle_renders_chart_on_open(monkeypatch):
    bundle = load_bundle(monkeypatch)
    def app(bundle):
        from app.sections.inline_domain.monitor.alert_matrix_detail import _render_detail_bundle
        _render_detail_bundle(bundle, row_key="spc_cpk_trend", prod_code="P1",
                              week_label="2026-W01", step_desc_map=None, memo_base="test")
    result = AppTest.from_function(app, args=(bundle,)).run()
    assert not result.exception
    assert len(result.dataframe) == 1
    assert len(result.get("plotly_chart")) == 1
    assert any("历史周台账" in item.label for item in result.expander)


def test_empty_alerts_do_not_render_charts(monkeypatch):
    monkeypatch.setattr(detail, "cpk_latest_store", lambda: SimpleNamespace(read_product=lambda product: None))
    bundle = detail._make_spc_cpk_loader(None)("P1", date(2026, 1, 5))
    assert bundle["history_df"].empty
    assert bundle["alerts_df"].empty
