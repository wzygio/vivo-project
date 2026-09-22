"""CPK detail reuses the SPC indicator charts while retaining matrix alert identity."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import alert_matrix_detail as detail
from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk


def ledger():
    rows = []
    for factory, week, value, flag in [
        ("ARRAY", "2025-W51", 1.1, True),
        ("ARRAY", "2026-W01", 0.8, False),
        ("ARRAY", "2026-W02", 0.6, False),
        ("TP", "2026-W01", 1.5, False),
    ]:
        rows.append(dict(prod_code="P1", factory=factory, step_id="100", param_name="CD",
                         period_type="week", period_label=week, cpk_corrected=value, flag=flag))
    return normalize_latest_cpk(pd.DataFrame(rows), "P1")


def load_bundle(monkeypatch):
    frame = ledger()
    before = frame.copy(deep=True)
    monkeypatch.setattr(detail, "cpk_latest_store", lambda: SimpleNamespace(read_product=lambda product: frame))
    monkeypatch.setattr(detail, "_report_end_date", lambda: date(2026, 1, 5))
    raw = pd.DataFrame([
        dict(factory=factory, step_id="100", param_name="CD", sheet_id=f"S{i}",
             sheet_start_time=day, point_value=9.0 + i / 10, sheet_mean=9.0 + i / 10,
             usl=12.0, lsl=8.0, ucl=11.0, lcl=9.0, target=10.0,
             main_eqp_id="EQ1", chamber_id="CH1")
        for factory in ("ARRAY", "TP")
        for i, day in enumerate(("2025-12-10", "2025-12-30", "2026-01-02"))
    ])
    view = SimpleNamespace(period_capability_df=frame, sheet_features_df=raw,
                           raw_measurements_df=raw)
    monkeypatch.setattr(detail, "_load_spc_view", lambda db, product: view)
    bundle = detail._make_spc_cpk_loader(object())("P1", date(2026, 1, 5))
    pd.testing.assert_frame_equal(frame, before)
    return bundle


def render_bundle(bundle):
    def app(bundle):
        from app.sections.inline_domain.monitor.alert_matrix_detail import _render_detail_bundle
        _render_detail_bundle(bundle, row_key="spc_cpk_trend", prod_code="P1",
                              week_label="2026-W01", step_desc_map=None, memo_base="test")
    return AppTest.from_function(app, args=(bundle,)).run(timeout=20)


def test_cpk_loads_full_chart_window_for_exact_alert_identity(monkeypatch):
    bundle = load_bundle(monkeypatch)
    assert bundle["kind"] == "spc_cpk"
    assert bundle["alerts_df"]["超规周次"].tolist() == ["2026-W01"]
    assert bundle["end_date"] == "2026-01-05"
    for frame in bundle["frames"].values():
        assert set(frame["factory"]) == {"ARRAY"}
    assert len(bundle["frames"]["raw_measurements_df"]) == 3


def test_production_cpk_bundle_renders_three_original_charts(monkeypatch):
    result = render_bundle(load_bundle(monkeypatch))
    assert not result.exception
    assert len(result.get("plotly_chart")) == 3
    assert not any("历史周台账" in item.label for item in result.expander)
    assert any("CPK 自动预警指标图像（1 个指标）" in item.label for item in result.expander)
    for table in result.dataframe:
        assert "flag" not in table.value.columns
        assert "cpk_corrected" not in table.value.columns


def test_empty_alerts_skip_spc_loading_and_charts(monkeypatch):
    monkeypatch.setattr(detail, "cpk_latest_store", lambda: SimpleNamespace(read_product=lambda product: None))
    monkeypatch.setattr(detail, "_load_spc_view", lambda *args: pytest.fail("No alerts must not load SPC"))
    bundle = detail._make_spc_cpk_loader(None)("P1", date(2026, 1, 5))
    assert bundle["alerts_df"].empty
    result = render_bundle(bundle)
    assert not result.exception
    assert len(result.get("plotly_chart")) == 0


def test_missing_spc_data_keeps_alert_table_and_business_message(monkeypatch):
    bundle = load_bundle(monkeypatch)
    bundle["frames"] = {name: pd.DataFrame() for name in bundle["frames"]}
    result = render_bundle(bundle)
    assert not result.exception
    assert len(result.dataframe) == 1
    assert len(result.get("plotly_chart")) == 0
    assert result.warning[0].value == "预警指标暂无可绘制的 Sheet 数据。"


def test_spc_failure_propagates_to_detail_error_boundary(monkeypatch):
    load_bundle(monkeypatch)
    def fail(*args):
        raise RuntimeError("source unavailable")
    monkeypatch.setattr(detail, "_load_spc_view", fail)
    with pytest.raises(RuntimeError, match="source unavailable"):
        detail._make_spc_cpk_loader(object())("P1", date(2026, 1, 5))


def test_spc_detail_signature_tracks_chart_inputs_without_board_invalidation(monkeypatch):
    from src.inline_domain.application.shared import decision_signature

    state = {"revision": "v1", "decision": "d1", "end": date(2026, 1, 5)}
    monkeypatch.setattr(detail, "_report_end_date", lambda: state["end"])
    monkeypatch.setattr(detail, "build_indicator_product_cache_signature", lambda *args: state["revision"])
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *args: state["decision"])
    first = detail._spc_detail_signature("P1", "cell")
    assert detail._spc_detail_signature("P1", "cell") == first
    for field, value in (("revision", "v2"), ("decision", "d2"), ("end", date(2026, 1, 6))):
        before = detail._spc_detail_signature("P1", "cell")
        state[field] = value
        assert detail._spc_detail_signature("P1", "cell") != before
