"""Yield 详情必须加载完整图像输入，并跟踪规格及各输入版本。"""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import alert_matrix_detail
from yield_domain.application.alert_service import AlertService
from yield_domain.application.yield_service import YieldAnalysisService


@pytest.fixture
def yield_sources(monkeypatch: pytest.MonkeyPatch) -> dict:
    config = SimpleNamespace(processing={})
    monkeypatch.setattr(alert_matrix_detail.ConfigLoader, "load_config", lambda _: config)
    monkeypatch.setattr(
        alert_matrix_detail.ConfigLoader, "get_domain_resource_dir", lambda _: Path("unused")
    )
    monkeypatch.setattr(alert_matrix_detail, "build_indicator_product_cache_signature",
                        lambda base, product, indicators: f"{base}|{product}|{','.join(indicators)}")
    context = {
        "analysis_start_date": "2026-09-01",
        "analysis_end_date": "2026-09-10",
        "modifier_signature": "modifier",
        "warning_signature": "spec",
        "rate_override_signature": "override",
    }
    monkeypatch.setattr(
        YieldAnalysisService,
        "build_cache_context",
        lambda *args: context.copy(),
    )
    code = "G向单亮线"
    trend = pd.DataFrame({"defect_group": ["Array_Line"], "defect_desc": [code],
                          "defect_rate": [0.0035], "time_period": ["2026-09月"]})
    lot = pd.DataFrame({"defect_desc": [code], "defect_rate": [0.005],
                        "lot_id": ["LOT-1"], "warehousing_time": ["20260908"],
                        "defect_panel_count": [5]})
    sources = {
        "get_mwd_trend_data": {"monthly": trend},
        "get_code_level_trend_data": {"monthly": trend},
        "get_lot_defect_rates": {"code_level_details": {"Array_Line": lot}},
        "get_sheet_defect_rates": {"code_level_details": {"Array_Line": lot}},
        "get_mapping_data": pd.DataFrame({"defect_group": ["Array_Line"],
            "defect_desc": [code], "batch_no": ["2026/09/08"],
            "batch_total_input": [1000], "panel_id": ["1-1"]}),
        "load_static_warning_lines": {code: {"upper": 0.004, "lower": 0.0005}},
    }
    calls = {}
    for name, value in sources.items():
        def read(*args, _name=name, _value=value, **kwargs):
            calls[_name] = (args, kwargs)
            return _value
        monkeypatch.setattr(YieldAnalysisService, name, read)
    records = [{"level": "code", "defect_group": "Array_Line", "defect_desc": code}]
    monkeypatch.setattr(AlertService, "get_dashboard_alert_records", lambda *args: records)
    monkeypatch.setattr(alert_matrix_detail, "compute_lot_oos_records", lambda *args: (
        [{"异常 Code": code, "入库时间": "2026/09/08"}], 1))
    return {"sources": sources, "calls": calls, "context": context}


@pytest.mark.parametrize("mode", ["trend", "lot"])
def test_yield_loader_loads_complete_readonly_chart_inputs(yield_sources, mode) -> None:
    bundle = alert_matrix_detail._make_yield_loader(None, mode)("Z517", date(2026, 9, 14))
    for field, method in {
        "mwd_code_data": "get_code_level_trend_data", "lot_data": "get_lot_defect_rates",
        "sheet_data": "get_sheet_defect_rates", "mapping_data": "get_mapping_data",
        "warning_lines": "load_static_warning_lines",
    }.items():
        assert bundle[field] is yield_sources["sources"][method]
    assert bundle["records"]
    for method, (_, kwargs) in yield_sources["calls"].items():
        if method != "load_static_warning_lines":
            assert kwargs["read_only"] is True
    expected = {
        "get_code_level_trend_data": "yield_trend_fluctuation",
        "get_mapping_data": "yield_trend_fluctuation",
        "get_lot_defect_rates": "yield_lot_oos",
        "get_sheet_defect_rates": "yield_sheet_oos",
    }
    for method, indicator in expected.items():
        assert yield_sources["calls"][method][1]["snapshot_signature"].endswith(indicator)


def _render_cached_trend_detail() -> None:
    import streamlit as st
    from app.sections.inline_domain.monitor.alert_matrix_detail import _render_yield_detail

    _render_yield_detail(
        st.session_state["bundle"], prod_code="Z517", week_label="2026-W37",
        memo_base=st.session_state.get("source_signature", ""),
    )


@pytest.mark.parametrize("mapping_data", [pd.DataFrame(), None], ids=["frame", "none"])
def test_cached_trend_detail_renders_charts_with_empty_mapping(mapping_data) -> None:
    trend = pd.DataFrame({
        "defect_group": ["Array_Line"],
        "defect_desc": ["S向亮线"],
        "defect_rate": [0.0217],
        "time_period": ["2026-09月"],
    })
    bundle = {
        "yield_mode": "trend",
        "records": [{
            "level": "code", "defect_group": "Array_Line",
            "defect_desc": "S向亮线", "period_scope": "weekly",
        }],
        "mwd_code_data": {"monthly": trend},
        "lot_data": {}, "sheet_data": {}, "mapping_data": mapping_data,
        "warning_lines": {}, "hotspot_scripts": [], "mapping_layout": None,
    }
    app = AppTest.from_function(_render_cached_trend_detail)
    app.session_state["bundle"] = bundle
    app.run()

    assert not app.exception
    assert len(app.dataframe) == 1
    assert len(app.get("plotly_chart")) == 1
    assert any("自动预警缺陷图像（1 个 Code）" in item.label for item in app.expander)
    assert any("Mapping 数据源为空" in item.value for item in app.warning)
    # 页面 rerun 继续复用图像缓存，且不原地修改已有详情数据包。
    app.run()
    assert not app.exception
    assert len(app.get("plotly_chart")) == 1
    assert isinstance(app.session_state["bundle"]["mapping_data"], type(mapping_data))


@pytest.mark.parametrize("mode", ["trend", "lot"])
def test_complete_detail_renders_product_spec_mapping_and_lot(yield_sources, mode) -> None:
    bundle = alert_matrix_detail._make_yield_loader(None, mode)("Z517", date(2026, 9, 14))
    app = AppTest.from_function(_render_cached_trend_detail)
    app.session_state["bundle"] = bundle
    app.session_state["source_signature"] = "source-v1"
    app.run()
    assert not app.exception
    assert any("Spec 0.40%" in item.label for item in app.expander)
    assert len(app.get("plotly_chart")) == 3  # 趋势、Mapping、Lot，使用真实绘图函数
    assert not app.warning
    # Code 和规格相同，输入数据更新仍须重建图像，不能复用旧空图。
    app.session_state["bundle"] = {**bundle, "mapping_data": pd.DataFrame()}
    app.session_state["source_signature"] = "source-v2"
    app.run()
    assert not app.exception
    assert len(app.get("plotly_chart")) == 2
    assert any("Mapping 数据源为空" in item.value for item in app.warning)


@pytest.mark.parametrize("field", [
    "warning_signature", "modifier_signature", "rate_override_signature",
    "analysis_start_date", "analysis_end_date",
])
def test_detail_cache_reloads_when_chart_input_changes(yield_sources, field) -> None:
    alert_matrix_detail._cached_matrix_detail_bundle.clear()
    calls = []

    def read():
        calls.append(1)
        return {"version": len(calls)}

    def cached(signature):
        return alert_matrix_detail.get_cached_matrix_detail(
            detail_key="yield_trend_fluctuation|Z517", reference_date=date(2026, 9, 14),
            signature=signature, _loader=read,
        )

    try:
        cached("cell-v1")  # 修复前的详情缓存不能继续复用。
        first = alert_matrix_detail._yield_detail_signature("Z517", "cell-v1")
        assert cached(first)["version"] == 2
        assert cached(alert_matrix_detail._yield_detail_signature("Z517", "cell-v1"))["version"] == 2
        yield_sources["context"][field] += "-updated"
        updated = alert_matrix_detail._yield_detail_signature("Z517", "cell-v1")
        assert updated != first
        assert cached(updated)["version"] == 3
    finally:
        alert_matrix_detail._cached_matrix_detail_bundle.clear()


@pytest.mark.parametrize("indicator", [
    "yield_trend_fluctuation", "yield_lot_oos", "yield_sheet_oos",
])
def test_detail_tracks_all_chart_revisions_for_selected_product(
    yield_sources, monkeypatch: pytest.MonkeyPatch, indicator: str,
) -> None:
    revisions = {}

    def signature(base, product, indicators):
        return base + repr([(key, revisions.get((product, key), "0")) for key in indicators])

    monkeypatch.setattr(alert_matrix_detail, "build_indicator_product_cache_signature", signature)
    first = alert_matrix_detail._yield_detail_signature("Z517", "cell-v1")
    revisions[("Z571", indicator)] = "other-product"
    assert alert_matrix_detail._yield_detail_signature("Z517", "cell-v1") == first
    revisions[("Z517", indicator)] = "updated"
    assert alert_matrix_detail._yield_detail_signature("Z517", "cell-v1") != first
