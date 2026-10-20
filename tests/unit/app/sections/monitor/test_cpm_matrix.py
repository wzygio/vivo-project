"""CPM matrix cells reuse saved capability results and the SPC payload cache."""

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import alert_matrix_cache as cache
from app.sections.inline_domain.monitor import alert_matrix_detail as detail
from app.sections.inline_domain.monitor import alert_matrix_service as service
from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk
from src.shared_kernel.config import ConfigLoader


REFERENCE = date(2026, 1, 5)


@pytest.fixture(autouse=True)
def _fixed_span_policy(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_spc_capability_alert_min_span_hours", lambda: 48.0)


def _ledger(**overrides) -> pd.DataFrame:
    return pd.DataFrame([{
        "prod_code": "P1", "factory": "ARRAY", "step_id": "100", "param_name": "CD",
        "period_type": "week", "period_label": "2026-W01",
        "period_start": "2025-12-29", "period_end": "2025-12-31",
        "cpk_corrected": 1.8, "cpm_corrected": 0.8, "flag": False, **overrides,
    }])


@pytest.mark.parametrize("changes, expected", [
    ({}, "alert"),
    ({"cpm_corrected": 1.33}, "ok"),
    ({"flag": True}, "ok"),
    ({"flag": "DELETE"}, "ok"),
    ({"cpm_corrected": None}, "no_data"),
    ({"period_label": "2025-W52"}, "no_data"),
    ({"period_label": "2026-W02"}, "no_data"),
    ({"period_end": "2025-12-30"}, "ok"),
    ({"period_end": "2025-12-30 23:59:59"}, "ok"),
    ({"period_end": "2025-12-31"}, "alert"),
])
def test_cpm_uses_saved_metric_and_previous_complete_week(changes, expected):
    frame = _ledger(**changes)
    context = service.AlertMatrixContext(REFERENCE, spc_cpm_loader=lambda product: frame)
    cell = service.MATRIX_ROW_MAP["spc_cpm_trend"].evaluator("P1", context)
    assert cell["state"] == expected
    assert cell["detail_key"] == "spc_cpm_trend|P1"
    assert cell["alert_factories"] == (["ARRAY"] if expected == "alert" else [])


@pytest.mark.parametrize("frame, expected", [(None, "no_data"), (pd.DataFrame(), "no_data"),
                                             (pd.DataFrame([{"invalid": 1}]), "error")])
def test_missing_or_invalid_cpm_results(frame, expected):
    context = service.AlertMatrixContext(REFERENCE, spc_cpm_loader=lambda product: frame)
    assert service.MATRIX_ROW_MAP["spc_cpm_trend"].evaluator("P1", context)["state"] == expected


def test_cpm_respects_factory_date_exclusion(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda:
                        (("ARRAY",), "2025-12-30", "2025-12-30"))
    frame = _ledger()
    context = service.AlertMatrixContext(REFERENCE, spc_cpm_loader=lambda product: frame)
    assert service.MATRIX_ROW_MAP["spc_cpm_trend"].evaluator("P1", context)["state"] == "no_data"


def test_cpm_matrix_and_detail_share_saved_workbook_without_loading_spc(tmp_path, monkeypatch):
    path = tmp_path / "capability.xlsx"
    with pd.ExcelWriter(path) as writer:
        _ledger(cpk_corrected=1.8).to_excel(writer, sheet_name="P1", index=False)
        _ledger().to_excel(writer, sheet_name="P1_cpm", index=False)
    original = ConfigLoader.get_domain_resource_path
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda domain, key, default=None:
                        path if key == "spc_cpk_cpm_decoration" else original(domain, key, default))
    monkeypatch.setattr(detail, "_load_spc_view", lambda *args: pytest.fail("matrix must not load SPC"))
    context = cache.build_default_matrix_context(["P1"], reference_date=REFERENCE)
    assert service.MATRIX_ROW_MAP["spc_cpk_trend"].evaluator("P1", context)["state"] == "ok"
    assert service.MATRIX_ROW_MAP["spc_cpm_trend"].evaluator("P1", context)["state"] == "alert"
    assert cache.build_cell_source_signature("spc_cpm_trend", "P1") == cache.build_cell_source_signature("spc_cpk_trend", "P1")
    # Empty CPM results never load chart data; the CPK sheet must not be used instead.
    monkeypatch.setattr(detail, "cpm_latest_store", lambda: SimpleNamespace(read_product=lambda product: None))
    bundle = detail.build_default_detail_loaders()["spc_cpm_trend"]("P1", REFERENCE)
    assert bundle["alerts_df"].empty


def test_cpm_detail_renders_metric_and_only_alert_indicator_charts(monkeypatch):
    frame = normalize_latest_cpk(_ledger(), "P1", metric="cpm")
    raw = pd.DataFrame([{
        "factory": factory, "step_id": "100", "param_name": "CD", "sheet_id": f"S{i}",
        "sheet_start_time": day, "point_value": 9.0 + i / 10, "sheet_mean": 9.0 + i / 10,
        "usl": 12.0, "lsl": 8.0, "ucl": 11.0, "lcl": 9.0, "target": 10.0,
        "main_eqp_id": "EQ1", "chamber_id": "CH1",
    } for factory in ("ARRAY", "TP")
      for i, day in enumerate(("2025-12-10", "2025-12-30", "2026-01-02"))])
    monkeypatch.setattr(detail, "cpm_latest_store", lambda: SimpleNamespace(read_product=lambda product: frame))
    monkeypatch.setattr(detail, "_load_spc_view", lambda *args: SimpleNamespace(
        period_capability_df=frame, sheet_features_df=raw, raw_measurements_df=raw))
    monkeypatch.setattr(detail, "_report_end_date", lambda: REFERENCE)
    bundle = detail.build_default_detail_loaders(object())["spc_cpm_trend"]("P1", REFERENCE)
    assert bundle["kind"] == "spc_cpm"
    assert bundle["alerts_df"].columns.tolist() == ["厂别", "站点", "参数名称", "超规周次", "CPM值"]
    assert set(bundle["frames"]["sheet_features_df"]["factory"]) == {"ARRAY"}

    def app(bundle):
        from app.sections.inline_domain.monitor.alert_matrix_detail import _render_detail_bundle
        _render_detail_bundle(bundle, row_key="spc_cpm_trend", prod_code="P1",
                              week_label="2026-W01", step_desc_map=None, memo_base="cpm-test")

    result = AppTest.from_function(app, args=(bundle,)).run(timeout=20)
    assert not result.exception
    assert any("CPM 预警明细" in item.label for item in result.expander)
    assert len(result.get("plotly_chart")) == 3
    assert "CPM值" in result.dataframe[0].value.columns
    assert not {"flag", "cpm_corrected"}.intersection(result.dataframe[0].value.columns)


def test_detail_reuses_application_payload_primed_with_spc_page_arguments(monkeypatch):
    from app.components.indicator_cache import build_indicator_product_cache_signature
    from app.sections.inline_domain.spc.spc_dashboard import (
        SPC_REPORT_CACHE_INDICATORS, SPC_REPORT_CACHE_SIGNATURE, get_default_spc_start_date,
    )
    from src.inline_domain import composition
    from src.inline_domain.application.shared import decision_signature
    from src.inline_domain.application.spc import spc_service
    from src.inline_domain.application.spc.dtos import SpcQueryConfig

    calls = []
    def features(**kwargs):
        calls.append(kwargs)
        return {"raw_measurements_df": pd.DataFrame(), "spec_empty": True}

    spc_service.SpcReportService.fetch_spc_report_payload.clear()
    monkeypatch.setattr(spc_service, "fetch_decorated_features", features)
    monkeypatch.setattr(composition, "build_spc_repository", lambda *args: object())
    monkeypatch.setattr(composition, "build_spc_input_signature", lambda *args: "cpm-test-input")
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *args: "cpm-test-decision")
    monkeypatch.setattr(detail, "_report_end_date", lambda: REFERENCE)
    query = SpcQueryConfig(prod_code="M678", start_date=str(get_default_spc_start_date(REFERENCE)),
                           end_date=str(REFERENCE), data_type_filter="SPC")
    signature = build_indicator_product_cache_signature(
        SPC_REPORT_CACHE_SIGNATURE, "M678", SPC_REPORT_CACHE_INDICATORS)
    try:
        spc_service.SpcReportService.get_spc_report_data(
            _data_port=object(), query_config_json=query.model_dump_json(),
            snapshot_signature=f"{signature}:cpm-test-input",
            period_sigma_source=ConfigLoader.get_spc_period_sigma_source(),
            product_revision=signature, decision_signature="cpm-test-decision",
        )
        detail._load_spc_view(object(), "M678")
        detail._load_spc_view(object(), "M678")
        assert len(calls) == 1
    finally:
        spc_service.SpcReportService.fetch_spc_report_payload.clear()
