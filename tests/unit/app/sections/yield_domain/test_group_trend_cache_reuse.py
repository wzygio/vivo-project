"""Exercise the real Group cache shared by the matrix and the all-product board."""

from datetime import date

import pandas as pd

from app.components import indicator_cache
from app.sections.inline_domain.monitor import alert_matrix_cache
from app.sections.yield_domain.group_trend_data import load_product_group_trends
from yield_domain.application.yield_service import YieldAnalysisService
from yield_domain.core.mwd_trend.mwd_trend_processor import MWDTrendProcessor


def test_matrix_warm_entries_are_reused_and_product_revision_is_targeted(monkeypatch):
    context = {
        "analysis_start_date": "2026-06-01", "analysis_end_date": "2026-09-24",
        "modifier_signature": "file-1", "warning_signature": "warning-1",
        "rate_override_signature": "override-1",
    }
    revisions = {"M678": "0", "M626": "0"}
    computations = []
    readonly = []
    frame = pd.DataFrame({"time_period": ["2026-09"], "defect_group": ["Array_Line"],
                          "defect_rate": [.01], "total_panels": [1000]})
    payload = {period: frame for period in ("monthly", "weekly", "daily")}

    monkeypatch.setattr(indicator_cache, "get_indicator_product_revision",
                        lambda _indicator, product, **kwargs: revisions[product])
    monkeypatch.setattr(alert_matrix_cache, "DatabaseManager", lambda: None)
    monkeypatch.setattr(YieldAnalysisService, "build_cache_context", lambda *args: dict(context))
    monkeypatch.setattr(YieldAnalysisService, "get_data_health", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(YieldAnalysisService, "get_modified_panel_details", lambda *args: frame)
    monkeypatch.setattr(YieldAnalysisService, "get_code_level_trend_data", lambda *args, **kwargs: payload)

    def modifier(*args, **kwargs):
        readonly.append(kwargs["read_only"])
        return {"group_targets": {}}

    def compute(**kwargs):
        computations.append(kwargs["config"].data_source.product_code)
        return payload

    monkeypatch.setattr(YieldAnalysisService, "get_modifier_context", modifier)
    monkeypatch.setattr(MWDTrendProcessor, "create_mwd_trend_data", compute)
    YieldAnalysisService.get_mwd_trend_data.clear()
    try:
        matrix = alert_matrix_cache.build_default_matrix_context(
            ["M678", "M626"], reference_date=date(2026, 9, 24),
        )
        for product in revisions:
            matrix.yield_trend_loader(product)
            result = load_product_group_trends(product)
            pd.testing.assert_frame_equal(result["trends"]["monthly"], frame)
        assert computations == ["M678", "M626"]

        revisions["M678"] = "1"
        load_product_group_trends("M678")
        load_product_group_trends("M626")
        assert computations == ["M678", "M626", "M678"]

        context["modifier_signature"] = "file-2"
        load_product_group_trends("M678")
        context["analysis_end_date"] = "2026-09-25"
        load_product_group_trends("M678")
        assert computations == ["M678", "M626", "M678", "M678", "M678"]
        assert readonly == [True] * 5
    finally:
        YieldAnalysisService.get_mwd_trend_data.clear()


def test_unavailable_product_does_not_request_trend_calculation(monkeypatch):
    monkeypatch.setattr(YieldAnalysisService, "get_data_health", lambda *args, **kwargs: {"status": "unavailable"})

    def unexpected(*args, **kwargs):
        raise AssertionError("Unavailable data must not be charted")

    monkeypatch.setattr(YieldAnalysisService, "get_mwd_trend_data", unexpected)
    result = load_product_group_trends("M678")
    assert result == {"data_health": {"status": "unavailable"}, "trends": None}
