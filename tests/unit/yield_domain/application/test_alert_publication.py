from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.yield_domain.application.alert_publication import generate_yield_alert_publications
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

DAY = date(2026, 10, 20)
NOW = datetime(2026, 10, 20, 10, tzinfo=timezone(timedelta(hours=8)))
HEALTH = {"status": "fresh", "refreshed_at": NOW.isoformat()}


def results(tmp_path):
    return YieldAlertResults(YieldAlertWorkbookStore({"lot": tmp_path / "lots.xlsx",
        "trend": tmp_path / "trends.xlsx"}), policy_signature=lambda product: "v1",
        ttl_seconds=43200, clock=lambda: NOW)


class Analysis:
    @staticmethod
    def get_data_health(*args, **kwargs):
        return HEALTH

    @staticmethod
    def get_mwd_trend_data(*args, **kwargs):
        return {"weekly": pd.DataFrame({"defect_group": ["G", "G"],
            "time_period": ["2026-W41", "2026-W42"], "defect_rate": [0.001, 0.02]})}

    @staticmethod
    def get_code_level_trend_data(*args, **kwargs):
        return {}

    @staticmethod
    def get_lot_defect_rates(*args, **kwargs):
        return {"code_level_details": {"D1": pd.DataFrame([{
            "lot_id": "L1", "warehousing_time": "20261015", "defect_desc": "D1",
            "defect_rate": 0.02, "defect_panel_count": 2, "array_input_time": "20261001",
        }])}}

    @staticmethod
    def load_static_warning_lines(*args, **kwargs):
        return {"D1": {"upper": 0.01}}


def generate(tmp_path, analysis=Analysis):
    return generate_yield_alert_publications(
        analysis=analysis, publisher=results(tmp_path),
        config=SimpleNamespace(data_source=SimpleNamespace(product_code="P1")), product_dir=tmp_path,
        cache_context={"analysis_start_date": "2026-07-01", "analysis_end_date": str(DAY),
            "modifier_signature": "m1", "warning_signature": "w1", "rate_override_signature": "r1"},
        trend_signature="t1", lot_signature="l1", db_manager=None,
    )


def test_producer_reuses_existing_rules_and_no_unrelated_sheet_mapping_dependency(tmp_path):
    outcome = generate(tmp_path)
    assert not outcome["errors"]
    assert outcome["lot_records"][0]["超规 Lot ID"] == "L1"
    assert outcome["trend_records"][0]["period_scope"] == "weekly"
    assert results(tmp_path).read("P1", "lot", DAY).status == "ready"
    assert results(tmp_path).read("P1", "trend", DAY).status == "ready"


def test_trend_publishes_even_when_lot_calculation_fails(tmp_path):
    class Fails(Analysis):
        @staticmethod
        def get_lot_defect_rates(*args, **kwargs):
            raise RuntimeError("internal details must not be persisted")
    outcome = generate(tmp_path, Fails)
    assert set(outcome["errors"]) == {"lot"}
    assert results(tmp_path).read("P1", "trend", DAY).status == "ready"
    assert results(tmp_path).read("P1", "lot", DAY).status == "failed"


def test_fresh_empty_source_publishes_zero_records_after_cache_hit(tmp_path):
    class Empty(Analysis):
        get_mwd_trend_data = staticmethod(lambda *a, **k: {})
        get_code_level_trend_data = staticmethod(lambda *a, **k: {})
        get_lot_defect_rates = staticmethod(lambda *a, **k: None)
    outcome = generate(tmp_path, Empty)
    assert not outcome["errors"]
    assert results(tmp_path).read("P1", "lot", DAY).status == "no_data"
    assert results(tmp_path).read("P1", "trend", DAY).status == "no_data"


def test_lot_publishes_even_when_trend_fails(tmp_path):
    class Fails(Analysis):
        @staticmethod
        def get_mwd_trend_data(*args, **kwargs):
            raise RuntimeError("trend unavailable")
    outcome = generate(tmp_path, Fails)
    assert set(outcome["errors"]) == {"trend"}
    assert results(tmp_path).read("P1", "lot", DAY).status == "ready"
    assert results(tmp_path).read("P1", "trend", DAY).status == "failed"
