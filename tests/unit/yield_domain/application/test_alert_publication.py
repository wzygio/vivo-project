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


def test_publication_keeps_full_lot_window_and_history_without_changing_page_results(tmp_path):
    class History(Analysis):
        @staticmethod
        def get_lot_defect_rates(*args, **kwargs):
            frame = Analysis.get_lot_defect_rates()["code_level_details"]["D1"]
            old = frame.assign(lot_id="OLD", warehousing_time="20260810")
            return {"code_level_details": {"D1": pd.concat([old, frame], ignore_index=True)}}

        @staticmethod
        def get_mwd_trend_data(*args, **kwargs):
            return {"monthly": pd.DataFrame({"defect_group": ["G"] * 3,
                "time_period": ["2026-08", "2026-09", "2026-10"],
                "defect_rate": [.001, .02, .001]})}

    outcome = generate(tmp_path, History)
    assert not outcome["errors"]
    assert [row["超规 Lot ID"] for row in outcome["lot_records"]] == ["L1"]
    published = pd.read_excel(tmp_path/"lots.xlsx", sheet_name="P1")
    assert set(published["超规 Lot ID"]) == {"OLD", "L1"}
    assert results(tmp_path).read("P1", "lot", DAY).metadata["coverage_start"] == "2026-07-01"
    history = pd.read_excel(tmp_path/"trends.xlsx", sheet_name="P1")
    assert history["time_period"].tolist() == ["2026-09"]
    assert not history["is_current"].any()
    assert outcome["trend_records"] == []
    assert results(tmp_path).read("P1", "trend", DAY).records.empty


def test_prior_period_is_only_a_baseline_for_selected_publication_window(tmp_path):
    class History(Analysis):
        @staticmethod
        def get_mwd_trend_data(*args, **kwargs):
            return {"monthly": pd.DataFrame({"defect_group":["G"]*4,
                "time_period":["2026-07","2026-08","2026-09","2026-10"],
                "defect_rate":[.001,.02,.001,.02]})}
    outcome = generate_yield_alert_publications(analysis=History,publisher=results(tmp_path),
        config=SimpleNamespace(data_source=SimpleNamespace(product_code="P1")),product_dir=tmp_path,
        cache_context={"analysis_start_date":"2026-07-01","analysis_end_date":str(DAY),
            "modifier_signature":"m","warning_signature":"w","rate_override_signature":"r"},
        trend_signature="t",lot_signature="l",db_manager=None,publication_start_date="2026-08-01")
    assert not outcome["errors"]
    frame = pd.read_excel(tmp_path/'trends.xlsx',sheet_name='P1')
    assert frame['time_period'].tolist() == ['2026-08','2026-10']
    assert frame['is_current'].tolist() == [False,True]
    assert results(tmp_path).read('P1','trend',DAY).metadata['coverage_start'] == '2026-08-01'


def test_history_uses_full_series_and_chinese_month_labels():
    from src.yield_domain.application.alert_service import AlertService
    from src.yield_domain.application.alert_publication import _trend_period_overlaps
    full=pd.DataFrame({'defect_group':['G']*4,'time_period':['2026-07月','2026-08月','2026-09月','2026-10月'],
                       'defect_rate':[.001,.02,.001,.02]})
    records=AlertService.get_dashboard_alert_history({'monthly':full.iloc[-3:],'monthly_full':full},{})
    assert [row['time_period'] for row in records] == ['2026-08月','2026-10月']
    assert all(_trend_period_overlaps(row,'2026-08-01','2026-10-21') for row in records)


def test_trend_only_republication_does_not_compute_or_change_lot(tmp_path):
    generate(tmp_path)
    before=(tmp_path/'lots.xlsx').read_bytes()
    class TrendOnly(Analysis):
        get_lot_defect_rates=staticmethod(lambda *a,**k: pytest.fail('Lot must not be recomputed'))
    result=generate_yield_alert_publications(analysis=TrendOnly,publisher=results(tmp_path),
        config=SimpleNamespace(data_source=SimpleNamespace(product_code='P1')),product_dir=tmp_path,
        cache_context={'analysis_start_date':'2026-07-01','analysis_end_date':str(DAY),
            'modifier_signature':'m','warning_signature':'w','rate_override_signature':'r'},
        trend_signature='t',lot_signature='l',db_manager=None,kinds=('trend',))
    assert not result['errors']
    assert (tmp_path/'lots.xlsx').read_bytes()==before
