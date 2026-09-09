"""Independent indicator/product reduction and Excel-only CPK contracts."""
from collections import Counter
from datetime import date

import pandas as pd

from app.sections.inline_domain.monitor import alert_matrix_cache as cache
from app.sections.inline_domain.monitor import alert_matrix_service as service
from app.sections.inline_domain.monitor import alert_matrix_detail as detail
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore


def test_cell_refresh_and_details_do_not_invalidate_neighbors(monkeypatch):
    cache._cached_alert_matrix_cell.clear()
    detail._cached_matrix_detail_bundle.clear()
    calls, detail_calls, revisions = Counter(), Counter(), {}
    rows = []
    for key in ("A", "B"):
        def evaluator(product, context, key=key):
            calls[key, product] += 1
            return {"state": "alert", "detail_key": f"{key}|{product}"}
        rows.append(service.AlertMatrixRow(key, key, key, "weekly", evaluator))
    monkeypatch.setattr(service, "MATRIX_ROWS", tuple(rows))

    def build():
        return cache.get_cached_alert_matrix(products=("P1", "P2"), reference_date=date(2026, 9, 8),
            _context_factory=lambda: service.AlertMatrixContext(date(2026, 9, 8)),
            _signature_provider=lambda key, product: "stable",
            _revision_provider=lambda key, product: revisions.get((key, product), "0"))

    def load_details(payload):
        for (key, product), cell in payload["cells"].items():
            def loader(key=key, product=product):
                detail_calls[key, product] += 1
                return {"value": 1}
            detail.get_cached_matrix_detail(detail_key=cell["detail_key"], reference_date=date(2026, 9, 7),
                                            signature=cell["cache_signature"], _loader=loader)
    first = build()
    load_details(first)
    load_details(build())
    assert set(calls.values()) == {1}
    assert set(detail_calls.values()) == {1}
    revisions["A", "P1"] = "changed"
    second = build()
    load_details(second)
    assert calls == detail_calls == Counter({("A", "P1"): 2, ("A", "P2"): 1, ("B", "P1"): 1, ("B", "P2"): 1})
    assert second["cells"]["B", "P2"]["computed_at"] == first["cells"]["B", "P2"]["computed_at"]


def test_cpk_excel_changes_rebuild_only_cpk_and_default_detail(tmp_path, monkeypatch):
    cache._cached_alert_matrix_cell.clear()
    path = tmp_path / "latest.xlsx"
    store = CpkLatestExcelStore(path)
    def write(flag):
        pd.DataFrame([{"prod_code": "P1", "factory": "ARRAY", "step_id": "100", "param_name": "CD",
                       "period_type": "week", "period_label": "2026-W36", "cpk_corrected": 0.8, "flag": flag}]).to_excel(path, sheet_name="P1", index=False)
    monkeypatch.setattr(service, "MATRIX_ROWS", (service.MATRIX_ROW_MAP["spc_cpk_trend"],))
    monkeypatch.setattr(detail, "cpk_latest_store", lambda: store)
    monkeypatch.setattr(detail, "_load_spc_view", lambda *args: (_ for _ in ()).throw(AssertionError("raw SPC forbidden")))
    def build():
        return cache.get_cached_alert_matrix(products=("P1",), reference_date=date(2026, 9, 8),
            _context_factory=lambda: service.AlertMatrixContext(date(2026, 9, 8), spc_cpk_loader=store.read_product),
            _signature_provider=lambda *args: store.source_signature(), _revision_provider=lambda *args: "0")
    write(False)
    assert build()["cells"]["spc_cpk_trend", "P1"]["state"] == "alert"
    assert len(detail.build_default_detail_loaders()["spc_cpk_trend"]("P1", date(2026, 9, 8))["alerts_df"]) == 1
    write(True)
    assert build()["cells"]["spc_cpk_trend", "P1"]["state"] == "ok"


def test_cpk_unknown_and_missing_week_are_not_backend_ok():
    row = {"prod_code": "P1", "factory": "ARRAY", "step_id": "100", "param_name": "CD",
           "period_type": "week", "period_label": "2026-W36", "cpk_corrected": None, "flag": False}
    context = service.AlertMatrixContext(date(2026, 9, 8), spc_cpk_loader=lambda p: pd.DataFrame([row]))
    assert service.MATRIX_ROW_MAP["spc_cpk_trend"].evaluator("P1", context)["state"] == "no_data"
    row["period_label"] = "2026-W35"
    assert service.MATRIX_ROW_MAP["spc_cpk_trend"].evaluator("P1", context)["state"] == "no_data"


def test_qtime_actual_reference_day_matches_report_without_changing_week(monkeypatch):
    cache._cached_alert_matrix_cell.clear()
    calls = []
    def evaluator(product, context):
        calls.append(context.reference_date)
        return {"state": "ok", "detail_key": f"qtime_sheet_oos|{product}"}
    row = service.AlertMatrixRow("qtime_sheet_oos", "Q-Time", "qtime", "weekly", evaluator)
    monkeypatch.setattr(service, "MATRIX_ROWS", (row,))
    monkeypatch.setattr(cache, "build_default_matrix_context", lambda products, reference_date: service.AlertMatrixContext(reference_date))
    def build(day):
        return cache.get_cached_alert_matrix(products=("P1",), reference_date=day,
            _signature_provider=lambda *args: "stable", _revision_provider=lambda *args: "0")
    first = build(date(2026, 9, 8))
    build(date(2026, 9, 8))
    second = build(date(2026, 9, 9))
    assert calls == [date(2026, 9, 8), date(2026, 9, 9)]
    assert first["reference_week"] == second["reference_week"]
    assert second["reference_date"] == "2026-09-09"
