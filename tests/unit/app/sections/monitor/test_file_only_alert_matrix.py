from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from app.sections.inline_domain.monitor import alert_matrix_cache as cache
from app.sections.inline_domain.monitor import alert_matrix_service as matrix
from src.shared_kernel.config import ConfigLoader


def test_default_yield_cells_never_call_report_analysis(tmp_path, monkeypatch):
    from src.yield_domain.application.yield_service import YieldAnalysisService
    from src.yield_domain.composition import build_yield_alert_results
    for name in ("get_data_health", "get_lot_defect_rates", "get_mwd_trend_data", "get_code_level_trend_data"):
        monkeypatch.setattr(YieldAnalysisService, name, lambda *a, **k: pytest.fail("matrix called Yield analysis"))
    monkeypatch.setattr(pd, "read_sql", lambda *a, **k: pytest.fail("matrix called SQL"))
    monkeypatch.setattr(cache, "DatabaseManager", lambda: pytest.fail("matrix initialized database"))
    original = ConfigLoader.get_domain_resource_path
    def resource(domain, key, default=None):
        if domain == "yield_domain" and key in {"lot_oos_results", "trend_alert_results"}:
            return tmp_path / f"{key}.xlsx"
        return original(domain, key, default)
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", resource)
    day = date.today()
    context = cache.build_default_matrix_context(["M678"], reference_date=day)
    for key in ("yield_lot_oos", "yield_trend_fluctuation"):
        assert matrix.MATRIX_ROW_MAP[key].evaluator("M678", context)["state"] == "error"
    publisher = build_yield_alert_results()
    now = datetime.now().astimezone()
    start = day - timedelta(days=30)
    for kind in ("lot", "trend"):
        publisher.publish("M678", kind, [], reference_date=day, coverage_start=str(start),
            coverage_end=str(day), has_data=True,
            health={"status": "fresh", "refreshed_at": now.isoformat()}, calculation_signature="test")
    for key in ("yield_lot_oos", "yield_trend_fluctuation"):
        assert matrix.MATRIX_ROW_MAP[key].evaluator("M678", context)["state"] == "ok"
    previous_week = day - timedelta(days=day.weekday()+7)
    publisher.publish("M678", "lot", [{"超规 Lot ID": "L1", "异常 Code": "D1",
        "管控规格线": "1.00%", "实际不良率": "2.00%", "不良panel数": 2,
        "入库时间": (previous_week+timedelta(days=2)).strftime("%Y/%m/%d"), "阵列投入时间": "-"}],
        reference_date=day, coverage_start=str(start), coverage_end=str(day), has_data=True,
        health={"status":"fresh", "refreshed_at":now.isoformat()}, calculation_signature="alert")
    assert matrix.MATRIX_ROW_MAP["yield_lot_oos"].evaluator("M678", context)["state"] == "alert"


def test_default_inline_reads_each_workbook_once_for_all_products(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.monitor import excel_alarm_store as workbooks
    from src.inline_domain.infrastructure.shared import sheet_oos_decoration_repository as legacy
    from src.inline_domain.composition import _build_oos_history_service
    products = [f"P{index}" for index in range(7)]
    paths = {scope: tmp_path / f"{scope}.xlsx" for scope in ("aoi_rs", "aoi_tt", "spc", "ctq")}
    for path in paths.values():
        pd.DataFrame().to_excel(path)
    monkeypatch.setattr(cache, "scope_decoration_path", lambda scope: paths[scope])
    monkeypatch.setattr("src.inline_domain.infrastructure.shared.resource_paths.scope_decoration_path", lambda scope: paths[scope])
    monkeypatch.setattr("src.inline_domain.infrastructure.shared.resource_paths.decision_workbook_paths", lambda alarm_type: paths)
    monkeypatch.setattr(matrix, "load_sheet_oos_decoration", lambda *a, **k: pytest.fail("per-product Excel read"))
    calls = []
    def read(path, *args, **kwargs):
        assert kwargs.get("sheet_name") is None
        calls.append(Path(path))
        return {product: pd.DataFrame() for product in products}
    workbooks.read_cached_alarm_workbook.clear()
    _build_oos_history_service.cache_clear()
    monkeypatch.setattr(pd, "read_excel", read)
    monkeypatch.setattr(workbooks.excel_tools, "_read_all_sheets_via_com", lambda *a, **k: pytest.fail("unexpected Office call"))
    try:
        context = cache.build_default_matrix_context(products, reference_date=date.today())
        for scope in paths:
            for product in products:
                # A successfully read empty product result is an explicit zero.
                assert matrix.MATRIX_ROW_MAP[f"{scope}_sheet_oos"].evaluator(product, context)["state"] == "ok"
        assert len(calls) == 4
        import os
        target = paths["spc"]
        stat = target.stat()
        os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
        for product in products:
            matrix.MATRIX_ROW_MAP["spc_sheet_oos"].evaluator(product, context)
        assert len(calls) == 5
    finally:
        workbooks.read_cached_alarm_workbook.clear()
        _build_oos_history_service.cache_clear()
