"""Exercise Q-Time's shared display reader through the actual matrix buttons."""

from datetime import date, datetime

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.indicator_domain import composition
from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults, QTIME_PUBLISHED_COLUMNS
from src.indicator_domain.infrastructure.qtime.alert_workbook import QTimeAlertWorkbook


@pytest.mark.parametrize("scenario", ["missing", "blank_tables", "template", "old_empty", "partial_alert"])
def test_qtime_matrix_uses_passing_empty_tables_without_sql(tmp_path, monkeypatch, scenario):
    results = QTimeAlertResults(
        QTimeAlertWorkbook(tmp_path / "qtime.xlsx"), ttl_seconds=3600,
        clock=lambda: datetime(2026, 11, 10, 12),
    )
    empty = pd.DataFrame(columns=QTIME_PUBLISHED_COLUMNS)
    if scenario == "template":
        results.store.initialize_empty_template()
    elif scenario == "blank_tables":
        with pd.ExcelWriter(results.store.path) as writer:
            for shop in ("ARRAY", "OLED", "TP"):
                pd.DataFrame().to_excel(writer, sheet_name=f"alerts_{shop}", index=False)
    elif scenario == "old_empty":
        for shop in ("ARRAY", "OLED", "TP"):
            results.publish(shop, empty, empty, {"status": "fresh", "refreshed_at": "2026-11-05T10:00:00"}, date(2026, 11, 5))
    elif scenario == "partial_alert":
        frame = pd.DataFrame([dict(
            shop="ARRAY", prodcode="M678", f_step="100", t_step="200", step_desc="PATH",
            lot_id="L1", q_spec=12., wait_time=14., over_hours=2., timekey="20261103100000", flag=False,
        )])
        results.publish("ARRAY", frame, frame, {"status": "fresh", "refreshed_at": "2026-11-10T11:30:00"}, date(2026, 11, 10))

    monkeypatch.setattr(composition, "build_qtime_alert_results", lambda: results)
    monkeypatch.setattr(pd, "read_sql", lambda *_a, **_k: pytest.fail("Q-Time display queried SQL"))
    app = AppTest.from_string('''
from datetime import date
from app.sections.inline_domain.monitor.alert_matrix import render_alert_matrix_section
from app.sections.inline_domain.monitor.alert_matrix_cache import build_default_matrix_context
from app.sections.inline_domain.monitor.alert_matrix_service import MATRIX_ROW_MAP
products = ["M678", "M626"]
context = build_default_matrix_context(products, reference_date=date(2026, 11, 10))
row = MATRIX_ROW_MAP["qtime_sheet_oos"]
cells = {(row.row_key, product): row.evaluator(product, context) for product in products}
render_alert_matrix_section({
    "products": products, "cells": cells,
    "rows": [{"row_key": row.row_key, "module_group": "qtime", "display_name": row.display_name}],
    "reference_week": {"start": "2026-11-02", "end": "2026-11-09"},
}, filter_selection=("Q-Time", products, ["ARRAY", "OLED", "TP"]))
''').run(timeout=15)
    assert not app.exception
    assert not app.warning and not app.error
    expected = ["🔴", "🟢"] if scenario == "partial_alert" else ["🟢", "🟢"]
    assert [button.label for button in app.button] == expected
    assert "达标" in app.button[-1].proto.help
    assert all("加载失败" not in button.proto.help for button in app.button)
    assert results.store.path.exists()

