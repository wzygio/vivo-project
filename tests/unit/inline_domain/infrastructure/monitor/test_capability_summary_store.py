"""Standalone metric workbooks preserve independent maintained objects."""

import pandas as pd
import pytest
from openpyxl import load_workbook

from src.inline_domain.core.monitor.capability_trend import normalize_capability_history
from src.inline_domain.infrastructure.monitor.capability_summary_store import CapabilitySummaryStore
from src.inline_domain.infrastructure.monitor.summary_workbook_store import MonitorSummaryWorkbookError
from src.shared_kernel.utils.excel_tools import WorkbookWriteResult


def history(product="M626", *, metric="cpk", total=10):
    return normalize_capability_history(pd.DataFrame([
        {"产品": product, "周期类型": kind, "时间标签": label,
         f"{metric.upper()}总项目数": total, "达标项目数": total - 1, "预警项目数": 1,
         f"{metric.capitalize()}≥1.33达标率": (total - 1) / total}
        for kind, label in [("周度", "2026-W40"), ("月度", "2026-10"),
                            ("季度", "2026-Q4"), ("年度", "2026")]
    ]), metric=metric)


def test_reads_protected_workbook_in_one_cache_call_and_checks_product(tmp_path):
    path = tmp_path / "history.xlsx"
    path.write_bytes(b"protected")
    calls = []
    sheets = {"M626 CPK": history(), "M678 CPK": history("M678"),
              "M626 CPM": pd.DataFrame({"unrelated": [1]})}
    store = CapabilitySummaryStore(path, workbook_reader=lambda *args: calls.append(args) or sheets)
    assert set(store.read()["产品"]) == {"M626", "M678"}
    assert len(calls) == 1
    sheets["M626 CPK"] = history("M673")
    with pytest.raises(MonitorSummaryWorkbookError, match="contains another product"):
        store.read()


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_atomic_write_omits_labels_preserves_objects_and_is_idempotent(tmp_path, metric):
    path = tmp_path / "history.xlsx"
    pd.DataFrame({"formula": ["=1+2"]}).to_excel(path, sheet_name="Notes", index=False)
    store = CapabilitySummaryStore(path, metric=metric)
    initial = history(metric=metric)
    written = store.write_history(initial)
    pd.testing.assert_frame_equal(written, initial)
    workbook = load_workbook(path)
    assert workbook["Notes"]["A2"].value == "=1+2"
    assert "显示标签" not in [cell.value for cell in workbook[f"M626 {metric.upper()}"][1]]
    workbook.close()
    before = path.stat().st_mtime_ns
    store.write_history(initial)
    assert path.stat().st_mtime_ns == before
    store.write_history(history("M678", metric=metric))
    assert set(store.read()["产品"]) == {"M626", "M678"}


def test_refreshes_week_and_owning_month_without_rewriting_other_products(tmp_path):
    path = tmp_path / "history.xlsx"
    store = CapabilitySummaryStore(path)
    store.write_history(pd.concat([history(), history("M678")], ignore_index=True))
    replacement = history(total=20).loc[lambda rows: rows["周期类型"].eq("周度")]
    result = store.refresh_weeks(replacement)
    assert result.loc[result["产品"].eq("M626"), "CPK总项目数"].tolist() == [20] * 4
    assert result.loc[result["产品"].eq("M678"), "CPK总项目数"].tolist() == [10] * 4
    before = path.stat().st_mtime_ns
    store.refresh_weeks(replacement)
    assert path.stat().st_mtime_ns == before


def test_write_failure_is_explicit_and_preserves_original_bytes(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.monitor import capability_summary_store as module

    path = tmp_path / "history.xlsx"
    store = CapabilitySummaryStore(path)
    store.write_history(history())
    before = path.read_bytes()
    monkeypatch.setattr(module, "replace_product_period_sheets", lambda *args, **kwargs:
                        WorkbookWriteResult(False, path, (), "locked fixture workbook"))
    with pytest.raises(MonitorSummaryWorkbookError, match="locked fixture"):
        store.write_history(history(total=20))
    assert path.read_bytes() == before
