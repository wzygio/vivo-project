from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.infrastructure.monitor import summary_workbook_store as store_module
from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
    MonitorSummaryWorkbookStore,
)
from src.shared_kernel.utils.excel_tools import WorkbookWriteResult


def _write_legacy_workbook(path: Path) -> None:
    alarm = pd.DataFrame(
        [
            {
                "产品": "M626", "周期类型": "季度", "时间标签": "2026-Q2",
                "显示标签": "Q2", "过货量": 100, "OOC报警率": 0.01,
                "SOOS报警率": 0.0, "OOS报警率": 0.02,
            },
            {
                "产品": "M626", "周期类型": "季度", "时间标签": "2026-Q3",
                "显示标签": "Q3", "过货量": 20, "OOC报警率": 0.0,
                "SOOS报警率": 0.0, "OOS报警率": 0.0,
            },
            {
                "产品": "M626", "周期类型": "月度", "时间标签": "2026-08",
                "显示标签": "M8", "过货量": 30, "OOC报警率": 0.0,
                "SOOS报警率": 0.0, "OOS报警率": 0.0,
            },
        ]
    )
    with pd.ExcelWriter(path) as writer:
        alarm.to_excel(writer, sheet_name="报警率", index=False)
        pd.DataFrame([{"marker": "keep-cpk"}]).to_excel(writer, sheet_name="CPK", index=False)


def _current_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"产品": "M626", "监控类型": "ALL", "厂别": "ALL", "周期类型": period_type,
             "时间标签": time_label, "显示标签": display_label, "过货量": 40,
             "OOC报警片数": 1, "OOC报警率": 0.025,
             "SOOS报警片数": 0, "SOOS报警率": 0.0,
             "OOS报警片数": 2, "OOS报警率": 0.05,
             "Total报警片数": 3, "Total报警率": 0.075}
            for period_type, time_label, display_label in [
                ("年度", "2026", "Y26"),
                ("季度", "2026-Q3", "Q3"),
                ("月度", "2026-09", "M9"),
                ("周度", "2026-W37", "W37"),
            ]
        ]
    )


def test_upsert_updates_only_open_periods_and_preserves_other_sheet(tmp_path) -> None:
    path = tmp_path / "summary.xlsx"
    _write_legacy_workbook(path)
    store = MonitorSummaryWorkbookStore(path)

    result = store.upsert_current_periods(
        _current_rows(), as_of=pd.Timestamp("2026-09-07")
    )

    q2 = result[result["时间标签"].eq("2026-Q2")].iloc[0]
    assert q2["过货量"] == 100
    assert pd.isna(q2["OOC报警片数"])
    assert pd.isna(q2["Total报警片数"])
    q3 = result[result["时间标签"].eq("2026-Q3")].iloc[0]
    assert q3["过货量"] == 40
    assert set(result["监控类型"]) == {"ALL"}
    assert pd.read_excel(path, sheet_name="CPK").to_dict("records") == [
        {"marker": "keep-cpk"}
    ]


def test_upsert_rejects_closed_period_update(tmp_path) -> None:
    path = tmp_path / "summary.xlsx"
    _write_legacy_workbook(path)
    store = MonitorSummaryWorkbookStore(path)
    closed = _current_rows().iloc[[0]].assign(
        周期类型="季度", 时间标签="2026-Q2", 显示标签="Q2"
    )

    with pytest.raises(ValueError, match="current year/quarter/month/week"):
        store.upsert_current_periods(closed, as_of=pd.Timestamp("2026-09-07"))


def test_identical_upsert_does_not_rewrite_or_change_signature(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "summary.xlsx"
    with pd.ExcelWriter(path) as writer:
        _current_rows().to_excel(writer, sheet_name="报警率", index=False)
        pd.DataFrame([{"marker": "keep-cpk"}]).to_excel(
            writer, sheet_name="CPK", index=False
        )
    store = MonitorSummaryWorkbookStore(path)
    signature = store.source_signature()
    monkeypatch.setattr(
        store_module,
        "_replace_summary_sheet",
        lambda *args, **kwargs: pytest.fail("identical rows must not be rewritten"),
    )

    store.upsert_current_periods(_current_rows(), as_of=pd.Timestamp("2026-09-07"))

    assert store.source_signature() == signature


def test_non_openpyxl_workbook_uses_preserving_com_writer(tmp_path, monkeypatch) -> None:
    path = tmp_path / "protected.xlsx"
    path.write_bytes(b"protected")
    expected = WorkbookWriteResult(True, path, ("报警率",))
    monkeypatch.setattr(
        "openpyxl.load_workbook",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("protected")),
    )
    monkeypatch.setattr(
        store_module,
        "_replace_encrypted_summary_sheet_via_com",
        lambda candidate, frame: expected,
    )

    result = store_module._replace_summary_sheet(path, _current_rows())

    assert result == expected
