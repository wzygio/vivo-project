from __future__ import annotations

import pandas as pd

from src.inline_domain.application.monitor.summary_workbook_service import (
    MonitorSummaryWorkbookService,
)


class _Store:
    def __init__(self) -> None:
        self.rows = pd.DataFrame()

    def source_signature(self) -> str:
        return "workbook-signature"

    def upsert_current_periods(
        self, rows: pd.DataFrame, *, as_of: pd.Timestamp
    ) -> pd.DataFrame:
        assert as_of == pd.Timestamp("2026-09-07")
        self.rows = rows.copy()
        return rows.copy()


def test_refresh_summary_uses_stable_all_selection_keys() -> None:
    store = _Store()
    service = MonitorSummaryWorkbookService(store)

    result = service.refresh_summary(
        products=["M626"],
        scopes=["spc", "ctq", "aoi_tt", "aoi_rs"],
        factories=["TP", "ARRAY", "OLED"],
        alerts_df=pd.DataFrame(),
        throughput_df=pd.DataFrame(),
        end_date=pd.Timestamp("2026-09-07"),
    )

    assert set(store.rows["监控类型"]) == {"ALL"}
    assert set(store.rows["厂别"]) == {"ALL"}
    assert result.columns.tolist() == [
        "报警类型", "Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W37"
    ]


def test_refresh_summary_keeps_partial_filter_identity_distinct() -> None:
    store = _Store()
    service = MonitorSummaryWorkbookService(store)

    result = service.refresh_summary(
        products=["M626"],
        scopes=["ctq", "spc"],
        factories=["OLED", "ARRAY"],
        alerts_df=pd.DataFrame(),
        throughput_df=pd.DataFrame(),
        end_date=pd.Timestamp("2026-09-07"),
    )

    assert set(store.rows["监控类型"]) == {"CTQ,SPC"}
    assert set(store.rows["厂别"]) == {"ARRAY,OLED"}
    assert result.set_index("报警类型").loc["过货量", "Q2"] == "—"
    assert service.source_signature() == "workbook-signature"
