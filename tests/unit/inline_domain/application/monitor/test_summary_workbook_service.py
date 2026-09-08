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

    def refresh_current_week(
        self, alerts, *, products, scope_key, factory_key, as_of, available_types
    ):
        assert as_of == pd.Timestamp("2026-09-07")
        self.available_types = available_types
        self.rows = pd.DataFrame([{"产品": products[0], "监控类型": scope_key,
                                   "厂别": factory_key, "周期类型": "年度",
                                   "时间标签": "2026", "过货量": 100,
                                   "OOS报警片数": 5, "OOC报警片数": 0,
                                   "SOOS报警片数": 0, "Total报警片数": 5}])
        return self.rows.copy(), ["缺少 OOC 明细"]


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
        "报警类型", "Y26", "Q1", "Q2", "Q3", "M7", "M8", "M9", "W34", "W35", "W36", "W37"
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


def test_missing_scope_does_not_make_alarm_type_available() -> None:
    store = _Store()
    service = MonitorSummaryWorkbookService(store)
    service.refresh_summary(
        products=["M626"], scopes=["spc", "ctq"], factories=["ARRAY"],
        alerts_df=pd.DataFrame(), throughput_df=pd.DataFrame(),
        end_date=pd.Timestamp("2026-09-07"), source_status_df=pd.DataFrame([
            {"prod_code": "M626", "scope": "spc", "alarm_type": "OOS", "source": "excel"},
            {"prod_code": "M626", "scope": "ctq", "alarm_type": "OOS", "source": "excel"},
            {"prod_code": "M626", "scope": "spc", "alarm_type": "OOC", "source": "excel"},
            {"prod_code": "M626", "scope": "ctq", "alarm_type": "OOC", "source": "missing"},
        ]),
    )
    assert store.available_types == {"M626": {"OOS"}}
    assert service.last_warnings == ("缺少 OOC 明细",)
