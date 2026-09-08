from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.monitor.oos_monitor_service import OosMonitorService


class _Reader:
    def read_product(self, scope: str, prod_code: str):
        rows = pd.DataFrame(
            [
                {
                    "scope": scope,
                    "factory": "ARRAY",
                    "prod_code": prod_code,
                    "step_id": "1100",
                    "metric_name": "PPA",
                    "item_id": f"{prod_code}-1",
                    "event_time": pd.Timestamp("2026-08-10"),
                    "observed_value": 3.0,
                    "usl": 2.5,
                    "lsl": 1.5,
                    "oos_type": "USL",
                    "flag": False,
                }
            ]
        )
        return SimpleNamespace(
            alerts_df=rows,
            source="history",
            refreshed_at=pd.Timestamp("2026-09-02 08:00:00"),
        )


class _OocReader(_Reader):
    def read_product(self, scope: str, prod_code: str):
        result = super().read_product(scope, prod_code)
        result.alerts_df["item_id"] = f"{prod_code}-2"
        result.alerts_df["alarm_type"] = "OOC"
        return result


class _ThroughputReader:
    def read_product(self, scope: str, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "scope": scope,
                    "factory": "ARRAY",
                    "prod_code": prod_code,
                    "event_date": pd.Timestamp("2026-08-10"),
                    "item_id": "S1",
                }
            ]
        )


class _SummaryWorkbook:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def refresh_summary(self, **kwargs) -> pd.DataFrame:
        self.calls.append(kwargs)
        return pd.DataFrame({"报警类型": ["过货量"], "Y26": [999]})


def test_build_dashboard_aggregates_shared_oos_facts() -> None:
    view = OosMonitorService(_Reader()).build_dashboard(
        products=["M626", "M678"],
        scopes=["spc"],
        factories=["ARRAY"],
        start_date="2026-08-01",
        end_date="2026-08-31",
    )

    assert len(view.detail_df) == 2
    assert view.summary_df["oos_count"].sum() == 2
    assert view.trend_df["oos_count"].sum() == 2
    assert view.station_df["oos_count"].sum() == 2
    assert len(view.refresh_status_df) == 2


def test_build_dashboard_date_and_factory_filters_do_not_hide_refresh_status() -> None:
    view = OosMonitorService(_Reader()).build_dashboard(
        products=["M626"],
        scopes=["ctq"],
        factories=["OLED"],
        start_date="2026-09-01",
        end_date="2026-09-02",
    )

    assert view.detail_df.empty
    assert view.summary_df.empty
    assert view.refresh_status_df.to_dict("records") == [
        {
            "prod_code": "M626",
            "scope": "ctq",
            "alarm_type": "OOS",
            "source": "history",
            "refreshed_at": pd.Timestamp("2026-09-02 08:00:00"),
        }
    ]


def test_build_dashboard_combines_oos_and_ooc() -> None:
    summary_workbook = _SummaryWorkbook()
    view = OosMonitorService(
        _Reader(),
        ooc_reader=_OocReader(),
        throughput_reader=_ThroughputReader(),
        summary_workbook=summary_workbook,
    ).build_dashboard(
        products=["M626"],
        scopes=["spc"],
        factories=["ARRAY"],
        start_date="2026-08-01",
        end_date="2026-08-31",
    )

    assert set(view.detail_df["alarm_type"]) == {"OOS", "OOC"}
    assert set(view.refresh_status_df["alarm_type"]) == {"OOS", "OOC"}
    assert view.period_summary_df.to_dict("records") == [
        {"报警类型": "过货量", "Y26": 999}
    ]
    call = summary_workbook.calls[0]
    assert call["products"] == ("M626",)
    assert call["scopes"] == ("spc",)
    assert call["factories"] == ("ARRAY",)
    assert set(call["alerts_df"]["alarm_type"]) == {"OOS", "OOC"}
