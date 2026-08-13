import pandas as pd
import sys
import types


streamlit_echarts_stub = types.ModuleType("streamlit_echarts")
streamlit_echarts_stub.st_echarts = lambda *args, **kwargs: None
streamlit_echarts_stub.JsCode = lambda code: code
sys.modules.setdefault("streamlit_echarts", streamlit_echarts_stub)

st_aggrid_stub = types.ModuleType("st_aggrid")
st_aggrid_stub.AgGrid = lambda *args, **kwargs: {}
st_aggrid_stub.GridOptionsBuilder = object
st_aggrid_stub.GridUpdateMode = types.SimpleNamespace(SELECTION_CHANGED="SELECTION_CHANGED")
st_aggrid_stub.DataReturnMode = types.SimpleNamespace()
st_aggrid_stub.JsCode = lambda code: code
sys.modules.setdefault("st_aggrid", st_aggrid_stub)

from app.sections.monitor.monitor_dashboard import (
    MonitorFilterState,
    _apply_compliance_visibility_filter,
    filter_and_rollup_monitor_data,
)
from src.shared_kernel.config import ConfigLoader


def test_filter_and_rollup_uses_all_data_for_type_switching() -> None:
    detail_df = pd.DataFrame(
        [
            {
                "time_group": "2026M06",
                "prod_code": "P1",
                "factory": "ARRAY",
                "data_type": "SPC",
                "抽检数": 10,
                "OOS片数": 0,
                "SOOS片数": 0,
                "OOC片数": 1,
            },
            {
                "time_group": "2026M06",
                "prod_code": "P1",
                "factory": "ARRAY",
                "data_type": "CTQ",
                "抽检数": 5,
                "OOS片数": 1,
                "SOOS片数": 0,
                "OOC片数": 0,
            },
            {
                "time_group": "2026M06",
                "prod_code": "P1",
                "factory": "ARRAY",
                "data_type": "报废",
                "抽检数": 2,
                "OOS片数": 0,
                "SOOS片数": 0,
                "OOC片数": 2,
            },
        ]
    )
    global_summary_df = pd.DataFrame({"time_group": ["2026M06"]})
    station_detail_df = detail_df.assign(step_id="S1")

    ctq_summary, ctq_detail, ctq_station = filter_and_rollup_monitor_data(
        detail_df,
        global_summary_df,
        station_detail_df,
        MonitorFilterState(
            selected_products=["P1"],
            selected_factories=["ARRAY"],
            data_type_filter="CTQ",
        ),
    )

    assert len(ctq_detail) == 1
    assert int(ctq_summary.loc[0, "抽检数"]) == 5
    assert int(ctq_summary.loc[0, "OOS片数"]) == 1
    assert int(ctq_station.loc[0, "抽检数"]) == 5

    all_summary, all_detail, all_station = filter_and_rollup_monitor_data(
        detail_df,
        global_summary_df,
        station_detail_df,
        MonitorFilterState(
            selected_products=["P1"],
            selected_factories=["ARRAY"],
            data_type_filter="ALL",
        ),
    )

    assert len(all_detail) == 1
    assert int(all_summary.loc[0, "抽检数"]) == 17
    assert int(all_summary.loc[0, "OOC片数"]) == 3
    assert int(all_summary.loc[0, "OOS片数"]) == 1
    assert int(all_station.loc[0, "抽检数"]) == 17


def test_alarm_detail_visibility_filter_hides_compliance_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigLoader,
        "get_compliance_config",
        staticmethod(
            lambda: {
                "rules": [
                    {
                        "factory": "ALL",
                        "prod_code": "Z571",
                        "data_type": "ALL",
                        "month": "M04",
                    }
                ]
            }
        ),
    )

    detail_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "prod_code": "Z571",
                "factory": "ARRAY",
                "data_type": "CTQ",
                "sheet_start_time": "2026-04-15",
                "spc_status": "OOC",
                "is_ooc": 1,
                "is_oos": 0,
            },
            {
                "sheet_id": "S2",
                "prod_code": "Z571",
                "factory": "ARRAY",
                "data_type": "CTQ",
                "sheet_start_time": "2026-05-15",
                "spc_status": "OOC",
                "is_ooc": 1,
                "is_oos": 0,
            },
            {
                "sheet_id": "S3",
                "prod_code": "M678",
                "factory": "ARRAY",
                "data_type": "CTQ",
                "sheet_start_time": "2026-04-15",
                "spc_status": "OOS",
                "is_ooc": 0,
                "is_oos": 1,
            },
        ]
    )

    visible_df = _apply_compliance_visibility_filter(detail_df)

    assert visible_df["sheet_id"].tolist() == ["S2", "S3"]
    assert "is_compliant_modified" not in visible_df.columns
    assert visible_df.columns.tolist() == detail_df.columns.tolist()
