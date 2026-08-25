"""Inline 共享预警中心组件测试：按键过滤与展示表构建。"""

import pandas as pd

from app.sections.inline_domain.shared.alert_center import (
    build_sheet_oos_alert_display,
    filter_report_by_alert_keys,
)

KEY_MAP = {"厂别": "factory", "站点": "step_id", "参数名称": "param_name"}


def _alerts() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"厂别": "ARRAY", "站点": "11629", "参数名称": "PPA_B_X"},
            {"厂别": "CELL", "站点": "22501", "参数名称": "PPA_G_Y"},
        ]
    )


def _report() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "11629", "param_name": "PPA_B_X", "v": 1},
            {"factory": "ARRAY", "step_id": "11629", "param_name": "PPA_B_Y", "v": 2},
            {"factory": "CELL", "step_id": "22501", "param_name": "PPA_G_Y", "v": 3},
            {"factory": "CELL", "step_id": "22501", "param_name": "OTHER", "v": 4},
        ]
    )


def test_filter_matches_exact_key_combinations() -> None:
    result = filter_report_by_alert_keys(_report(), _alerts(), KEY_MAP)
    assert sorted(result["v"].tolist()) == [1, 3]


def test_filter_empty_inputs_return_empty() -> None:
    assert filter_report_by_alert_keys(_report(), pd.DataFrame(), KEY_MAP).empty
    assert filter_report_by_alert_keys(pd.DataFrame(), _alerts(), KEY_MAP).empty


def test_filter_missing_columns_return_empty() -> None:
    bad_report = _report().drop(columns=["param_name"])
    assert filter_report_by_alert_keys(bad_report, _alerts(), KEY_MAP).empty
    bad_alerts = _alerts().drop(columns=["站点"])
    assert filter_report_by_alert_keys(_report(), bad_alerts, KEY_MAP).empty


def test_filter_deduplicates_alert_keys() -> None:
    dup = pd.concat([_alerts(), _alerts()], ignore_index=True)
    result = filter_report_by_alert_keys(_report(), dup, KEY_MAP)
    assert sorted(result["v"].tolist()) == [1, 3]


def test_build_display_renames_and_orders_columns() -> None:
    raw = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "11629",
                "param_name": "PPA_B_X",
                "sheet_id": "S1",
                "sheet_start_time": pd.Timestamp("2026-08-20 10:00"),
                "oos_type": "USL",
            }
        ]
    )
    display = build_sheet_oos_alert_display(
        raw,
        column_map={
            "factory": "厂别",
            "step_id": "站点",
            "param_name": "参数名称",
            "sheet_id": "Sheet ID",
            "sheet_start_time": "超规时间",
            "oos_type": "超规类型",
        },
        output_columns=["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"],
    )
    assert list(display.columns) == ["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]
    assert display.loc[0, "厂别"] == "ARRAY"
    assert display.loc[0, "Sheet ID"] == "S1"


def test_build_display_skips_missing_columns() -> None:
    raw = pd.DataFrame([{"factory": "ARRAY", "step_id": "11629"}])
    display = build_sheet_oos_alert_display(
        raw,
        column_map={"factory": "厂别", "step_id": "站点", "param_name": "参数名称"},
        output_columns=["厂别", "站点", "参数名称"],
    )
    assert list(display.columns) == ["厂别", "站点"]


def test_build_display_empty_input() -> None:
    display = build_sheet_oos_alert_display(
        pd.DataFrame(),
        column_map={"factory": "厂别"},
        output_columns=["厂别"],
    )
    assert display.empty
