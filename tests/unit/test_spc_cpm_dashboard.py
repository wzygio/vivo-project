from datetime import date

import pandas as pd

from app.sections.spc_cpm_dashboard import (
    _create_lot_cpm_chart,
    _sheet_detail_for_lot,
    filter_cpm_report,
    get_available_factories,
    get_default_cpm_start_date,
    get_params_for_factory_steps,
    get_steps_for_factory,
)


def _sample_lot_cpm_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_Rs", "lot_id": "L1", "cpm": 1.2},
            {"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_UNI", "lot_id": "L2", "cpm": 1.8},
            {"factory": "ARRAY", "step_id": "17450", "param_name": "CD1", "lot_id": "L3", "cpm": 2.1},
            {"factory": "OLED", "step_id": "21200", "param_name": "PPA_B_X", "lot_id": "L4", "cpm": 1.5},
            {"factory": "TP", "step_id": "41140", "param_name": "SE_L1T", "lot_id": "L5", "cpm": 1.1},
        ]
    )


def test_default_cpm_start_date_uses_first_day_of_three_month_window() -> None:
    assert get_default_cpm_start_date(date(2026, 6, 16)) == date(2026, 4, 1)
    assert get_default_cpm_start_date(date(2026, 1, 10)) == date(2025, 11, 1)


def test_filter_options_follow_factory_step_param_cascade() -> None:
    lot_cpm_df = _sample_lot_cpm_df()

    assert get_available_factories(lot_cpm_df) == ["ARRAY", "OLED", "TP"]
    assert get_steps_for_factory(lot_cpm_df, "ARRAY") == ["15260", "17450"]
    assert get_steps_for_factory(lot_cpm_df, "OLED") == ["21200"]
    assert get_params_for_factory_steps(lot_cpm_df, "ARRAY", ["15260"]) == ["4PP_Rs", "4PP_UNI"]


def test_filter_cpm_report_uses_single_factory_and_selected_steps_params() -> None:
    filtered = filter_cpm_report(
        lot_cpm_df=_sample_lot_cpm_df(),
        selected_factory="ARRAY",
        selected_params=["4PP_Rs", "4PP_UNI"],
        selected_steps=["15260"],
    )

    assert filtered["factory"].tolist() == ["ARRAY", "ARRAY"]
    assert filtered["step_id"].tolist() == ["15260", "15260"]
    assert filtered["param_name"].tolist() == ["4PP_Rs", "4PP_UNI"]


def test_lot_cpm_chart_is_bar_chart() -> None:
    indicator_df = pd.DataFrame(
        [
            {"lot_id": "L1", "cpm": 1.2, "sheet_count": 2, "lot_mean": 10.0, "lot_std": 0.5},
            {"lot_id": "L2", "cpm": 1.8, "sheet_count": 3, "lot_mean": 11.0, "lot_std": 0.4},
        ]
    )

    fig = _create_lot_cpm_chart(indicator_df, "ARRAY | 15260 | 4PP_Rs")

    assert fig.data[0].type == "bar"
    assert list(fig.data[0].x) == ["L1", "L2"]


def test_lot_metric_chart_uses_single_color_without_reference_line() -> None:
    indicator_df = pd.DataFrame(
        [
            {"lot_id": "L1", "cpk": 0.8, "sheet_count": 2, "lot_mean": 10.0, "lot_std": 0.5},
            {"lot_id": "L2", "cpk": 1.8, "sheet_count": 3, "lot_mean": 11.0, "lot_std": 0.4},
        ]
    )

    fig = _create_lot_cpm_chart(indicator_df, "ARRAY | 15260 | 4PP_Rs", metric_key="cpk", metric_label="CPK")

    assert fig.data[0].marker.color == "#2563eb"
    assert list(fig.data[0].y) == [0.8, 1.8]
    assert len(fig.layout.shapes) == 0


def test_sheet_detail_for_lot_keeps_only_requested_columns() -> None:
    sheet_measurements_df = pd.DataFrame(
        [
            {
                "sheet_id": "LOT00000102",
                "sheet_start_time": "2026-06-02 09:00:00",
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_mean": 11.0,
                "sheet_max": 12.0,
                "sheet_min": 10.0,
                "usl": 15.0,
                "lsl": 5.0,
            },
            {
                "sheet_id": "LOT00000101",
                "sheet_start_time": "2026-06-02 08:00:00",
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "sheet_mean": 10.0,
                "sheet_max": 11.0,
                "sheet_min": 9.0,
                "usl": 15.0,
                "lsl": 5.0,
            },
        ]
    )

    detail = _sheet_detail_for_lot(sheet_measurements_df, "LOT000001", "15260", "4PP_Rs")

    assert detail.columns.tolist() == ["lot_id", "sheet_id", "厂别", "站点", "参数名称", "sheet_mean"]
    assert detail["sheet_id"].tolist() == ["LOT00000101", "LOT00000102"]
