from __future__ import annotations

from datetime import date
from pathlib import Path

import openpyxl
import pandas as pd

from tools.generate_ppa_oos_weekly_summary import (
    build_ppa_ratio_summary,
    previous_calendar_week,
    write_summary_workbook,
)


def test_previous_calendar_week_uses_complete_monday_to_sunday_window() -> None:
    start, end_exclusive = previous_calendar_week(date(2026, 8, 18))

    assert start == pd.Timestamp("2026-08-10")
    assert end_exclusive == pd.Timestamp("2026-08-17")


def test_build_ppa_ratio_summary_uses_raw_measurement_point_denominator() -> None:
    oos_sheets = {
        "M626": pd.DataFrame(
            {
                "param_name": [
                    "PPA_B_X",
                    "ppa_b_x",
                    "PPA_G_X",
                    "PPA_R_X",
                    "OTHER",
                ],
                "sheet_start_time": [
                    "2026-08-10 00:00:00",
                    "2026-08-16 23:59:59",
                    "2026-08-17 00:00:00",
                    "2026-08-01 12:00:00",
                    "2026-08-12 12:00:00",
                ],
                "step_id": ["S1"] * 5,
                "sheet_id": ["A", "B", "C", "D", "E"],
                "usl": [1.0] * 5,
                "lsl": [0.0] * 5,
            }
        ),
        "M678": pd.DataFrame(
            {
                "param_name": ["PPA_B_X", "PPA_G_X", "OTHER_PARAM"],
                "sheet_start_time": [
                    "2026-08-11 10:00:00",
                    "invalid",
                    "2026-08-12 10:00:00",
                ],
                "step_id": ["S1"] * 3,
                "sheet_id": ["F", "G", "H"],
                "usl": [1.0] * 3,
                "lsl": [0.0] * 3,
            }
        ),
    }
    measurement_sheets = {
        "M626": pd.DataFrame(
            {
                "param_name": [
                    "PPA_B_X",
                    "PPA_B_X",
                    "PPA_B_X",
                    "PPA_B_X",
                    "PPA_R_X",
                ],
                "start_time": ["2026-08-11"] * 5,
                "step_id": ["S1"] * 5,
                "sheet_id": ["A", "A", "B", "C", "D"],
                "param_value": [1.2, 0.5, -0.1, 1.3, 1.4],
            }
        ),
        "M678": pd.DataFrame(
            {
                "param_name": ["PPA_B_X", "PPA_B_X"],
                "start_time": ["2026-08-11", "2026-08-12"],
                "step_id": ["S1", "S1"],
                "sheet_id": ["F", "I"],
                "param_value": [2.0, 0.5],
            }
        ),
    }

    summary = build_ppa_ratio_summary(
        oos_sheets,
        measurement_sheets,
        start=pd.Timestamp("2026-08-10"),
        end_exclusive=pd.Timestamp("2026-08-17"),
        product_order=("M626", "M678"),
    )

    assert summary["项目"].tolist() == ["PPA_B_X", "PPA_G_X", "PPA_R_X"]
    assert summary["分类"].tolist() == ["PPA", "PPA", "PPA"]
    assert summary["M626"].iloc[0] == 2 / 4
    assert pd.isna(summary["M626"].iloc[1])
    assert summary["M626"].iloc[2] == 0.0
    assert summary["M678"].iloc[0] == 1 / 2
    assert pd.isna(summary["M678"].iloc[1])
    assert pd.isna(summary["M678"].iloc[2])


def test_write_summary_workbook_creates_styled_table_and_metadata(
    tmp_path: Path,
) -> None:
    summary = pd.DataFrame(
        {
            "分类": ["PPA", "PPA"],
            "项目": ["PPA_B_X", "PPA_B_Y"],
            "M626": [0.25, 0.0],
            "M678": [0.125, float("nan")],
        }
    )
    output_path = tmp_path / "ppa_summary.xlsx"

    write_summary_workbook(
        summary,
        output_path,
        start=pd.Timestamp("2026-08-10"),
        end_exclusive=pd.Timestamp("2026-08-17"),
        source_path=Path("resources/spc_sheet_oos_decoration.xlsx"),
    )

    workbook = openpyxl.load_workbook(output_path, data_only=True)
    worksheet = workbook["PPA超规统计"]
    assert [worksheet.cell(1, column).value for column in range(1, 5)] == [
        "分类",
        "项目",
        "M626",
        "M678",
    ]
    assert str(next(iter(worksheet.merged_cells.ranges))) == "A2:A3"
    assert worksheet["A2"].value == "PPA"
    assert worksheet["C2"].value == 0.25
    assert worksheet["C2"].number_format == "0.00%"
    assert worksheet["D3"].value is None
    assert worksheet["A1"].fill.fgColor.rgb == "FF0B50F4"
    assert workbook["统计说明"]["B2"].value == "2026-08-10 至 2026-08-16"
    assert workbook["统计说明"]["B4"].value == (
        "超规原始测量点数 / 同产品同参数的有效原始测量点总数"
    )
