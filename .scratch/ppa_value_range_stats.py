"""Count param_value (column J) value ranges per product sheet in the
enterprise-encrypted workbook output/ppa_raw_measurements_202607.xlsx,
using Excel COM, and write the result into a new sheet of the same file.

Buckets (lower-bound inclusive):
    < 6.5 | [6.5, 7.5) | [7.5, 8.5) | [8.5, 9.5) | >= 9.5
"""
from __future__ import annotations

import sys
from pathlib import Path

import win32com.client  # type: ignore

WORKBOOK_PATH = Path(r"D:\wzy\Python\vivo-project\output\ppa_raw_measurements_202607.xlsx")
RESULT_SHEET = "param_value区间统计"
PARAM_COL = 10  # J
HEADER_ROW = 1

BUCKETS = ["<6.5", "6.5-7.5", "7.5-8.5", "8.5-9.5", ">=9.5"]


def classify(v: float) -> int:
    if v < 6.5:
        return 0
    if v < 7.5:
        return 1
    if v < 8.5:
        return 2
    if v < 9.5:
        return 3
    return 4


def main() -> int:
    excel = win32com.client.Dispatch("Excel.Application")

    wb = None
    for book in excel.Workbooks:
        try:
            if book.FullName.lower() == str(WORKBOOK_PATH).lower():
                wb = book
                break
        except Exception:
            continue
    if wb is None:
        wb = excel.Workbooks.Open(str(WORKBOOK_PATH), ReadOnly=False)

    # Verify column J header on each data sheet.
    data_sheets = [
        ws.Name for ws in wb.Worksheets if ws.Name != RESULT_SHEET
    ]
    print("sheets:", data_sheets)
    for name in data_sheets:
        ws = wb.Worksheets(name)
        header = ws.Cells(HEADER_ROW, PARAM_COL).Value
        print(f"{name}: J{HEADER_ROW} header = {header!r}")

    rows_out = []
    for name in data_sheets:
        ws = wb.Worksheets(name)
        last_row = ws.Cells(ws.Rows.Count, PARAM_COL).End(-4162).Row  # xlUp
        if last_row <= HEADER_ROW:
            rows_out.append([name, 0, 0, 0, 0, 0, 0, 0])
            continue
        values = ws.Range(
            ws.Cells(HEADER_ROW + 1, PARAM_COL), ws.Cells(last_row, PARAM_COL)
        ).Value

        counts = [0] * 5
        invalid = 0
        for (v,) in values:
            if v is None or v == "":
                invalid += 1
                continue
            try:
                counts[classify(float(v))] += 1
            except (TypeError, ValueError):
                invalid += 1
        total = sum(counts)
        rows_out.append([name, *counts, total, invalid])
        print(name, "counts:", counts, "total:", total, "invalid:", invalid)

    # (Re)create result sheet at the end.
    try:
        old = wb.Worksheets(RESULT_SHEET)
        old.Delete()
        excel.DisplayAlerts = True
    except Exception:
        pass
    ws_out = wb.Worksheets.Add(After=wb.Worksheets(wb.Worksheets.Count))
    ws_out.Name = RESULT_SHEET

    header = ["产品", *BUCKETS, "合计", "无效/空值数"]
    for c, h in enumerate(header, start=1):
        ws_out.Cells(1, c).Value = h
    for r, row in enumerate(rows_out, start=2):
        for c, v in enumerate(row, start=1):
            ws_out.Cells(r, c).Value = v
    note_row = len(rows_out) + 3
    ws_out.Cells(note_row, 1).Value = (
        "说明：区间按左闭右开统计，最后一档为 >=9.5（即用户口径中的“大于9.5”）。"
    )
    ws_out.Columns("A:H").AutoFit()

    wb.Save()
    print("saved:", wb.FullName)
    return 0


if __name__ == "__main__":
    sys.exit(main())
