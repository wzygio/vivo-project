"""Workbook-like tables with escaped content and bounded scrolling."""

from html import escape

import pandas as pd


def format_cell(value: object, column: str, category: str, report: str) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S" if report == "inspection" else "%Y-%m-%d")
    if isinstance(value, (int, float)):
        if report == "inspection" and column != "序号":
            return f"{value:.3f}"
        if report == "lifetime" and category == "衰减比例" and (column.endswith("#") or column == "AVG"):
            return f"{value:.2%}"
        return f"{value:g}"
    return str(value)


def build_report_table(frame: pd.DataFrame, report: str) -> str:
    headers = "".join(f'<th scope="col">{escape(str(c))}</th>' for c in frame.columns)
    rows = []
    for record in frame.to_dict("records"):
        category = str(record.get("类别", ""))
        cells = "".join(
            f"<td>{escape(format_cell(value, column, category, report))}</td>"
            for column, value in record.items()
        )
        rows.append(f"<tr>{cells}</tr>")
    # Custom HTML is needed to retain the source workbook's blue/white headers,
    # alternating fills and blank cells; native dataframes do not style headers.
    return """
<style>
.iqc-sheet {max-height:640px; overflow:auto; border:1px solid #ccd4df;
 border-radius:4px; background:white; color:#17212f;}
.iqc-sheet table {border-collapse:separate; border-spacing:0; width:max-content;
 min-width:100%; font-family:'Microsoft YaHei',sans-serif; font-size:13px;}
.iqc-sheet th,.iqc-sheet td {padding:11px 14px; white-space:nowrap;
 text-align:center; border-right:1px solid #dce2ea; border-bottom:1px solid #dce2ea;}
.iqc-sheet th {position:sticky; top:0; z-index:2; background:white; color:#17212f;}
.iqc-sheet.inspection th {background:#5c93b0; color:white;}
.iqc-sheet.inspection tbody tr:nth-child(odd) {background:#c6e2ff;}
.iqc-sheet tbody tr:hover {background:#edf3f8;}
.iqc-sheet th:first-child,.iqc-sheet td:first-child {position:sticky; left:0;}
.iqc-sheet td:first-child {background:inherit;}
.iqc-sheet tbody tr {background:white;}
.iqc-sheet th:first-child {z-index:3;}
.iqc-sheet.lifetime td {padding:9px 12px;}
</style>
""" + (
        f'<div class="iqc-sheet {escape(report)}" tabindex="0" role="region" '
        'aria-label="报表明细，可横向和纵向滚动">'
        f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )
