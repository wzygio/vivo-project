"""Generate weekly PPA Sheet-OOS ratios from decoration details and snapshots.

The default reporting window is the previous complete calendar week
(Monday 00:00 inclusive to the following Monday 00:00 exclusive).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_INPUT_PATH = PROJECT_ROOT / "resources" / "spc_sheet_oos_decoration.xlsx"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "ppa_oos_weekly_summary.xlsx"
DEFAULT_SNAPSHOT_DIR = PROJECT_ROOT / "data"
DEFAULT_PRODUCT_ORDER = ("M626", "M678", "M673", "Z517", "Z571")
DEFAULT_PARAMETER_ORDER = (
    "PPA_B_X",
    "PPA_B_Y",
    "PPA_G_X",
    "PPA_G_Y",
    "PPA_G1_X",
    "PPA_G1_Y",
    "PPA_R_X",
    "PPA_R_Y",
    "PPA_R1_X",
    "PPA_R1_Y",
)
OOS_REQUIRED_COLUMNS = frozenset(
    {"param_name", "sheet_start_time", "step_id", "sheet_id", "usl", "lsl"}
)
MEASUREMENT_REQUIRED_COLUMNS = frozenset(
    {"param_name", "start_time", "step_id", "sheet_id", "param_value"}
)
PRODUCT_SHEET_PATTERN = re.compile(r"^[A-Z]+\d+$", re.IGNORECASE)


def previous_calendar_week(reference_date: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the previous Monday and this Monday as a half-open interval."""
    reference = pd.Timestamp(reference_date).normalize()
    this_monday = reference - pd.Timedelta(days=reference.weekday())
    return this_monday - pd.Timedelta(days=7), this_monday


def read_source_workbook(path: Path) -> dict[str, pd.DataFrame]:
    """Read all sheets, falling back to the repository's Excel COM adapter."""
    if not path.is_file():
        raise FileNotFoundError(f"明细工作簿不存在: {path}")

    try:
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        return dict(sheets)
    except Exception as standard_error:
        try:
            from src.shared_kernel.utils.excel_tools import _read_all_sheets_via_com

            return _read_all_sheets_via_com(path)
        except Exception as com_error:
            try:
                return _read_open_workbook_via_com(path)
            except Exception as active_workbook_error:
                raise RuntimeError(
                    f"无法读取明细工作簿 {path}；请确认本机 Excel 可正常打开该文件。"
                ) from ExceptionGroup(
                    "标准读取、新建 COM 会话和现有 Excel 会话读取均失败",
                    [standard_error, com_error, active_workbook_error],
                )


def _read_open_workbook_via_com(path: Path) -> dict[str, pd.DataFrame]:
    """Read a workbook already open in Excel without saving or closing it."""
    import pythoncom
    import win32com.client

    pythoncom.CoInitialize()
    try:
        excel = win32com.client.GetActiveObject("Excel.Application")
        expected_path = os.path.normcase(str(path.resolve()))
        for workbook_index in range(1, excel.Workbooks.Count + 1):
            workbook = excel.Workbooks(workbook_index)
            if os.path.normcase(str(Path(workbook.FullName).resolve())) != expected_path:
                continue
            sheets: dict[str, pd.DataFrame] = {}
            for worksheet in workbook.Worksheets:
                data = worksheet.UsedRange.Value
                if data is None:
                    sheets[worksheet.Name] = pd.DataFrame()
                    continue
                headers = list(data[0])
                rows = [list(row) for row in data[1:]]
                sheets[worksheet.Name] = pd.DataFrame(rows, columns=headers)
            return sheets
        raise FileNotFoundError(f"当前 Excel 会话未打开目标工作簿: {path}")
    finally:
        pythoncom.CoUninitialize()


def resolve_product_order(
    sheets: Mapping[str, pd.DataFrame],
    preferred_order: Sequence[str] = DEFAULT_PRODUCT_ORDER,
) -> tuple[str, ...]:
    """Keep the requested products first and append other product-like sheets."""
    preferred = tuple(dict.fromkeys(str(product).strip() for product in preferred_order))
    extras = sorted(
        name
        for name, frame in sheets.items()
        if name not in preferred
        and PRODUCT_SHEET_PATTERN.fullmatch(name)
        and OOS_REQUIRED_COLUMNS.issubset(frame.columns)
    )
    return preferred + tuple(extras)


def load_measurement_snapshots(
    snapshot_dir: Path,
    product_order: Sequence[str],
) -> dict[str, pd.DataFrame]:
    """Load the L1 measurement snapshot required for each product denominator."""
    snapshots: dict[str, pd.DataFrame] = {}
    for product in product_order:
        snapshot_path = snapshot_dir / product / f"inline_measurements_{product}.parquet"
        if not snapshot_path.is_file():
            raise FileNotFoundError(f"底层测量快照不存在: {snapshot_path}")
        frame = pd.read_parquet(snapshot_path)
        missing = MEASUREMENT_REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise ValueError(f"底层测量快照 {snapshot_path} 缺少字段: {missing_text}")
        snapshots[product] = frame
    return snapshots


def _normalized_parameters(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.upper()


def _normalized_timestamps(series: pd.Series) -> pd.Series:
    """Normalize Excel/COM datetimes, including malformed COM tz metadata."""
    if is_datetime64_any_dtype(series.dtype) and hasattr(series.array, "asi8"):
        values = pd.to_datetime(series.array.asi8, unit="ns", errors="coerce")
        return pd.Series(values, index=series.index)
    return pd.to_datetime(series, errors="coerce")


def _parameter_sort_key(parameter: str) -> tuple[int, int | str]:
    try:
        return 0, DEFAULT_PARAMETER_ORDER.index(parameter)
    except ValueError:
        return 1, parameter


def _weekly_oos_specs(
    frame: pd.DataFrame,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
) -> tuple[set[str], pd.DataFrame]:
    missing = OOS_REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"超规明细缺少字段: {missing_text}")

    normalized = frame.assign(
        _parameter=_normalized_parameters(frame["param_name"]),
        _timestamp=_normalized_timestamps(frame["sheet_start_time"]),
        step_id=frame["step_id"].fillna("").astype(str),
        sheet_id=frame["sheet_id"].fillna("").astype(str),
        usl=pd.to_numeric(frame["usl"], errors="coerce"),
        lsl=pd.to_numeric(frame["lsl"], errors="coerce"),
    )
    is_ppa = normalized["_parameter"].str.contains(
        "PPA", case=False, regex=False, na=False
    )
    parameters = set(normalized.loc[is_ppa, "_parameter"].dropna().tolist())
    weekly_specs = normalized.loc[
        is_ppa
        & normalized["_timestamp"].ge(start)
        & normalized["_timestamp"].lt(end_exclusive)
    ][["step_id", "_parameter", "sheet_id", "usl", "lsl"]].drop_duplicates(
        ["step_id", "_parameter", "sheet_id"], keep="last"
    )
    return parameters, weekly_specs


def _weekly_measurement_ratios(
    frame: pd.DataFrame,
    oos_specs: pd.DataFrame,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
) -> dict[str, float]:
    missing = MEASUREMENT_REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"底层测量数据缺少字段: {missing_text}")

    normalized = frame.assign(
        _parameter=_normalized_parameters(frame["param_name"]),
        _timestamp=_normalized_timestamps(frame["start_time"]),
        _value=pd.to_numeric(frame["param_value"], errors="coerce"),
        step_id=frame["step_id"].fillna("").astype(str),
        sheet_id=frame["sheet_id"].fillna("").astype(str),
    )
    is_ppa = normalized["_parameter"].str.contains(
        "PPA", case=False, regex=False, na=False
    )
    weekly = normalized.loc[
        is_ppa
        & normalized["_timestamp"].ge(start)
        & normalized["_timestamp"].lt(end_exclusive)
        & normalized["_value"].notna()
    ]
    totals = weekly["_parameter"].value_counts().astype("int64")
    if weekly.empty or oos_specs.empty:
        return {parameter: 0.0 for parameter in totals.index}

    candidates = weekly.merge(
        oos_specs,
        on=["step_id", "_parameter", "sheet_id"],
        how="left",
        validate="many_to_one",
    )
    is_oos = (
        candidates["usl"].notna() & candidates["_value"].gt(candidates["usl"])
    ) | (
        candidates["lsl"].notna() & candidates["_value"].lt(candidates["lsl"])
    )
    oos_counts = candidates.loc[is_oos, "_parameter"].value_counts()
    return {
        parameter: float(oos_counts.get(parameter, 0)) / int(total_count)
        for parameter, total_count in totals.items()
        if int(total_count) > 0
    }


def build_ppa_ratio_summary(
    oos_sheets: Mapping[str, pd.DataFrame],
    measurement_sheets: Mapping[str, pd.DataFrame],
    *,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    product_order: Sequence[str] = DEFAULT_PRODUCT_ORDER,
) -> pd.DataFrame:
    """Calculate OOS-Sheet count divided by measured-Sheet count."""
    if start >= end_exclusive:
        raise ValueError("统计开始时间必须早于结束时间")

    parameters: set[str] = set()
    ratios_by_product: dict[str, dict[str, float]] = {}

    for product in product_order:
        oos_frame = oos_sheets.get(product)
        measurement_frame = measurement_sheets.get(product)
        if oos_frame is None:
            raise ValueError(f"超规明细工作簿缺少产品工作表: {product}")
        if measurement_frame is None:
            raise ValueError(f"缺少产品底层测量数据: {product}")

        product_parameters, oos_specs = _weekly_oos_specs(
            oos_frame, start, end_exclusive
        )
        product_ratios = _weekly_measurement_ratios(
            measurement_frame, oos_specs, start, end_exclusive
        )
        parameters.update(product_parameters)
        ratios_by_product[product] = product_ratios

    ordered_parameters = sorted(parameters, key=_parameter_sort_key)
    result: dict[str, list[object]] = {
        "分类": ["PPA"] * len(ordered_parameters),
        "项目": ordered_parameters,
    }
    for product in product_order:
        ratios = ratios_by_product[product]
        result[product] = [
            ratios.get(parameter, float("nan")) for parameter in ordered_parameters
        ]
    return pd.DataFrame(result)


def write_summary_workbook(
    summary: pd.DataFrame,
    output_path: Path,
    *,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    source_path: Path,
) -> None:
    """Write the styled summary table and its auditable reporting metadata."""
    required = {"分类", "项目"}
    if not required.issubset(summary.columns):
        raise ValueError("汇总数据必须包含“分类”和“项目”列")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("PPA超规统计")
        writer.sheets["PPA超规统计"] = worksheet

        header_format = workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": "#0B50F4",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "border_color": "#FFFFFF",
            }
        )
        row_formats = [
            workbook.add_format(
                {
                    "bg_color": color,
                    "align": "center",
                    "valign": "vcenter",
                    "border": 1,
                    "border_color": "#FFFFFF",
                }
            )
            for color in ("#C4CAF6", "#E1E4F8")
        ]
        percentage_formats = [
            workbook.add_format(
                {
                    "bg_color": color,
                    "align": "center",
                    "valign": "vcenter",
                    "border": 1,
                    "border_color": "#FFFFFF",
                    "num_format": "0.00%",
                }
            )
            for color in ("#C4CAF6", "#E1E4F8")
        ]
        category_format = workbook.add_format(
            {
                "bold": True,
                "bg_color": "#C4CAF6",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "border_color": "#FFFFFF",
            }
        )

        columns = list(summary.columns)
        worksheet.set_row(0, 38)
        for column_index, column_name in enumerate(columns):
            worksheet.write(0, column_index, column_name, header_format)

        for row_index, row in enumerate(summary.itertuples(index=False, name=None), start=1):
            format_index = (row_index - 1) % len(row_formats)
            row_format = row_formats[format_index]
            percentage_format = percentage_formats[format_index]
            worksheet.set_row(row_index, 32)
            for column_index, value in enumerate(row):
                if column_index == 0:
                    continue
                cell_format = percentage_format if column_index >= 2 else row_format
                if pd.isna(value):
                    worksheet.write_blank(row_index, column_index, None, cell_format)
                else:
                    worksheet.write(row_index, column_index, value, cell_format)

        if len(summary) == 1:
            worksheet.write(1, 0, "PPA", category_format)
        elif len(summary) > 1:
            worksheet.merge_range(1, 0, len(summary), 0, "PPA", category_format)

        worksheet.set_column(0, 0, 12)
        worksheet.set_column(1, 1, 24)
        worksheet.set_column(2, max(2, len(columns) - 1), 16)
        worksheet.freeze_panes(1, 2)
        worksheet.autofilter(0, 1, max(0, len(summary)), len(columns) - 1)

        metadata = workbook.add_worksheet("统计说明")
        writer.sheets["统计说明"] = metadata
        metadata_header = workbook.add_format(
            {"bold": True, "font_color": "#FFFFFF", "bg_color": "#0B50F4"}
        )
        metadata.write_row(0, 0, ["项目", "内容"], metadata_header)
        period_end = end_exclusive - pd.Timedelta(days=1)
        metadata.write_row(
            1,
            0,
            ["统计周期", f"{start:%Y-%m-%d} 至 {period_end:%Y-%m-%d}"],
        )
        metadata.write_row(2, 0, ["来源文件", str(source_path)])
        metadata.write_row(
            3,
            0,
            ["统计口径", "超规原始测量点数 / 同产品同参数的有效原始测量点总数"],
        )
        metadata.write_row(
            4,
            0,
            ["空白说明", "统计周期内无对应有效原始测量点时留空"],
        )
        metadata.set_column(0, 0, 14)
        metadata.set_column(1, 1, 64)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH, help="明细工作簿路径")
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIR,
        help="底层 Inline 测量快照根目录",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="汇总工作簿路径")
    parser.add_argument(
        "--reference-date",
        type=date.fromisoformat,
        default=date.today(),
        help="用于确定上周的参考日期，格式 YYYY-MM-DD",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start, end_exclusive = previous_calendar_week(args.reference_date)
    sheets = read_source_workbook(args.input)
    products = resolve_product_order(sheets)
    measurement_sheets = load_measurement_snapshots(args.snapshot_dir, products)
    summary = build_ppa_ratio_summary(
        sheets,
        measurement_sheets,
        start=start,
        end_exclusive=end_exclusive,
        product_order=products,
    )
    write_summary_workbook(
        summary,
        args.output,
        start=start,
        end_exclusive=end_exclusive,
        source_path=args.input,
    )
    print(
        f"已生成 {args.output} | 周期 {start:%Y-%m-%d} 至 "
        f"{end_exclusive - pd.Timedelta(days=1):%Y-%m-%d} | "
        f"产品 {len(products)} | 参数 {len(summary)} | 指标 超规测量点占比"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
