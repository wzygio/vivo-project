"""Calendar-period CPK calculations and the persistent workbook contract."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    _period_windows,
    current_period_windows,
)
from src.inline_domain.core.spc.spc_calculator import calculate_cpk

CPK_THRESHOLD = 1.33
CPK_SUMMARY_COLUMNS = [
    "产品", "监控类型", "厂别", "周期类型", "时间标签", "显示标签",
    "CPK总项目数", "Cpk≥1.33达标率", "达标项目数", "预警项目数",
]
CPK_SUMMARY_KEYS = CPK_SUMMARY_COLUMNS[:5]
CPK_DETAIL_COLUMNS = [
    "prod_code", "factory", "step_id", "param_name", "period_type",
    "period_label", "period_sort", "period_start", "period_end",
    "sample_count", "mean_value", "std_value", "usl", "lsl", "cpk",
]
PERIOD_NAMES = {"年度": "year", "季度": "quarter", "月度": "month", "周度": "week"}


def normalize_cpk_records(frame: pd.DataFrame) -> pd.DataFrame:
    """Upgrade legacy rates without inventing rounded historical project counts."""
    if frame.empty:
        return pd.DataFrame(columns=CPK_SUMMARY_COLUMNS)
    result = frame.copy()
    required = {"产品", "周期类型", "时间标签", "显示标签", "CPK总项目数", "Cpk≥1.33达标率"}
    missing = required - set(result.columns)
    if missing:
        raise ValueError(f"CPK 汇总缺少列: {sorted(missing)}")
    for column, default in (("监控类型", "SPC"), ("厂别", "ALL")):
        if column not in result:
            result[column] = default
    for column in ("达标项目数", "预警项目数"):
        if column not in result:
            result[column] = pd.NA
    for column in CPK_SUMMARY_COLUMNS[:6]:
        result[column] = result[column].fillna("").astype(str).str.strip()
    if result[CPK_SUMMARY_KEYS].eq("").any().any():
        raise ValueError("CPK 汇总业务键不得为空")
    if result.duplicated(CPK_SUMMARY_KEYS).any():
        raise ValueError("CPK 汇总存在重复业务键")
    for column in CPK_SUMMARY_COLUMNS[6:]:
        dtype = "Float64" if column == "Cpk≥1.33达标率" else "Int64"
        result[column] = pd.to_numeric(result[column], errors="coerce").astype(dtype)
    return result[CPK_SUMMARY_COLUMNS].reset_index(drop=True)


def build_current_cpk_detail(
    sheet_features: pd.DataFrame, *, end_date: pd.Timestamp
) -> pd.DataFrame:
    """Compute each period from its Sheet means, never from monthly CPK averages."""
    if sheet_features.empty:
        return pd.DataFrame(columns=CPK_DETAIL_COLUMNS)
    required = {"prod_code", "factory", "step_id", "param_name", "sheet_id",
                "sheet_start_time", "sheet_mean", "usl", "lsl"}
    if missing := required - set(sheet_features.columns):
        raise ValueError(f"CPK 原始特征缺少列: {sorted(missing)}")
    frame = sheet_features.copy()
    frame["sheet_start_time"] = pd.to_datetime(frame["sheet_start_time"], errors="coerce")
    records: list[dict[str, object]] = []
    group_columns = ["prod_code", "factory", "step_id", "param_name"]
    for order, window in enumerate(current_period_windows(end_date)):
        period = frame[frame["sheet_start_time"].ge(window.start)
                       & frame["sheet_start_time"].lt(window.end)]
        for keys, group in period.groupby(group_columns, dropna=False, sort=True):
            valid = group.dropna(subset=["sheet_mean", "usl", "lsl"])
            if valid.empty:
                continue
            mean = float(valid["sheet_mean"].mean())
            std = float(valid["sheet_mean"].std(ddof=1))
            usl, lsl = float(valid["usl"].iloc[0]), float(valid["lsl"].iloc[0])
            records.append({
                **dict(zip(group_columns, keys)),
                "period_type": PERIOD_NAMES[window.period_type],
                "period_label": window.time_label, "period_sort": order,
                "period_start": window.start, "period_end": window.end,
                "sample_count": valid["sheet_id"].nunique(),
                "mean_value": mean, "std_value": std, "usl": usl, "lsl": lsl,
                "cpk": calculate_cpk(mean, std, usl, lsl),
            })
    return pd.DataFrame(records, columns=CPK_DETAIL_COLUMNS)


def build_current_cpk_records(
    detail: pd.DataFrame, *, products: Iterable[str], factory_key: str,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    records = []
    for product in dict.fromkeys(products):
        for window in current_period_windows(end_date):
            rows = detail[detail["prod_code"].eq(product)
                          & detail["period_label"].eq(window.time_label)]
            values = pd.to_numeric(rows["cpk"], errors="coerce").dropna()
            passed = int(values.ge(CPK_THRESHOLD).sum())
            total = len(values)
            records.append({
                "产品": product, "监控类型": "SPC", "厂别": factory_key,
                "周期类型": window.period_type, "时间标签": window.time_label,
                "显示标签": window.display_label, "CPK总项目数": total,
                "Cpk≥1.33达标率": passed / total if total else float("nan"),
                "达标项目数": passed, "预警项目数": total - passed,
            })
    return pd.DataFrame(records, columns=CPK_SUMMARY_COLUMNS)


def build_cpk_summary_from_records(
    records: pd.DataFrame, *, end_date: pd.Timestamp,
    expected_products: Iterable[str] = (),
) -> pd.DataFrame:
    rows_order = ["CPK总项目数", "达标项目数", "预警项目数", "Cpk≥1.33达标率"]
    result: dict[str, list[object]] = {"指标": rows_order}
    expected = set(expected_products)
    for window in _period_windows(end_date):
        rows = records[records["时间标签"].eq(window.time_label)]
        complete = not rows.empty and (not expected or set(rows["产品"]) == expected)
        values: list[object] = []
        for column in rows_order[:3]:
            numeric = pd.to_numeric(rows[column], errors="coerce")
            values.append(int(numeric.sum()) if complete and numeric.notna().all() else "—")
        counts = pd.to_numeric(rows["CPK总项目数"], errors="coerce")
        rates = pd.to_numeric(rows["Cpk≥1.33达标率"], errors="coerce")
        valid = complete and counts.notna().all() and rates[counts.gt(0)].notna().all()
        values.append(f"{(counts * rates).sum() / counts.sum():.2%}"
                      if valid and counts.sum() > 0 else "—")
        result[window.display_label] = values
    return pd.DataFrame(result)
