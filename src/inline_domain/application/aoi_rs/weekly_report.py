"""Assemble last week's AOI RS values for a product-column report.

The half-open week is fixed independently of the chart's compact period axis.
Its numeric values reuse the current chart projection, including that chart's
month context through ``reference_date``. No files or databases are read here.
"""

from __future__ import annotations

from collections.abc import Callable
from collections import Counter
from dataclasses import dataclass
from datetime import date

import pandas as pd

from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportViewModel
from src.inline_domain.application.aoi_rs.decoration_service import build_aoi_rs_period_trend
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from src.inline_domain.application.shared.weekly_summary import (
    WeeklySummaryRow as WeeklyRsRow,
    WeeklySummaryTable as WeeklyRsTable,
    format_weekly_average,
)

_KEY_COLUMNS = ("factory", "step_id", "rs_code")
_ARRAY_LAYERS = {
    "11629": "3layer", "12629": "M1", "13629": "M2", "15629": "M3", "18629": "M4",
}
_LAYER_ORDER = {step: index for index, step in enumerate(_ARRAY_LAYERS)}
_FRAME_COLUMNS = {
    "rs_details_df": (*_KEY_COLUMNS, "start_time", "code_qty"),
    "pass_through_df": ("factory", "step_id", "start_time", "sheet_id"),
    "spec_df": _KEY_COLUMNS,
    "indicators_df": _KEY_COLUMNS,
}
_IndicatorKey = tuple[str, str, str]


@dataclass(frozen=True)
class _ProductWeek:
    descriptions: dict[_IndicatorKey, str]
    values: dict[_IndicatorKey, object]
    throughput: dict[tuple[str, str], int]
    detail_keys: frozenset[_IndicatorKey]


def _prepare_frame(frame: pd.DataFrame, name: str, product: str) -> pd.DataFrame:
    """Allow empty frames, but reject incomplete nonempty adapter results."""
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{name} must be a DataFrame")
    required = _FRAME_COLUMNS[name]
    if frame.empty:
        return pd.DataFrame(columns=required)
    missing = set(required).difference(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(sorted(missing))}")
    result = frame.copy()
    if "prod_code" in result:
        result = result.loc[result["prod_code"].astype(str).str.strip().eq(product)].copy()
    for column in set(_KEY_COLUMNS).intersection(required):
        if result[column].isna().any() or result[column].astype(str).str.strip().eq("").any():
            raise ValueError(f"{name} contains an empty {column}")
        result[column] = result[column].astype(str).str.strip()
    result["factory"] = result["factory"].str.upper()
    if "start_time" in required:
        result["start_time"] = pd.to_datetime(result["start_time"], errors="coerce")
        if result["start_time"].isna().any():
            raise ValueError(f"{name} contains an invalid start_time")
    if name == "pass_through_df" and result["sheet_id"].isna().any():
        raise ValueError(f"{name} contains an empty sheet_id")
    if name == "rs_details_df":
        result["code_qty"] = pd.to_numeric(result["code_qty"], errors="coerce")
        if result["code_qty"].isna().any():
            raise ValueError(f"{name} contains an invalid code_qty")
    return result


def _indicator_descriptions(frames: dict[str, pd.DataFrame]) -> dict[_IndicatorKey, str]:
    descriptions: dict[_IndicatorKey, str] = {}
    # A specification description takes precedence; blank descriptions may be filled later.
    for name in ("spec_df", "indicators_df", "rs_details_df"):
        for row in frames[name].itertuples(index=False):
            key = (row.factory, row.step_id, row.rs_code)
            desc = getattr(row, "code_desc", None)
            text = desc.strip() if isinstance(desc, str) else ""
            if key not in descriptions or not descriptions[key]:
                descriptions[key] = text
    return descriptions


def _prepare_product_week(
    report: AoiRsReportViewModel, product: str, reference_date: date,
    week_start: pd.Timestamp, week_end: pd.Timestamp,
) -> _ProductWeek:
    frames = {name: _prepare_frame(getattr(report, name), name, product) for name in _FRAME_COLUMNS}
    details, passes = frames["rs_details_df"], frames["pass_through_df"]
    # Keep the full monthly context. Cropping to last Sunday changes chart values.
    trend = build_aoi_rs_period_trend(details, passes, reference_date)
    iso = week_start.isocalendar()
    week_label = f"{iso.year}-W{iso.week:02d}"
    weekly = trend.loc[trend["period_type"].eq("week") & trend["period_label"].eq(week_label)]
    if weekly.duplicated(list(_KEY_COLUMNS)).any():
        raise ValueError("Weekly AOI RS trend contains duplicate indicators")
    values = {(row.factory, row.step_id, row.rs_code): row.value for row in weekly.itertuples(index=False)}
    weekly_passes = passes.loc[passes["start_time"].ge(week_start) & passes["start_time"].lt(week_end)]
    throughput = weekly_passes.groupby(["factory", "step_id"])["sheet_id"].nunique().to_dict()
    weekly_details = details.loc[details["start_time"].ge(week_start) & details["start_time"].lt(week_end)]
    detail_keys = frozenset(weekly_details[list(_KEY_COLUMNS)].itertuples(index=False, name=None))
    return _ProductWeek(_indicator_descriptions(frames), values, throughput, detail_keys)


def _cell_value(product: _ProductWeek, key: _IndicatorKey) -> str:
    if product.throughput.get(key[:2], 0) <= 0:
        return "无过货"
    if key not in product.values:
        return "数据不足" if key in product.detail_keys else "0"
    return format_weekly_average(product.values[key])


def build_weekly_rs_tables(
    products: tuple[str, ...], *, load_report: Callable[[str], AoiRsReportViewModel],
    reference_date: date, factories: tuple[str, ...] = ("ARRAY", "OLED", "TP"),
    steps: tuple[str, ...] = (),
) -> list[WeeklyRsTable]:
    """Build per-factory tables, keeping product order and each station/Code row.

    ``week_end`` is exclusive. ``steps`` filters Array only; an empty tuple means
    all Array stations. Loader errors propagate so unavailable data cannot become
    a successful report containing fabricated "无过货" values.
    """
    week_start, week_end = previous_iso_week_range(reference_date)
    if not products:
        return []
    if len(products) != len(set(products)):
        raise ValueError("Weekly AOI RS products must be unique")
    product_weeks = tuple(
        _prepare_product_week(load_report(product), product, reference_date, week_start, week_end)
        for product in products
    )
    descriptions: dict[_IndicatorKey, str] = {}
    for product in product_weeks:
        for key, desc in product.descriptions.items():
            if key not in descriptions or not descriptions[key]:
                descriptions[key] = desc
    allowed_steps = {str(step).strip() for step in steps}
    tables: list[WeeklyRsTable] = []
    for factory in dict.fromkeys(name.strip().upper() for name in factories):
        keys = [key for key in descriptions if key[0] == factory and (
            factory != "ARRAY" or not allowed_steps or key[1] in allowed_steps
        )]
        keys.sort(key=lambda key: (_LAYER_ORDER.get(key[1], len(_LAYER_ORDER)), key[1], key[2]))
        code_counts = Counter(key[2] for key in keys)

        def label(key: _IndicatorKey) -> str:
            text = f"{key[2]}（{descriptions[key]}）" if descriptions[key] else key[2]
            return f"{text} · {key[1]}" if factory != "ARRAY" and code_counts[key[2]] > 1 else text

        rows = tuple(
            WeeklyRsRow(
                step_id=key[1], layer=_ARRAY_LAYERS.get(key[1], key[1]) if factory == "ARRAY" else "",
                code=key[2], label=label(key),
                values=tuple(_cell_value(product, key) for product in product_weeks),
            )
            for key in keys
        )
        tables.append(WeeklyRsTable(factory, products, rows, week_start.date(), week_end.date()))
    return tables
