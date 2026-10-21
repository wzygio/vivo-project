"""Product-column TT weekly values from the existing chart projection."""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import date

import pandas as pd

from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportViewModel
from src.inline_domain.application.shared.weekly_summary import (
    WeeklySummaryRow, WeeklySummaryTable, format_weekly_average, judge_weekly_density,
)
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import (
    PARTICLE_SIZE_OPTIONS, build_period_trend_df, attach_spec_values,
)
from src.inline_domain.core.aoi_tt.parameters import is_aoi_tt_parameter
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range

ARRAY_TT_LAYERS = {"11620": "3layer", "12620": "M1", "13620": "M2", "15620": "M3", "18620": "M4"}
_FACTORY_BY_STEP = {**dict.fromkeys(ARRAY_TT_LAYERS, "ARRAY"), "21320": "OLED", "43620": "TP"}
_KEYS = ["factory", "step_id", "tt_name", "particle_size"]
_DETAIL_COLUMNS = ["factory", "step_id", "tt_name", "start_time", "sheet_id", "tt_qty"]


def _prepare(frame: pd.DataFrame, product: str, *, details: bool) -> pd.DataFrame:
    required = _DETAIL_COLUMNS if details else ["step_id", "tt_name"]
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("TT report input must be a DataFrame")
    if frame.empty:
        return pd.DataFrame(columns=required + (["particle_size"] if details else []))
    if not set(required).issubset(frame.columns):
        raise ValueError("TT report input is missing required columns")
    result = frame.copy()
    if "prod_code" in result:
        result = result.loc[result["prod_code"].astype(str).str.strip().eq(product)].copy()
    for key in ["step_id", "tt_name"] + (["factory", "sheet_id"] if details else []):
        if result[key].isna().any() or result[key].astype(str).str.strip().eq("").any():
            raise ValueError(f"TT report input contains an empty {key}")
        result[key] = result[key].astype(str).str.strip()
    result = result.loc[result["tt_name"].map(is_aoi_tt_parameter)].copy()
    if details:
        result["factory"] = result["factory"].str.upper()
        result["start_time"] = pd.to_datetime(result["start_time"], errors="coerce")
        result["tt_qty"] = pd.to_numeric(result["tt_qty"], errors="coerce")
        if result[["start_time", "tt_qty"]].isna().any().any():
            raise ValueError("TT report contains invalid times or quantities")
        if "particle_size" not in result:
            result["particle_size"] = "Total"
        result["particle_size"] = result["particle_size"].fillna("Total").astype(str).str.strip()
    return result


def build_weekly_tt_tables(
    products: tuple[str, ...], *, load_report: Callable[[str], AoiTtReportViewModel],
    reference_date: date, factories: tuple[str, ...] = ("ARRAY", "OLED", "TP"),
    steps: tuple[str, ...] = (), particle_sizes: tuple[str, ...] = ("Total",),
) -> list[WeeklySummaryTable]:
    """Keep the exact previous ISO week; never substitute an older nonempty week.

    TT detection counts come from its own measurements, including zero-valued
    measurements. Missing TT/particle measurements cannot be assumed to be zero.
    Array station filtering never removes OLED/TP, which retain their own stations.
    OLED is always Total-only, matching the existing report.
    """
    if not products:
        return []
    if len(set(products)) != len(products):
        raise ValueError("TT report products must be unique")
    if any(size not in PARTICLE_SIZE_OPTIONS for size in particle_sizes):
        raise ValueError("Unknown TT particle size")
    if any(factory not in ("ARRAY", "OLED", "TP") for factory in factories):
        raise ValueError("Unknown TT factory")
    start, end = previous_iso_week_range(reference_date)
    iso = start.isocalendar()
    week_label = f"{iso.year}-W{iso.week:02d}"
    prepared = []
    factory_by_step = dict(_FACTORY_BY_STEP)
    for product in products:
        report = load_report(product)
        details = _prepare(report.tt_details_df, product, details=True)
        specs = _prepare(report.spec_df, product, details=False)
        for row in details[["factory", "step_id"]].drop_duplicates().itertuples(index=False):
            if row.step_id in factory_by_step and factory_by_step[row.step_id] != row.factory:
                raise ValueError("Conflicting TT station factory metadata")
            factory_by_step[row.step_id] = row.factory
        prepared.append((details, specs))

    candidates = set()
    all_values, all_counts, all_limits = [], [], []
    for details, specs in prepared:
        candidates.update(details[["factory", "step_id", "tt_name"]].itertuples(index=False, name=None))
        for row in specs.itertuples(index=False):
            factory = factory_by_step.get(row.step_id)
            if factory is not None:
                candidates.add((factory, row.step_id, row.tt_name))
        spec_rows = attach_spec_values(
            specs[["step_id", "tt_name"]].drop_duplicates(),
            specs.reindex(columns=["step_id", "tt_name", "usl", "ucl"]),
        )
        all_limits.append({(row.step_id, row.tt_name): row.usl for row in spec_rows.itertuples(index=False)})
        # Use the same unfiltered station/particle frame as the default line chart.
        trend = build_period_trend_df(details, reference_date)
        weekly = trend.loc[trend["period_type"].eq("week") & trend["period_label"].eq(week_label)]
        if weekly.duplicated(_KEYS).any():
            raise ValueError("Duplicate TT weekly trend indicators")
        all_values.append({(r.factory, r.step_id, r.tt_name, r.particle_size): r.value
                           for r in weekly.itertuples(index=False)})
        week_details = details.loc[details["start_time"].ge(start) & details["start_time"].lt(end)]
        all_counts.append(week_details.groupby(["factory", "step_id"])["sheet_id"].nunique().to_dict())

    sizes = tuple(size for size in PARTICLE_SIZE_OPTIONS if not particle_sizes or size in particle_sizes)
    layer_order = {step: i for i, step in enumerate(ARRAY_TT_LAYERS)}
    tables = []
    for factory in dict.fromkeys(factories):
        keys = [key for key in candidates if key[0] == factory and
                (factory != "ARRAY" or not steps or key[1] in steps)]
        keys.sort(key=lambda key: (layer_order.get(key[1], len(layer_order)), key[1], key[2]))
        name_counts = Counter(key[2] for key in keys)
        rows = []
        for key in keys:
            for size in (("Total",) if factory == "OLED" else sizes):
                values = tuple("无过货" if counts.get(key[:2], 0) == 0 else
                    format_weekly_average(data[(*key, size)]) if (*key, size) in data else "数据不足"
                    for data, counts in zip(all_values, all_counts))
                label = f"{key[2]}（{size}）"
                if factory != "ARRAY" and name_counts[key[2]] > 1:
                    label += f" · {key[1]}"
                verdicts = tuple(judge_weekly_density(
                    data.get((*key, size)) if counts.get(key[:2], 0) > 0 else None,
                    limits.get(key[1:]),
                ) for data, counts, limits in zip(all_values, all_counts, all_limits))
                rows.append(WeeklySummaryRow(key[1], ARRAY_TT_LAYERS.get(key[1], key[1]) if factory == "ARRAY" else "",
                                              f"{key[2]}|{size}", label, values, within_spec=verdicts))
        tables.append(WeeklySummaryTable(factory, products, tuple(rows), start.date(), end.date()))
    return tables
