"""AOI_RS 报表投影：Sheet 截断 → Lot 归零 → 周/日密度限幅。

Sheet 超规值取 [0, 0.5 × spec] 的稳定随机值；重算后仍超规的 Lot
及其 Sheet 归零。周/日密度最多为所属月份的 1.3 倍，不回写明细。
沿用工作簿三态及参数豁免；规则只生成投影，不修改源事实。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    attach_spec_values,
    build_lot_point_df,
    build_period_trend_df,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    merge_detail_with_decoration_flags,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_KEY_COLUMNS,
    apply_aoi_rs_decoration as apply_standard_decoration,
    normalize_aoi_rs_points,
)

AOI_RS_DECORATION_POLICY_VERSION = "factory-scoped-sheet-half-lot-zero-period-1.3-v2"
_INDICATOR_KEYS = ["factory", "step_id", "rs_code"]
_SHEET_KEYS = [*_INDICATOR_KEYS, "sheet_id"]
_LOT_KEYS = [*_INDICATOR_KEYS, "lot_id"]
_SHEET_SPEC_RATIO = 0.5
_PERIOD_MONTH_RATIO = 1.3
_CHART_POINT_META = {"lot": ("lot_id", "value"), "sheet": ("sheet_id", "rs_qty")}


def _apply_chart_decoration(
    points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    chart_kind: str,
    prod_code: str,
    exempt_param_name_contains: Iterable[str] | None = None,
) -> pd.DataFrame:
    """应用三态 flag：Delete 剔除、False 保留、True 按图类型修饰。"""
    _, value_col = _CHART_POINT_META[chart_kind]
    attached = attach_spec_values(points_df, spec_df, chart_kind=chart_kind)
    if attached.empty:
        return attached.drop(columns=["spec"], errors="ignore")

    normalized = normalize_aoi_rs_points(attached, chart_kind, prod_code)
    normalized["value"] = normalized["value"].astype(float)
    chart_flags = (
        decoration_df[decoration_df["chart_kind"].astype(str) == chart_kind]
        if not decoration_df.empty and "chart_kind" in decoration_df.columns
        else pd.DataFrame()
    )
    decorated = merge_detail_with_decoration_flags(
        normalized, chart_flags, AOI_RS_OOS_KEY_COLUMNS,
    )
    decorated = decorated.loc[decorated["flag"].ne("Delete")].copy()
    over = (
        decorated["flag"].eq(True)
        & decorated["spec"].ge(0)
        & decorated["value"].gt(decorated["spec"])
    ).fillna(False)
    for token in exempt_param_name_contains or ():
        if token is not None and str(token).strip():
            over &= ~decorated["rs_code"].str.contains(
                str(token).strip(), case=False, regex=False, na=False,
            )
    if over.any():
        if chart_kind == "sheet":
            decorated.loc[over, "value"] = decorated.loc[over].apply(
                _sheet_replacement_value, axis=1,
            )
        else:
            decorated.loc[over, ["value", "rs_qty"]] = 0.0
    if value_col != "value":
        decorated[value_col] = decorated["value"]
        decorated = decorated.drop(columns=["value"])
    return decorated.drop(columns=["spec", "chart_kind", "point_id", "sheet_start_time", "flag"])


def _sheet_replacement_value(row: pd.Series) -> float:
    seed = json.dumps(
        [str(row[key]) for key in AOI_RS_OOS_KEY_COLUMNS], ensure_ascii=False,
    )
    fraction = int.from_bytes(hashlib.sha256(seed.encode("utf-8")).digest()[:8], "big") / 2**64
    return float(row["spec"]) * _SHEET_SPEC_RATIO * fraction


def project_aoi_rs_details(
    rs_details_df: pd.DataFrame, sheet_points_df: pd.DataFrame,
) -> pd.DataFrame:
    """按各 Sheet 原明细占比分配修饰后总量；Delete 的 Sheet 不进入投影。"""
    if rs_details_df.empty or sheet_points_df.empty:
        return rs_details_df.iloc[:0].copy()
    details = rs_details_df.copy()
    details["code_qty"] = pd.to_numeric(details["code_qty"], errors="coerce")
    totals = details.groupby(_SHEET_KEYS)["code_qty"].transform("sum")
    details["_original_total"] = totals
    details["_share"] = details["code_qty"].div(totals.where(totals.ne(0))).fillna(0)
    targets = sheet_points_df[[*_SHEET_KEYS, "rs_qty"]].rename(columns={"rs_qty": "_target"})
    projected = details.merge(targets, on=_SHEET_KEYS, how="inner", validate="many_to_one")
    # Unchanged sheets retain exact source values, including missing values.
    changed = projected["_original_total"].ne(projected["_target"])
    projected["code_qty"] = projected["code_qty"].astype(float)
    projected.loc[changed, "code_qty"] = (
        projected.loc[changed, "_share"] * projected.loc[changed, "_target"]
    )
    return projected[rs_details_df.columns]


def _rebuild_lots(
    details: pd.DataFrame, lot_points_df: pd.DataFrame,
) -> pd.DataFrame:
    lots = build_lot_point_df(details, pd.DataFrame()).drop(columns=["sheet_qty", "value"])
    lots = lots.merge(
        lot_points_df[[*_LOT_KEYS, "sheet_qty"]], on=_LOT_KEYS,
        how="left", validate="one_to_one",
    )
    lots["value"] = lots["rs_qty"].div(lots["sheet_qty"].where(lots["sheet_qty"].gt(0)))
    return lots


def apply_aoi_rs_decoration(
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
    decoration_df: pd.DataFrame,
    exempt_param_name_contains: Iterable[str] | None = None,
    *,
    rs_details_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """先修饰 Sheet，再重算并修饰 Lot；明细用于跨图关联和分子重算。"""
    exemptions = tuple(exempt_param_name_contains or ())
    sheet_decorated = _apply_chart_decoration(
        sheet_points_df,
        spec_df,
        decoration_df,
        "sheet",
        prod_code,
        exemptions,
    )
    if rs_details_df is not None:
        projected = project_aoi_rs_details(rs_details_df, sheet_decorated)
        lot_points_df = _rebuild_lots(projected, lot_points_df)
    lot_decorated = _apply_chart_decoration(
        lot_points_df,
        spec_df,
        decoration_df,
        "lot",
        prod_code,
        exemptions,
    )
    if rs_details_df is not None and not sheet_decorated.empty:
        zeroed = lot_decorated.loc[lot_decorated["value"].eq(0), _LOT_KEYS]
        affected = rs_details_df.merge(zeroed, on=_LOT_KEYS, how="inner")
        affected = affected[_SHEET_KEYS].drop_duplicates().assign(_zero=True)
        sheet_decorated = sheet_decorated.merge(
            affected, on=_SHEET_KEYS, how="left", validate="one_to_one",
        )
        sheet_decorated.loc[sheet_decorated["_zero"].eq(True), "rs_qty"] = 0.0
        sheet_decorated = sheet_decorated.drop(columns=["_zero"])
    return lot_decorated, sheet_decorated


def build_decorated_period_trend_df(
    rs_details_df: pd.DataFrame, pass_through_df: pd.DataFrame, end_date: date,
    *, special_factories: Iterable[str] | None = None,
) -> pd.DataFrame:
    """周/日投影限于所属月密度的 1.3 倍；跨月周取截至报表日的周末所属月。"""
    trend = build_period_trend_df(rs_details_df, pass_through_df, end_date)
    if trend.empty:
        return trend
    trend["rs_qty"] = trend["rs_qty"].astype(float)
    monthly = trend.loc[trend["period_type"].eq("month")].set_index(
        [*_INDICATOR_KEYS, "period_label"],
    )["value"]
    eligible = trend["period_type"].isin(["week", "day"])
    if special_factories is not None:
        eligible &= factory_mask(trend, special_factories)
    for index, row in trend.loc[eligible].iterrows():
        if row["period_type"] == "week":
            year, week = row["period_label"].split("-W")
            period_end = min(date.fromisocalendar(int(year), int(week), 7), end_date)
        else:
            period_end = date.fromisoformat(row["period_label"])
        month_key = (*[row[key] for key in _INDICATOR_KEYS], period_end.strftime("%Y-%m"))
        month_value = monthly.get(month_key)
        if pd.isna(month_value) or month_value < 0 or pd.isna(row["value"]):
            continue
        cap = float(month_value) * _PERIOD_MONTH_RATIO
        if row["value"] > cap:
            trend.loc[index, "value"] = cap
            trend.loc[index, "rs_qty"] = cap * row["sheet_qty"]
    return trend


def factory_mask(frame: pd.DataFrame, factories: Iterable[str]) -> pd.Series:
    """Match factory names without changing source keys or presentation values."""
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    names = {name.strip().upper() for name in factories}
    return frame["factory"].astype("string").str.strip().str.upper().isin(names)


def apply_factory_scoped_decoration(
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
    decoration_df: pd.DataFrame,
    exempt_param_name_contains: Iterable[str] | None = None,
    *,
    rs_details_df: pd.DataFrame | None = None,
    special_factories: Iterable[str] = (),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Route selected factories through special rules and all others through standard rules."""
    factories = tuple(special_factories)
    exemptions = tuple(exempt_param_name_contains or ())
    lot_mask = factory_mask(lot_points_df, factories)
    sheet_mask = factory_mask(sheet_points_df, factories)
    standard = apply_standard_decoration(
        lot_points_df.loc[~lot_mask], sheet_points_df.loc[~sheet_mask],
        spec_df, prod_code, decoration_df, exemptions,
    )
    if not lot_mask.any() and not sheet_mask.any():
        return standard
    if rs_details_df is None:
        raise ValueError("AOI_RS special decoration requires RS details")
    special = apply_aoi_rs_decoration(
        lot_points_df.loc[lot_mask], sheet_points_df.loc[sheet_mask],
        spec_df, prod_code, decoration_df, exemptions,
        rs_details_df=rs_details_df.loc[factory_mask(rs_details_df, factories)],
    )
    return tuple(
        pd.concat([normal, modified], ignore_index=True).sort_values(
            [*_INDICATOR_KEYS, "first_start_time"], kind="stable",
        ).reset_index(drop=True)
        for normal, modified in zip(standard, special)
    )


def project_factory_scoped_details(
    rs_details_df: pd.DataFrame, sheet_points_df: pd.DataFrame,
    special_factories: Iterable[str],
) -> pd.DataFrame:
    """Only special factories propagate Sheet/Lot adjustments into period numerators."""
    mask = factory_mask(rs_details_df, special_factories)
    if not mask.any():
        return rs_details_df.copy()
    details = rs_details_df.assign(_row_order=range(len(rs_details_df)))
    projected = project_aoi_rs_details(details.loc[mask], sheet_points_df)
    return pd.concat([details.loc[~mask], projected], ignore_index=True).sort_values(
        "_row_order", kind="stable",
    ).drop(columns="_row_order").reset_index(drop=True)
