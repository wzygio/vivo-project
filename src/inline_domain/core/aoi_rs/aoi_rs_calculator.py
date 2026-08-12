"""AOI_RS 报表核心聚合：月周天趋势（分子/分母）、By Lot、By Sheet 与规格线匹配。

口径（依据 docs/dev_docs/dev_prompt/feat-AOI_RS.md）：
- 月周天趋势：每个 period 的值 = Σcode_qty ÷ 同 period 同站点过货 distinct sheet/glass 数；
  period 轴复用 SPC 的 build_available_period_axis（跳过空值向前补全，最近 2 月/3 周/7 天）。
- By Lot / By Sheet：每个 lot / sheet 的 RS 个数 = Σcode_qty。
- 规格线：mdw.dwd_imp_rs_code_xishu_fo_tzsbjx 按 type_flag 区分适用图类型，
  spec 为单边上限值；无规格时不画线（NaN）。
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.inline_domain.core.spc.spc_calculator import build_available_period_axis

# type_flag 与图类型的映射（SHEET_ID/GLASS_ID 均适用于 By Sheet 图）
SPEC_TYPE_BY_CHART = {
    "mwd": ("MWD_RATIO",),
    "lot": ("LOT_RATIO",),
    "sheet": ("SHEET_ID", "GLASS_ID"),
}

_INDICATOR_KEYS = ["factory", "step_id", "rs_code"]


def build_period_trend_df(
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    end_date: date,
) -> pd.DataFrame:
    """按 period × 指标（厂别+站点+Code）计算 Σcode_qty ÷ 过货 distinct sheet 数。

    分母为 0 时 value 记 NaN（不除零）；某 Code 在某 period 无数据时该行不存在。
    """
    columns = [
        "period_type",
        "period_label",
        "period_sort",
        *_INDICATOR_KEYS,
        "rs_qty",
        "sheet_qty",
        "value",
    ]
    if rs_details_df.empty:
        return pd.DataFrame(columns=columns)

    details = rs_details_df.copy()
    details["start_time"] = pd.to_datetime(details["start_time"], errors="coerce")
    details = details.dropna(subset=["start_time"])
    if details.empty:
        return pd.DataFrame(columns=columns)

    # 复用 SPC 的"跳过空值向前补全"切分（其入参列名为 sheet_start_time）
    axis_input = details[["start_time"]].rename(columns={"start_time": "sheet_start_time"})
    axis = build_available_period_axis(axis_input, end_date)

    pass_df = pd.DataFrame(columns=["factory", "step_id", "sheet_id", "start_time"])
    if not pass_through_df.empty:
        pass_df = pass_through_df.copy()
        pass_df["start_time"] = pd.to_datetime(pass_df["start_time"], errors="coerce")
        pass_df = pass_df.dropna(subset=["start_time"])

    records: list[dict[str, object]] = []
    for period in axis.itertuples(index=False):
        window_start = pd.Timestamp(period.period_start)
        window_end = pd.Timestamp(period.period_end) + pd.Timedelta(days=1)

        period_details = details[
            (details["start_time"] >= window_start) & (details["start_time"] < window_end)
        ]
        if period_details.empty:
            continue

        numerator = (
            period_details.groupby(_INDICATOR_KEYS, as_index=False)["code_qty"]
            .sum()
            .rename(columns={"code_qty": "rs_qty"})
        )

        if pass_df.empty:
            denominator = pd.DataFrame(columns=["factory", "step_id", "sheet_qty"])
        else:
            period_pass = pass_df[
                (pass_df["start_time"] >= window_start) & (pass_df["start_time"] < window_end)
            ]
            denominator = (
                period_pass.groupby(["factory", "step_id"])["sheet_id"]
                .nunique()
                .reset_index(name="sheet_qty")
            )

        merged = numerator.merge(denominator, on=["factory", "step_id"], how="left")
        merged["sheet_qty"] = merged["sheet_qty"].fillna(0).astype(int)
        merged["value"] = merged.apply(
            lambda row: row["rs_qty"] / row["sheet_qty"] if row["sheet_qty"] > 0 else pd.NA,
            axis=1,
        )
        merged["value"] = pd.to_numeric(merged["value"], errors="coerce")
        merged.insert(0, "period_sort", period.period_sort)
        merged.insert(0, "period_label", period.period_label)
        merged.insert(0, "period_type", period.period_type)
        records.extend(merged[columns].to_dict("records"))

    return pd.DataFrame(records, columns=columns)


def build_lot_point_df(
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
) -> pd.DataFrame:
    """By Lot：每个 lot 的 Lot 内平均每片 RS 个数 = Σcode_qty ÷ 该 lot 同站点过货 distinct sheet 数。

    分母口径与月周天趋势一致（过货视图 distinct sheet/glass）；分母为 0 记 NaN。
    按首次过货时间排序。
    """
    columns = [*_INDICATOR_KEYS, "lot_id", "rs_qty", "sheet_qty", "value", "first_start_time"]
    if rs_details_df.empty:
        return pd.DataFrame(columns=columns)

    lots = (
        rs_details_df.groupby([*_INDICATOR_KEYS, "lot_id"], as_index=False)
        .agg(rs_qty=("code_qty", "sum"), first_start_time=("start_time", "min"))
        .sort_values([*_INDICATOR_KEYS, "first_start_time"], kind="stable")
        .reset_index(drop=True)
    )

    if not pass_through_df.empty and "lot_id" in pass_through_df.columns:
        denominator = (
            pass_through_df.groupby(["factory", "step_id", "lot_id"])["sheet_id"]
            .nunique()
            .reset_index(name="sheet_qty")
        )
        lots = lots.merge(denominator, on=["factory", "step_id", "lot_id"], how="left")
    else:
        lots["sheet_qty"] = 0
    lots["sheet_qty"] = lots["sheet_qty"].fillna(0).astype(int)
    lots["value"] = lots.apply(
        lambda row: row["rs_qty"] / row["sheet_qty"] if row["sheet_qty"] > 0 else pd.NA,
        axis=1,
    )
    lots["value"] = pd.to_numeric(lots["value"], errors="coerce")
    return lots[columns]


def build_sheet_point_df(rs_details_df: pd.DataFrame) -> pd.DataFrame:
    """By Sheet：每个 sheet/glass 的 RS 个数（Σcode_qty），按过货时间排序。"""
    if rs_details_df.empty:
        return pd.DataFrame(
            columns=[*_INDICATOR_KEYS, "sheet_id", "rs_qty", "first_start_time"]
        )
    sheets = (
        rs_details_df.groupby([*_INDICATOR_KEYS, "sheet_id"], as_index=False)
        .agg(rs_qty=("code_qty", "sum"), first_start_time=("start_time", "min"))
        .sort_values([*_INDICATOR_KEYS, "first_start_time"], kind="stable")
        .reset_index(drop=True)
    )
    return sheets


def build_period_throughput_df(
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    end_date: date,
) -> pd.DataFrame:
    """按 period × (厂别+站点) 计算过货量（distinct sheet/glass 数），供趋势图柱状图。

    覆盖 period 轴上的全部 period（无过货记 0），与具体 Code 无关；
    period 轴仍由 RS 明细可用性决定（与 build_period_trend_df 同轴）。
    """
    columns = ["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]
    if rs_details_df.empty:
        return pd.DataFrame(columns=columns)

    details = rs_details_df.copy()
    details["start_time"] = pd.to_datetime(details["start_time"], errors="coerce")
    details = details.dropna(subset=["start_time"])
    if details.empty:
        return pd.DataFrame(columns=columns)

    axis_input = details[["start_time"]].rename(columns={"start_time": "sheet_start_time"})
    axis = build_available_period_axis(axis_input, end_date)

    pass_df = pd.DataFrame(columns=["factory", "step_id", "sheet_id", "start_time"])
    if not pass_through_df.empty:
        pass_df = pass_through_df.copy()
        pass_df["start_time"] = pd.to_datetime(pass_df["start_time"], errors="coerce")
        pass_df = pass_df.dropna(subset=["start_time"])

    step_groups = details[["factory", "step_id"]].drop_duplicates()
    records: list[dict[str, object]] = []
    for period in axis.itertuples(index=False):
        window_start = pd.Timestamp(period.period_start)
        window_end = pd.Timestamp(period.period_end) + pd.Timedelta(days=1)
        if pass_df.empty:
            period_pass = pass_df
        else:
            period_pass = pass_df[
                (pass_df["start_time"] >= window_start) & (pass_df["start_time"] < window_end)
            ]
        counts = (
            period_pass.groupby(["factory", "step_id"])["sheet_id"]
            .nunique()
            .to_dict()
        )
        for group in step_groups.itertuples(index=False):
            records.append(
                {
                    "period_type": period.period_type,
                    "period_label": period.period_label,
                    "period_sort": period.period_sort,
                    "factory": group.factory,
                    "step_id": group.step_id,
                    "sheet_qty": int(counts.get((group.factory, group.step_id), 0)),
                }
            )
    return pd.DataFrame(records, columns=columns)


def attach_spec_values(    df: pd.DataFrame,
    spec_df: pd.DataFrame,
    *,
    chart_kind: str,
) -> pd.DataFrame:
    """按图类型把规格上限值左连接到指标行上；无规格为 NaN。

    chart_kind ∈ {"mwd", "lot", "sheet"}，对应 SPEC_TYPE_BY_CHART 的 type_flag。
    """
    result = df.copy()
    type_flags = SPEC_TYPE_BY_CHART[chart_kind]
    if spec_df.empty or result.empty:
        result["spec"] = pd.NA
        return result

    spec = spec_df[spec_df["type_flag"].isin(type_flags)]
    spec = (
        spec.groupby(_INDICATOR_KEYS, as_index=False)["spec"]
        .max()
        if not spec.empty
        else pd.DataFrame(columns=[*_INDICATOR_KEYS, "spec"])
    )
    result = result.merge(spec, on=_INDICATOR_KEYS, how="left")
    result["spec"] = pd.to_numeric(result["spec"], errors="coerce")
    return result
