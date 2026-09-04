"""AOI_TT 报表核心聚合：月周天趋势（分子/分母）、By Lot、By Sheet 与规格线匹配。

口径（依据 docs/dev_docs/dev_prompt/feat-AOI_TT.md 与 references/domain/aoi_tt/spec-data_source.md）：
- 月周天趋势：每个 period 的值 = Σtt_qty ÷ 同 period 同站点 distinct sheet/glass 数（AOI 检测片数，
  分母取自 TT 测量表自身——TDSUM/DSUM 每片必测；过货视图不含 AOI 站点记录，不可用）；
  period 轴复用 SPC 的 build_available_period_axis（跳过空值向前补全，最近 2 月/3 周/7 天）。
- By Lot：每个 lot 的 Lot 内平均每片 TT 个数 = Σtt_qty ÷ 该 lot 内 distinct sheet 数；
  By Sheet：每个 sheet 的 TT 个数 = Σtt_qty（每 (step,sheet,param) 恰一行，By Sheet 即原值）。
- 规格线：mdw.dwd_imp_dv_param_spec 的 usl/ucl（越小越好型上限），按 (step_id, tt_name) 匹配，
  三张图共用；无规格时不画线（NaN）。
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.inline_domain.core.spc.spc_calculator import build_available_period_axis

_INDICATOR_KEYS = ["factory", "step_id", "tt_name", "particle_size"]
# 规格表无 factory 列（step_id 全局唯一隐含厂别），规格匹配备用键
_SPEC_KEYS = ["step_id", "tt_name"]
PARTICLE_SIZE_OPTIONS = ("Total", "O", "L")
_PARTICLE_JOIN_KEYS = ["factory", "prod_code", "step_id", "sheet_id"]


def build_particle_size_details(
    tt_details_df: pd.DataFrame,
    particle_counts_df: pd.DataFrame,
) -> pd.DataFrame:
    """为每个 ARRAY/TDSUM Sheet 补齐 O/L 明细，同时原样保留 Total 明细。"""
    if tt_details_df.empty:
        return tt_details_df.assign(particle_size=pd.Series(dtype="object"))

    total_details = tt_details_df.copy()
    total_details["particle_size"] = "Total"
    eligible = total_details[
        total_details["factory"].astype(str).str.upper().eq("ARRAY")
        & total_details["tt_name"].astype(str).str.upper().eq("TDSUM")
    ]
    if eligible.empty:
        return total_details

    sheet_base = (
        eligible.sort_values("start_time", kind="stable")
        .drop_duplicates([*_PARTICLE_JOIN_KEYS, "tt_name"], keep="first")
        .drop(columns=["tt_qty", "particle_size"])
    )
    particle_sizes = pd.DataFrame({"particle_size": ["O", "L"]})
    expanded = sheet_base.merge(particle_sizes, how="cross")

    if particle_counts_df.empty:
        expanded["tt_qty"] = 0
    else:
        counts = particle_counts_df.copy()
        counts["particle_size"] = (
            counts["particle_size"].astype(str).str.strip().str.upper()
        )
        counts["particle_qty"] = pd.to_numeric(
            counts["particle_qty"], errors="coerce"
        ).fillna(0)
        counts = (
            counts[counts["particle_size"].isin({"O", "L"})]
            .groupby([*_PARTICLE_JOIN_KEYS, "particle_size"], as_index=False)["particle_qty"]
            .sum()
            .rename(columns={"particle_qty": "tt_qty"})
        )
        expanded = expanded.merge(
            counts,
            on=[*_PARTICLE_JOIN_KEYS, "particle_size"],
            how="left",
        )
        expanded["tt_qty"] = expanded["tt_qty"].fillna(0)

    expanded = expanded.reindex(columns=total_details.columns)
    return pd.concat([total_details, expanded], ignore_index=True)


def _prepare_details(tt_details_df: pd.DataFrame) -> pd.DataFrame:
    details = tt_details_df.copy()
    if "particle_size" not in details.columns:
        details["particle_size"] = "Total"
    else:
        details["particle_size"] = details["particle_size"].fillna("Total")
    details["start_time"] = pd.to_datetime(details["start_time"], errors="coerce")
    return details.dropna(subset=["start_time"])


def build_period_trend_df(
    tt_details_df: pd.DataFrame,
    end_date: date,
) -> pd.DataFrame:
    """按 period × 指标（厂别+站点+TT）计算 Σtt_qty ÷ 检测 distinct sheet 数。

    分母为 0 时 value 记 NaN（不除零）；某 TT 在某 period 无数据时该行不存在。
    """
    columns = [
        "period_type",
        "period_label",
        "period_sort",
        *_INDICATOR_KEYS,
        "tt_qty",
        "sheet_qty",
        "value",
    ]
    if tt_details_df.empty:
        return pd.DataFrame(columns=columns)

    details = _prepare_details(tt_details_df)
    if details.empty:
        return pd.DataFrame(columns=columns)

    # 复用 SPC 的"跳过空值向前补全"切分（其入参列名为 sheet_start_time）
    axis_input = details[["start_time"]].rename(columns={"start_time": "sheet_start_time"})
    axis = build_available_period_axis(axis_input, end_date)

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
            period_details.groupby(_INDICATOR_KEYS, as_index=False)["tt_qty"]
            .sum()
            .rename(columns={"tt_qty": "tt_qty"})
        )
        # 分母 = 测量表自身 distinct sheet（AOI 检测片数）
        denominator = (
            period_details.groupby(["factory", "step_id"])["sheet_id"]
            .nunique()
            .reset_index(name="sheet_qty")
        )

        merged = numerator.merge(denominator, on=["factory", "step_id"], how="left")
        merged["sheet_qty"] = merged["sheet_qty"].fillna(0).astype(int)
        merged["value"] = merged.apply(
            lambda row: row["tt_qty"] / row["sheet_qty"] if row["sheet_qty"] > 0 else pd.NA,
            axis=1,
        )
        merged["value"] = pd.to_numeric(merged["value"], errors="coerce")
        merged.insert(0, "period_sort", period.period_sort)
        merged.insert(0, "period_label", period.period_label)
        merged.insert(0, "period_type", period.period_type)
        records.extend(merged[columns].to_dict("records"))

    return pd.DataFrame(records, columns=columns)


def build_lot_point_df(tt_details_df: pd.DataFrame) -> pd.DataFrame:
    """By Lot：每个 lot 的 Lot 内平均每片 TT 个数 = Σtt_qty ÷ 该 lot 内 distinct sheet 数。

    分母取自 TT 测量表自身（TDSUM/DSUM 每片必测，含 tt_qty=0 的片）；
    lot 有记录即分母 ≥1，不存在除零。按首次过货时间排序。
    """
    if tt_details_df.empty:
        return pd.DataFrame(
            columns=[*_INDICATOR_KEYS, "lot_id", "tt_qty", "sheet_qty", "value", "first_start_time"]
        )
    details = _prepare_details(tt_details_df)
    lots = (
        details.groupby([*_INDICATOR_KEYS, "lot_id"], as_index=False)
        .agg(
            tt_qty=("tt_qty", "sum"),
            sheet_qty=("sheet_id", "nunique"),
            first_start_time=("start_time", "min"),
        )
        .sort_values([*_INDICATOR_KEYS, "first_start_time"], kind="stable")
        .reset_index(drop=True)
    )
    lots["value"] = lots.apply(
        lambda row: row["tt_qty"] / row["sheet_qty"] if row["sheet_qty"] > 0 else pd.NA,
        axis=1,
    )
    lots["value"] = pd.to_numeric(lots["value"], errors="coerce")
    return lots


def build_sheet_point_df(tt_details_df: pd.DataFrame) -> pd.DataFrame:
    """By Sheet：每个 sheet/glass 的 TT 个数（Σtt_qty），按过货时间排序。"""
    if tt_details_df.empty:
        return pd.DataFrame(
            columns=[*_INDICATOR_KEYS, "sheet_id", "tt_qty", "first_start_time"]
        )
    details = _prepare_details(tt_details_df)
    sheets = (
        details.groupby([*_INDICATOR_KEYS, "sheet_id"], as_index=False)
        .agg(tt_qty=("tt_qty", "sum"), first_start_time=("start_time", "min"))
        .sort_values([*_INDICATOR_KEYS, "first_start_time"], kind="stable")
        .reset_index(drop=True)
    )
    return sheets


def build_period_throughput_df(
    tt_details_df: pd.DataFrame,
    end_date: date,
) -> pd.DataFrame:
    """按 period × (厂别+站点) 计算检测片数（distinct sheet/glass），供趋势图柱状图。

    覆盖 period 轴上的全部 period（无检测记 0），与具体 TT 无关；
    period 轴仍由 TT 明细可用性决定（与 build_period_trend_df 同轴）。
    """
    columns = ["period_type", "period_label", "period_sort", "factory", "step_id", "sheet_qty"]
    if tt_details_df.empty:
        return pd.DataFrame(columns=columns)

    details = _prepare_details(tt_details_df)
    if details.empty:
        return pd.DataFrame(columns=columns)

    axis_input = details[["start_time"]].rename(columns={"start_time": "sheet_start_time"})
    axis = build_available_period_axis(axis_input, end_date)

    step_groups = details[["factory", "step_id"]].drop_duplicates()
    records: list[dict[str, object]] = []
    for period in axis.itertuples(index=False):
        window_start = pd.Timestamp(period.period_start)
        window_end = pd.Timestamp(period.period_end) + pd.Timedelta(days=1)
        period_details = details[
            (details["start_time"] >= window_start) & (details["start_time"] < window_end)
        ]
        counts = (
            period_details.groupby(["factory", "step_id"])["sheet_id"]
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


def attach_spec_values(
    df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """把 USL/UCL 规格上限左连接到指标行上（按 step_id+tt_name）；无规格为 NaN。"""
    result = df.copy()
    if spec_df.empty or result.empty:
        result["usl"] = pd.NA
        result["ucl"] = pd.NA
        return result

    spec = (
        spec_df.groupby(_SPEC_KEYS, as_index=False)[["usl", "ucl"]]
        .max()
        if not spec_df.empty
        else pd.DataFrame(columns=[*_SPEC_KEYS, "usl", "ucl"])
    )
    result = result.merge(spec, on=_SPEC_KEYS, how="left")
    result["usl"] = pd.to_numeric(result["usl"], errors="coerce")
    result["ucl"] = pd.to_numeric(result["ucl"], errors="coerce")
    return result
