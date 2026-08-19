"""AOI_TT 报表 Dashboard：筛选（厂别/站点/Code名称）与三张图渲染。

图表口径（与 AOI_RS 布局一致，规格线为 USL/UCL 双上限）：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：
   值 = Σtt_qty ÷ 同 period 同站点检测 distinct sheet/glass 数，按 TT 分线，
   叠加 USL/UCL 规格线，双 Y 轴检测片数柱状。
2. By Lot 别点线图：每 lot 的 Lot 内平均每片 TT 个数（Σtt_qty ÷ Lot 内检测片数），叠加 USL/UCL。
3. By Sheet 别点线图：每 sheet/glass 的 TT 个数，叠加 USL/UCL。

公共筛选与绘图管线位于 ``app.sections.inline_domain.shared``，本模块只保留
TT 业务差异：USL/UCL 双上限规格线（虚线/点线）与检测片数文案。
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.sections.inline_domain.shared import (
    AoiSpecLine,
    INLINE_FACTORY_OPTIONS,
    apply_report_filter,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
    get_available_factories as _shared_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory as _shared_steps_for_factory,
    render_cascade_filters,
)
from app.utils.step_labels import format_step_label
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import (
    attach_spec_values,
    build_lot_point_df,
    build_period_throughput_df,
    build_period_trend_df,
    build_sheet_point_df,
)
from src.inline_domain.core.spc.spc_calculator import get_period_window_start

AOI_TT_FACTORY_OPTIONS = INLINE_FACTORY_OPTIONS
USL_COLOR = "#dc2626"
UCL_COLOR = "#f59e0b"


def get_default_aoi_tt_start_date(end_date: date) -> date:
    """固定窗口起点：上一自然月 1 日。"""
    return get_period_window_start(end_date)


# ---------------------------------------------------------------------------
# 筛选
# ---------------------------------------------------------------------------


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    return _shared_available_factories(report_df, AOI_TT_FACTORY_OPTIONS)


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    return _shared_steps_for_factory(report_df, selected_factory)


def get_codes_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    return get_options_for_factory_steps(report_df, selected_factory, selected_steps, "tt_name")


def render_aoi_tt_filters(
    indicator_df: pd.DataFrame,
    *,
    step_desc_map: dict[str, str] | None = None,
) -> tuple[str, list[str], list[str], bool]:
    """渲染厂别/站点/Code名称筛选与查询门控，返回 (factory, codes, steps, should_render)。"""
    return render_cascade_filters(
        indicator_df,
        key_prefix="aoi_tt",
        third_label="Code名称",
        third_column="tt_name",
        third_kind="code",
        factory_options=AOI_TT_FACTORY_OPTIONS,
        step_desc_map=step_desc_map,
    )


def filter_aoi_tt_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_codes: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """对含 factory/tt_name/step_id 列的数据框应用前端筛选。"""
    return apply_report_filter(
        report_df,
        selected_factory,
        selected_codes,
        selected_steps,
        third_column="tt_name",
    )


# ---------------------------------------------------------------------------
# 图表
# ---------------------------------------------------------------------------


def _usl_ucl_spec_lines(usl: float | None, ucl: float | None) -> list[AoiSpecLine]:
    """TT 双上限规格线：USL 虚线、UCL 点线；缺省值由 shared 跳过。"""
    return [
        AoiSpecLine(usl, "USL", USL_COLOR, "dash"),
        AoiSpecLine(ucl, "UCL", UCL_COLOR, "dot"),
    ]


def create_aoi_tt_trend_chart(
    *,
    trend_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    usl: float | None,
    ucl: float | None,
    code_name: str,
    title: str,
) -> go.Figure:
    """单 TT 月周天趋势图：双 Y 轴（左=TT/片比值线+USL/UCL，右=检测片数柱）。

    x 轴按 period_sort 排列（2月→3周→7天），月/周/天组间插入零宽空格留白，
    柱状按 period_type 分组配色以区分粒度。
    """
    return create_aoi_period_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_lines=_usl_ucl_spec_lines(usl, ucl),
        code_name=code_name,
        title=title,
        line_value_label="TT/片",
        bar_unit_name="检测片数",
        y_title="平均每片 TT 个数",
    )


def create_aoi_tt_point_chart(
    *,
    point_df: pd.DataFrame,
    id_col: str,
    code_specs: dict[str, tuple[float | None, float | None]],
    title: str,
    y_title: str,
    y_col: str = "tt_qty",
) -> go.Figure:
    """By Lot / By Sheet 点线图：x 按首次过货时间排序，每个 TT 一条线 + USL/UCL。

    y_col 指定纵轴列：By Sheet 用 "tt_qty"（每片个数），By Lot 用 "value"（Lot 内平均每片）。
    """
    spec_lines = {
        code: _usl_ucl_spec_lines(usl, ucl)
        for code, (usl, ucl) in code_specs.items()
    }
    return create_aoi_point_chart(
        point_df=point_df,
        id_col=id_col,
        code_column="tt_name",
        code_specs=spec_lines,
        title=title,
        y_title=y_title,
        y_col=y_col,
    )


# ---------------------------------------------------------------------------
# 渲染
# ---------------------------------------------------------------------------


def _code_spec_map(
    indicators_df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> dict[str, tuple[float | None, float | None]]:
    """tt_name → (usl, ucl)（规格表无 factory 列，按 step_id+tt_name 匹配）。"""
    keyed = attach_spec_values(
        indicators_df[["step_id", "tt_name"]].drop_duplicates(),
        spec_df,
    )
    return {
        str(row.tt_name): (
            float(row.usl) if pd.notna(row.usl) else None,
            float(row.ucl) if pd.notna(row.ucl) else None,
        )
        for row in keyed.itertuples(index=False)
    }


def render_aoi_tt_indicator_sections(
    *,
    tt_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    end_date: date,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """按（厂别+站点）分组，组内每个 TT 一个默认展开的 Expander，并列渲染三张图。"""
    if tt_details_df.empty or indicators_df.empty:
        st.info("当前筛选条件下暂无 AOI TT 数据。")
        return

    trend_df = build_period_trend_df(tt_details_df, end_date)
    throughput_df = build_period_throughput_df(tt_details_df, end_date)
    lot_df = build_lot_point_df(tt_details_df)
    sheet_df = build_sheet_point_df(tt_details_df)

    groups = (
        indicators_df[["factory", "step_id"]]
        .drop_duplicates()
        .sort_values(["factory", "step_id"], kind="stable")
    )
    for group in groups.itertuples(index=False):
        factory, step_id = str(group.factory), str(group.step_id)
        group_indicators = indicators_df[
            (indicators_df["factory"].astype(str) == factory)
            & (indicators_df["step_id"].astype(str) == step_id)
        ]
        st.subheader(f"{factory} | 站点 {format_step_label(step_id, step_desc_map)}")

        step_trend = trend_df[
            (trend_df["factory"].astype(str) == factory)
            & (trend_df["step_id"].astype(str) == step_id)
        ]
        step_throughput = throughput_df[
            (throughput_df["factory"].astype(str) == factory)
            & (throughput_df["step_id"].astype(str) == step_id)
        ]
        step_lot = lot_df[
            (lot_df["factory"].astype(str) == factory)
            & (lot_df["step_id"].astype(str) == step_id)
        ]
        step_sheet = sheet_df[
            (sheet_df["factory"].astype(str) == factory)
            & (sheet_df["step_id"].astype(str) == step_id)
        ]
        code_specs = _code_spec_map(group_indicators, spec_df)

        for indicator in group_indicators.itertuples(index=False):
            code = str(indicator.tt_name)
            usl, ucl = code_specs.get(code, (None, None))
            with st.expander(f"{code} | 站点 {format_step_label(step_id, step_desc_map)}", expanded=True):
                c_trend, c_lot, c_sheet = st.columns(3)
                with c_trend:
                    st.plotly_chart(
                        create_aoi_tt_trend_chart(
                            trend_df=step_trend[step_trend["tt_name"].astype(str) == code],
                            throughput_df=step_throughput,
                            usl=usl,
                            ucl=ucl,
                            code_name=code,
                            title="月周天趋势（平均每片 TT 个数）",
                        ),
                        width="stretch",
                    )
                with c_lot:
                    st.plotly_chart(
                        create_aoi_tt_point_chart(
                            point_df=step_lot[step_lot["tt_name"].astype(str) == code],
                            id_col="lot_id",
                            code_specs={code: (usl, ucl)},
                            title="By Lot（Lot 内平均每片 TT 个数）",
                            y_title="平均每片 TT 个数",
                            y_col="value",
                        ),
                        width="stretch",
                    )
                with c_sheet:
                    st.plotly_chart(
                        create_aoi_tt_point_chart(
                            point_df=step_sheet[step_sheet["tt_name"].astype(str) == code],
                            id_col="sheet_id",
                            code_specs={code: (usl, ucl)},
                            title="By Sheet（每片的 TT 个数）",
                            y_title="TT 个数",
                        ),
                        width="stretch",
                    )
