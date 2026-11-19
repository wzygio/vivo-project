"""Figure builders for AOI TT trend, Lot and Sheet charts."""

import pandas as pd
import plotly.graph_objects as go

from app.charts.inline_domain.aoi_charts import (
    AoiSpecLine,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
)

USL_COLOR = "#dc2626"
UCL_COLOR = "#f59e0b"


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
