"""AOI_RS 报表 Dashboard：筛选（厂别/站点/Code名称）与三张图渲染。

图表口径：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：
   值 = Σcode_qty ÷ 同 period 同站点过货 distinct sheet/glass 数，按 RS Code 分线，
   规格线取 type_flag=MWD_RATIO。
2. By Lot 别点线图：每 lot Σcode_qty，规格线 LOT_RATIO。
3. By Sheet 别点线图：每 sheet/glass Σcode_qty，规格线 SHEET_ID/GLASS_ID。
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.components.distribution_charts import create_point_line_trace
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    attach_spec_values,
    build_lot_point_df,
    build_period_trend_df,
    build_sheet_point_df,
)
from src.inline_domain.core.spc.spc_calculator import get_period_window_start

AOI_RS_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]
CODE_PALETTE = ["#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#0f766e", "#dc2626", "#64748b"]


def get_default_aoi_rs_start_date(end_date: date) -> date:
    """固定窗口起点：上一自然月 1 日。"""
    return get_period_window_start(end_date)


# ---------------------------------------------------------------------------
# 筛选
# ---------------------------------------------------------------------------


def _unique_sorted(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(df[column].dropna().astype(str).unique().tolist())


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    factories = set(_unique_sorted(report_df, "factory"))
    ordered = [factory for factory in AOI_RS_FACTORY_OPTIONS if factory in factories]
    extras = sorted(factories.difference(AOI_RS_FACTORY_OPTIONS))
    return ordered + extras


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    if report_df.empty or not selected_factory:
        return []
    factory_df = report_df[report_df["factory"].astype(str) == str(selected_factory)]
    return _unique_sorted(factory_df, "step_id")


def get_codes_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    if report_df.empty or not selected_factory or not selected_steps:
        return []
    df = report_df[
        (report_df["factory"].astype(str) == str(selected_factory))
        & (report_df["step_id"].astype(str).isin(selected_steps))
    ]
    return _unique_sorted(df, "rs_code")


def _normalise_selection(selection: list[str], options: list[str]) -> list[str]:
    selected = [item for item in (selection or []) if item in options]
    return selected if selected else []


def _filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_codes: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_codes)


def render_aoi_rs_filters(indicator_df: pd.DataFrame) -> tuple[str, list[str], list[str], bool]:
    """渲染厂别/站点/Code名称筛选与查询门控，返回 (factory, codes, steps, should_render)。"""
    with st.container(border=True):
        st.markdown("#### 筛选")
        c_factory, c_step, c_code, c_query = st.columns(
            [1.1, 2.5, 3.4, 0.9],
            vertical_alignment="bottom",
        )

        available_factories = get_available_factories(indicator_df) or AOI_RS_FACTORY_OPTIONS
        factory_key = "aoi_rs_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with c_factory:
            selected_factory = st.selectbox("厂别", options=available_factories, key=factory_key)

        available_steps = get_steps_for_factory(indicator_df, selected_factory)
        step_key = "aoi_rs_step_filter"
        code_key = "aoi_rs_code_filter"
        previous_factory_key = "aoi_rs_previous_factory_filter"
        if st.session_state.get(previous_factory_key) != selected_factory:
            st.session_state[step_key] = []
            st.session_state[code_key] = []
            st.session_state[previous_factory_key] = selected_factory

        with c_step:
            st.session_state[step_key] = _normalise_selection(
                st.session_state.get(step_key, []), available_steps
            )
            selected_steps = st.multiselect("站点", options=available_steps, key=step_key)

        available_codes = get_codes_for_factory_steps(indicator_df, selected_factory, selected_steps)
        steps_signature_key = "aoi_rs_steps_for_code_autoselect"
        steps_signature = _filter_signature(selected_factory, selected_steps, [])
        if st.session_state.get(steps_signature_key) != steps_signature:
            st.session_state[code_key] = available_codes
            st.session_state[steps_signature_key] = steps_signature
        st.session_state[code_key] = _normalise_selection(
            st.session_state.get(code_key, []), available_codes
        )

        with c_code:
            selected_codes = st.multiselect(
                "Code名称",
                options=available_codes,
                key=code_key,
                disabled=not selected_steps,
            )

        current_signature = _filter_signature(selected_factory, selected_steps, selected_codes)
        applied_signature_key = "aoi_rs_applied_filter_signature"
        can_query = bool(selected_factory and selected_steps and selected_codes)
        with c_query:
            if st.button("查询", type="primary", width="stretch", disabled=not can_query):
                st.session_state[applied_signature_key] = current_signature

    should_render = bool(can_query and st.session_state.get(applied_signature_key) == current_signature)
    return selected_factory, selected_codes, selected_steps, should_render


def filter_aoi_rs_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_codes: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """对含 factory/rs_code/step_id 列的数据框应用前端筛选。"""
    if report_df.empty:
        return report_df

    df = report_df.copy()
    if selected_factory and "factory" in df.columns:
        df = df[df["factory"].astype(str) == str(selected_factory)]
    if selected_codes and "rs_code" in df.columns:
        df = df[df["rs_code"].astype(str).isin(selected_codes)]
    if selected_steps and "step_id" in df.columns:
        df = df[df["step_id"].astype(str).isin(selected_steps)]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 图表
# ---------------------------------------------------------------------------


def _code_color_map(codes: list[str]) -> dict[str, str]:
    return {code: CODE_PALETTE[index % len(CODE_PALETTE)] for index, code in enumerate(codes)}


def _add_spec_trace(
    figure: go.Figure,
    x_values: list[str],
    spec_value: float,
    name: str,
    color: str,
) -> None:
    figure.add_trace(
        go.Scatter(
            x=[x_values[0], x_values[-1]],
            y=[spec_value, spec_value],
            mode="lines",
            name=name,
            line={"color": color, "width": 1.5, "dash": "dash"},
            hovertemplate=f"{name}: %{{y}}<extra></extra>",
        )
    )


def create_aoi_rs_trend_chart(
    *,
    trend_df: pd.DataFrame,
    code_specs: dict[str, float | None],
    code_names: dict[str, str],
    title: str,
) -> go.Figure:
    """月周天趋势图：x = period_label（2月→3周→7天），每个 Code 一条比值线 + 规格线。"""
    figure = go.Figure()
    axis = (
        trend_df[["period_label", "period_sort"]]
        .drop_duplicates()
        .sort_values("period_sort", kind="stable")
    )
    x_labels = axis["period_label"].astype(str).tolist()
    if not x_labels:
        return figure

    codes = sorted(trend_df["rs_code"].astype(str).unique().tolist())
    colors = _code_color_map(codes)
    for code in codes:
        code_df = trend_df[trend_df["rs_code"].astype(str) == code].set_index("period_label")
        y_values = [
            (float(code_df.loc[label, "value"]) if label in code_df.index and pd.notna(code_df.loc[label, "value"]) else None)
            for label in x_labels
        ]
        figure.add_trace(
            create_point_line_trace(
                x_values=x_labels,
                y_values=y_values,
                name=code_names.get(code, code),
                color=colors[code],
                hovertemplate="%{x}<br>RS/片: %{y:.3f}<extra></extra>",
            )
        )
        spec_value = code_specs.get(code)
        if spec_value is not None and pd.notna(spec_value):
            _add_spec_trace(figure, x_labels, float(spec_value), f"{code_names.get(code, code)} 规格", colors[code])

    figure.update_layout(
        title=title,
        xaxis={"type": "category", "title": "期间（月/周/天）"},
        yaxis={"title": "平均每片 RS 个数"},
        legend={"orientation": "h", "yanchor": "top", "y": -0.22},
        margin={"l": 40, "r": 20, "t": 60, "b": 130},
        height=460,
    )
    return figure


def create_aoi_rs_point_chart(
    *,
    point_df: pd.DataFrame,
    id_col: str,
    code_specs: dict[str, float | None],
    code_names: dict[str, str],
    title: str,
    y_title: str,
) -> go.Figure:
    """By Lot / By Sheet 点线图：x 按首次过货时间排序，每个 Code 一条线 + 规格线。"""
    figure = go.Figure()
    if point_df.empty:
        return figure

    x_order = (
        point_df[[id_col, "first_start_time"]]
        .drop_duplicates()
        .sort_values("first_start_time", kind="stable")[id_col]
        .astype(str)
        .tolist()
    )
    codes = sorted(point_df["rs_code"].astype(str).unique().tolist())
    colors = _code_color_map(codes)
    for code in codes:
        code_df = point_df[point_df["rs_code"].astype(str) == code].set_index(id_col)
        y_values = [
            (float(code_df.loc[x, "rs_qty"]) if x in code_df.index else None) for x in x_order
        ]
        figure.add_trace(
            create_point_line_trace(
                x_values=x_order,
                y_values=y_values,
                name=code_names.get(code, code),
                color=colors[code],
                hovertemplate="%{x}<br>RS个数: %{y}<extra></extra>",
            )
        )
        spec_value = code_specs.get(code)
        if spec_value is not None and pd.notna(spec_value):
            _add_spec_trace(figure, x_order, float(spec_value), f"{code_names.get(code, code)} 规格", colors[code])

    figure.update_layout(
        title=title,
        xaxis={"type": "category", "title": id_col},
        yaxis={"title": y_title},
        legend={"orientation": "h", "yanchor": "top", "y": -0.22},
        margin={"l": 40, "r": 20, "t": 60, "b": 130},
        height=460,
    )
    return figure


# ---------------------------------------------------------------------------
# 渲染
# ---------------------------------------------------------------------------


def _code_display_names(indicators_df: pd.DataFrame) -> dict[str, str]:
    """rs_code → 显示名（带中文描述）。"""
    names: dict[str, str] = {}
    for row in indicators_df.itertuples(index=False):
        code = str(getattr(row, "rs_code"))
        desc = getattr(row, "code_desc", None)
        names[code] = f"{code}（{desc}）" if isinstance(desc, str) and desc else code
    return names


def _code_spec_map(indicators_df: pd.DataFrame, spec_df: pd.DataFrame, chart_kind: str) -> dict[str, float | None]:
    keyed = attach_spec_values(
        indicators_df[["factory", "step_id", "rs_code"]].drop_duplicates(),
        spec_df,
        chart_kind=chart_kind,
    )
    return {
        str(row.rs_code): (float(row.spec) if pd.notna(row.spec) else None)
        for row in keyed.itertuples(index=False)
    }


def render_aoi_rs_indicator_sections(
    *,
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    end_date: date,
) -> None:
    """按（厂别+站点）分组渲染三张图：月周天趋势 / By Lot / By Sheet。"""
    if rs_details_df.empty or indicators_df.empty:
        st.info("当前筛选条件下暂无 AOI RS 数据。")
        return

    trend_df = build_period_trend_df(rs_details_df, pass_through_df, end_date)
    lot_df = build_lot_point_df(rs_details_df)
    sheet_df = build_sheet_point_df(rs_details_df)
    code_names = _code_display_names(indicators_df)

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
        st.subheader(f"{factory} | 站点 {step_id}")

        with st.container(border=True):
            st.plotly_chart(
                create_aoi_rs_trend_chart(
                    trend_df=trend_df[
                        (trend_df["factory"].astype(str) == factory)
                        & (trend_df["step_id"].astype(str) == step_id)
                    ],
                    code_specs=_code_spec_map(group_indicators, spec_df, "mwd"),
                    code_names=code_names,
                    title="月周天趋势（平均每片 RS 个数）",
                ),
                width="stretch",
            )
        with st.container(border=True):
            st.plotly_chart(
                create_aoi_rs_point_chart(
                    point_df=lot_df[
                        (lot_df["factory"].astype(str) == factory)
                        & (lot_df["step_id"].astype(str) == step_id)
                    ],
                    id_col="lot_id",
                    code_specs=_code_spec_map(group_indicators, spec_df, "lot"),
                    code_names=code_names,
                    title="By Lot（每个 Lot 的 RS 个数）",
                    y_title="RS 个数",
                ),
                width="stretch",
            )
        with st.container(border=True):
            st.plotly_chart(
                create_aoi_rs_point_chart(
                    point_df=sheet_df[
                        (sheet_df["factory"].astype(str) == factory)
                        & (sheet_df["step_id"].astype(str) == step_id)
                    ],
                    id_col="sheet_id",
                    code_specs=_code_spec_map(group_indicators, spec_df, "sheet"),
                    code_names=code_names,
                    title="By Sheet（每片的 RS 个数）",
                    y_title="RS 个数",
                ),
                width="stretch",
            )
