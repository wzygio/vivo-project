"""AOI_RS 报表 Dashboard：筛选（厂别/站点/Code名称）与三张图渲染。

图表口径：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：
   值 = Σcode_qty ÷ 同 period 同站点过货 distinct sheet/glass 数，按 RS Code 分线，
   规格线取 type_flag=MWD_RATIO。
2. By Lot 别点线图：每 lot Σcode_qty，规格线 LOT_RATIO。
3. By Sheet 别点线图：每 sheet/glass Σcode_qty，规格线 SHEET_ID/GLASS_ID。
"""

from __future__ import annotations

import re
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from app.components.distribution_charts import create_point_line_trace
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    attach_spec_values,
    build_period_throughput_df,
    build_period_trend_df,
)
from src.inline_domain.core.spc.spc_calculator import get_period_window_start

AOI_RS_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]
CODE_PALETTE = ["#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#0f766e", "#dc2626", "#64748b"]
PERIOD_BAR_COLORS = {
    "month": "rgba(37, 99, 235, 0.55)",
    "week": "rgba(22, 163, 74, 0.55)",
    "day": "rgba(245, 158, 11, 0.55)",
}
PERIOD_TYPE_NAMES = {"month": "月", "week": "周", "day": "天"}
# 月/周/天组间留白：零宽空格分隔符，两个间隙用不同数量避免 category 轴合并
_PERIOD_SEPARATORS = ["​", "​​"]


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
    showlegend: bool = True,
) -> None:
    figure.add_trace(
        go.Scatter(
            x=[x_values[0], x_values[-1]],
            y=[spec_value, spec_value],
            mode="lines",
            name=name,
            line={"color": color, "width": 1.5, "dash": "dash"},
            showlegend=showlegend,
            hovertemplate=f"{name}: %{{y}}<extra></extra>",
        )
    )


def create_aoi_rs_trend_chart(
    *,
    trend_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    spec_value: float | None,
    code_name: str,
    title: str,
) -> go.Figure:
    """单 Code 月周天趋势图：双 Y 轴（左=RS/片比值线+规格，右=过货量柱）。

    x 轴按 period_sort 排列（2月→3周→7天），月/周/天组间插入零宽空格留白，
    柱状按 period_type 分组配色以区分粒度。
    """
    figure = make_subplots(specs=[[{"secondary_y": True}]])

    axis_source = throughput_df if not throughput_df.empty else trend_df
    axis = (
        axis_source[["period_type", "period_label", "period_sort"]]
        .drop_duplicates()
        .sort_values("period_sort", kind="stable")
    )
    if axis.empty:
        return figure

    grouped_labels: list[tuple[str, list[str]]] = []
    for period_type in ("month", "week", "day"):
        labels = axis[axis["period_type"] == period_type]["period_label"].astype(str).tolist()
        if labels:
            grouped_labels.append((period_type, labels))

    # 显示标签去掉年份前缀（2026-07→07、2026-W31→W31、2026-08-10→08-10），值映射仍用原始标签
    def _display(label: str) -> str:
        return re.sub(r"^\d{4}-", "", label)

    x_labels: list[str] = []
    raw_labels: list[str] = []
    separator_positions: list[int] = []
    for group_index, (_period_type, labels) in enumerate(grouped_labels):
        if group_index > 0:
            separator_positions.append(len(x_labels))
            x_labels.append(_PERIOD_SEPARATORS[(group_index - 1) % len(_PERIOD_SEPARATORS)])
            raw_labels.append("")
        x_labels.extend(_display(label) for label in labels)
        raw_labels.extend(labels)

    # 过货量柱状（次 Y 轴，按粒度分组配色）
    throughput_map = (
        throughput_df.set_index("period_label")["sheet_qty"].to_dict()
        if not throughput_df.empty
        else {}
    )
    for period_type, labels in grouped_labels:
        figure.add_trace(
            go.Bar(
                x=[_display(label) for label in labels],
                y=[int(throughput_map.get(label, 0)) for label in labels],
                name=f"过货量（{PERIOD_TYPE_NAMES[period_type]}）",
                marker_color=PERIOD_BAR_COLORS[period_type],
                hovertemplate="%{x}<br>过货量: %{y} 片<extra></extra>",
            ),
            secondary_y=True,
        )

    # 单 Code 比值线（主 Y 轴），分隔位断开；折线与规格线不进图注
    trend_map = (
        trend_df.set_index("period_label")["value"].to_dict() if not trend_df.empty else {}
    )
    y_values = []
    for position, raw_label in enumerate(raw_labels):
        if position in separator_positions:
            y_values.append(None)
            continue
        value = trend_map.get(raw_label)
        y_values.append(float(value) if value is not None and pd.notna(value) else None)
    line_trace = create_point_line_trace(
        x_values=x_labels,
        y_values=y_values,
        name=code_name,
        color=CODE_PALETTE[0],
        hovertemplate="%{x}<br>RS/片: %{y:.3f}<extra></extra>",
    )
    line_trace.showlegend = False
    figure.add_trace(line_trace, secondary_y=False)

    if spec_value is not None and pd.notna(spec_value):
        _add_spec_trace(
            figure, x_labels, float(spec_value), f"{code_name} 规格", CODE_PALETTE[0], showlegend=False
        )

    figure.update_layout(
        title=title,
        xaxis={"type": "category", "title": "期间（月/周/天）"},
        yaxis={"title": "平均每片 RS 个数"},
        yaxis2={"title": "过货量（片）", "overlaying": "y", "side": "right", "showgrid": False},
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
    y_col: str = "rs_qty",
) -> go.Figure:
    """By Lot / By Sheet 点线图：x 按首次过货时间排序，每个 Code 一条线 + 规格线。

    y_col 指定纵轴列：By Sheet 用 "rs_qty"（每片个数），By Lot 用 "value"（Lot 内平均每片）。
    """
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
            (float(code_df.loc[x, y_col]) if x in code_df.index and pd.notna(code_df.loc[x, y_col]) else None)
            for x in x_order
        ]
        figure.add_trace(
            create_point_line_trace(
                x_values=x_order,
                y_values=y_values,
                name=code_names.get(code, code),
                color=colors[code],
                hovertemplate=f"%{{x}}<br>{y_title}: %{{y}}<extra></extra>",
            )
        )
        spec_value = code_specs.get(code)
        if spec_value is not None and pd.notna(spec_value):
            _add_spec_trace(figure, x_order, float(spec_value), f"{code_names.get(code, code)} 规格", colors[code])

    figure.update_layout(
        title=title,
        xaxis={"type": "category", "title": id_col},
        yaxis={"title": y_title},
        legend={"orientation": "h", "yanchor": "top", "y": -0.5},
        margin={"l": 40, "r": 20, "t": 60, "b": 200},
        height=520,
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
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    end_date: date,
) -> None:
    """按（厂别+站点）分组，组内每个 Code 一个默认展开的 Expander，并列渲染三张图。

    lot/sheet 点帧由 service 层完成超规修饰后传入，本层仅做筛选与渲染（D4）。
    """
    if rs_details_df.empty or indicators_df.empty:
        st.info("当前筛选条件下暂无 AOI RS 数据。")
        return

    trend_df = build_period_trend_df(rs_details_df, pass_through_df, end_date)
    throughput_df = build_period_throughput_df(rs_details_df, pass_through_df, end_date)
    lot_df = lot_points_df
    sheet_df = sheet_points_df
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
        mwd_specs = _code_spec_map(group_indicators, spec_df, "mwd")
        lot_specs = _code_spec_map(group_indicators, spec_df, "lot")
        sheet_specs = _code_spec_map(group_indicators, spec_df, "sheet")

        for indicator in group_indicators.itertuples(index=False):
            code = str(indicator.rs_code)
            code_name = code_names.get(code, code)
            with st.expander(f"{code_name} | 站点 {step_id}", expanded=True):
                c_trend, c_lot, c_sheet = st.columns(3)
                with c_trend:
                    st.plotly_chart(
                        create_aoi_rs_trend_chart(
                            trend_df=step_trend[step_trend["rs_code"].astype(str) == code],
                            throughput_df=step_throughput,
                            spec_value=mwd_specs.get(code),
                            code_name=code_name,
                            title="月周天趋势（平均每片 RS 个数）",
                        ),
                        width="stretch",
                    )
                with c_lot:
                    st.plotly_chart(
                        create_aoi_rs_point_chart(
                            point_df=step_lot[step_lot["rs_code"].astype(str) == code],
                            id_col="lot_id",
                            code_specs={code: lot_specs.get(code)},
                            code_names=code_names,
                            title="By Lot（Lot 内平均每片 RS 个数）",
                            y_title="平均每片 RS 个数",
                            y_col="value",
                        ),
                        width="stretch",
                    )
                with c_sheet:
                    st.plotly_chart(
                        create_aoi_rs_point_chart(
                            point_df=step_sheet[step_sheet["rs_code"].astype(str) == code],
                            id_col="sheet_id",
                            code_specs={code: sheet_specs.get(code)},
                            code_names=code_names,
                            title="By Sheet（每片的 RS 个数）",
                            y_title="RS 个数",
                        ),
                        width="stretch",
                    )
