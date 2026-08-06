from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from functools import partial
from io import BytesIO
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.components.distribution_charts import (
    create_box_distribution_trace,
    create_point_line_trace,
)
from app.manager.render_gate import RenderGate
from src.inline_domain.core.spc.spc_calculator import (
    build_available_period_axis,
    build_period_axis,
    get_period_window_start,
)
from src.inline_domain.core.spc.spc_sheet_oos_decoration import (
    OOS_DECORATION_COLUMNS,
    OOS_DETAIL_COLUMNS,
    OOS_KEY_COLUMNS,
    SheetOosDecorationResult,
)
from src.inline_domain.core.spc.cpk_decoration import (
    CPK_DECORATION_COLUMNS,
    CPK_DETAIL_COLUMNS,
    CPK_KEY_COLUMNS,
    CpkDecorationResult,
)

SPC_FACTORY_OPTIONS = ["ARRAY", "OLED", "TP"]
PERIOD_LABELS = {"month": "月", "week": "周", "day": "日"}
PERIOD_WINDOW_LIMITS = {"month": 2, "week": 3, "day": 7}
PERIOD_COLORS = {"month": "#2563eb", "week": "#16a34a", "day": "#f59e0b"}
PERIOD_FILL_COLORS = {
    "month": "rgba(37, 99, 235, 0.18)",
    "week": "rgba(22, 163, 74, 0.18)",
    "day": "rgba(245, 158, 11, 0.18)",
}
SHEET_BOX_PALETTE = ["#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#0f766e", "#dc2626", "#64748b"]
CPK_ALERT_THRESHOLD = 1.33
CPK_ALERT_COLUMNS = ["厂别", "站点", "参数名称", "超规周次", "CPK值"]
CPK_ALERT_KEY_COLUMN_MAP = {
    "厂别": "factory",
    "站点": "step_id",
    "参数名称": "param_name",
}
CHART_TYPE_BOX = "box"
CHART_TYPE_LINE = "line"


def get_default_spc_start_date(end_date: date) -> date:
    """Return the first day needed by the CPM/CPK Task2 report."""
    return get_period_window_start(end_date)


def build_weekly_cpk_alerts(
    period_capability_df: pd.DataFrame,
    threshold: float = CPK_ALERT_THRESHOLD,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """Return below-threshold, undecorated CPK records from the previous full week."""
    required_columns = {"factory", "step_id", "param_name", "period_type", "period_label", "cpk"}
    if period_capability_df.empty or not required_columns.issubset(period_capability_df.columns):
        return pd.DataFrame(columns=CPK_ALERT_COLUMNS)

    reference_day = pd.Timestamp(reference_date or date.today()).normalize()
    current_week_start = reference_day - pd.Timedelta(days=reference_day.weekday())
    previous_week_start = current_week_start - pd.Timedelta(days=7)
    iso_week = previous_week_start.isocalendar()
    target_week_label = f"{iso_week.year}-W{iso_week.week:02d}"

    capability_df = period_capability_df[
        period_capability_df["period_type"].astype(str).eq("week")
    ].copy()
    if capability_df.empty:
        return pd.DataFrame(columns=CPK_ALERT_COLUMNS)

    capability_df["cpk"] = pd.to_numeric(capability_df["cpk"], errors="coerce")
    is_decorated = (
        capability_df["cpk_decorated"].fillna(False).astype(bool)
        if "cpk_decorated" in capability_df.columns
        else pd.Series(False, index=capability_df.index, dtype=bool)
    )
    alert_rows = capability_df[
        capability_df["cpk"].lt(threshold)
        & capability_df["period_label"].astype(str).eq(target_week_label)
        & ~is_decorated
    ].copy()
    if alert_rows.empty:
        return pd.DataFrame(columns=CPK_ALERT_COLUMNS)

    alerts_df = alert_rows.rename(
        columns={
            "factory": "厂别",
            "step_id": "站点",
            "param_name": "参数名称",
            "period_label": "超规周次",
            "cpk": "CPK值",
        }
    )[CPK_ALERT_COLUMNS]
    alerts_df["超规周次"] = alerts_df["超规周次"].astype(str)
    return alerts_df.sort_values(
        ["厂别", "站点", "参数名称", "CPK值"],
        ascending=[True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def render_cpk_alert_center(
    alerts_df: pd.DataFrame,
    *,
    has_capability_data: bool,
    threshold: float = CPK_ALERT_THRESHOLD,
) -> None:
    """Render the product-level CPK alert summary and details."""
    has_alerts = not alerts_df.empty
    with st.expander(
        f"CPK预警中心（CPK < {threshold:.2f}）",
        expanded=has_alerts,
    ):
        if has_alerts:
            st.error(f"检测到 {len(alerts_df)} 条 CPK 预警，请关注。")
            st.dataframe(
                alerts_df,
                column_config={"CPK值": st.column_config.NumberColumn("CPK值", format="%.3f")},
                hide_index=True,
                use_container_width=True,
            )
        elif has_capability_data:
            st.success(f"未发现低于 {threshold:.2f} 的 CPK。")
        else:
            st.info("当前产品暂无可计算的 CPK 数据。")


def filter_spc_report_by_alerts(
    report_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
) -> pd.DataFrame:
    """Keep only exact factory/station/parameter combinations present in CPK alerts."""
    if report_df.empty or alerts_df.empty:
        return report_df.iloc[0:0].copy()

    alert_columns = set(CPK_ALERT_KEY_COLUMN_MAP)
    report_columns = set(CPK_ALERT_KEY_COLUMN_MAP.values())
    if not alert_columns.issubset(alerts_df.columns) or not report_columns.issubset(report_df.columns):
        return report_df.iloc[0:0].copy()

    alert_keys_df = (
        alerts_df[list(CPK_ALERT_KEY_COLUMN_MAP)]
        .rename(columns=CPK_ALERT_KEY_COLUMN_MAP)
        .astype(str)
        .drop_duplicates()
    )
    report_keys_df = report_df[list(CPK_ALERT_KEY_COLUMN_MAP.values())].astype(str)
    alert_key_index = pd.MultiIndex.from_frame(alert_keys_df)
    report_key_index = pd.MultiIndex.from_frame(report_keys_df)
    return report_df.loc[report_key_index.isin(alert_key_index)].copy().reset_index(drop=True)


def _normalise_selection(selection: Iterable[str], available: list[str]) -> list[str]:
    return [item for item in selection if item in available]


def _unique_sorted(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    return sorted(df[column].dropna().astype(str).unique().tolist())


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    """Return available factories with known factory order preserved."""
    factories = set(_unique_sorted(report_df, "factory"))
    ordered = [factory for factory in SPC_FACTORY_OPTIONS if factory in factories]
    extras = sorted(factories.difference(SPC_FACTORY_OPTIONS))
    return ordered + extras


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    """Return stations available under the selected factory."""
    if report_df.empty or not selected_factory:
        return []
    factory_df = report_df[report_df["factory"].astype(str) == str(selected_factory)]
    return _unique_sorted(factory_df, "step_id")


def get_params_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    """Return parameters available under the selected factory and stations."""
    if report_df.empty or not selected_factory or not selected_steps:
        return []
    df = report_df[
        (report_df["factory"].astype(str) == str(selected_factory))
        & (report_df["step_id"].astype(str).isin(selected_steps))
    ]
    return _unique_sorted(df, "param_name")


def _filter_signature(
    selected_factory: str,
    selected_steps: list[str],
    selected_params: list[str],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return selected_factory, tuple(selected_steps), tuple(selected_params)


def render_spc_filters(indicator_df: pd.DataFrame) -> tuple[str, list[str], list[str], bool]:
    """Render CPM/CPK filters and return selected factory, params, steps, and query state."""
    with st.container(border=True):
        st.markdown("#### 筛选")
        c_factory, c_step, c_param, c_query = st.columns(
            [1.1, 2.5, 3.4, 0.9],
            vertical_alignment="bottom",
        )

        available_factories = get_available_factories(indicator_df) or SPC_FACTORY_OPTIONS
        factory_key = "spc_factory_filter"
        if st.session_state.get(factory_key) not in available_factories:
            st.session_state[factory_key] = available_factories[0]

        with c_factory:
            selected_factory = st.selectbox(
                "厂别",
                options=available_factories,
                key=factory_key,
            )

        available_steps = get_steps_for_factory(indicator_df, selected_factory)
        step_key = "spc_step_filter"
        param_key = "spc_param_filter"
        previous_factory_key = "spc_previous_factory_filter"
        if st.session_state.get(previous_factory_key) != selected_factory:
            st.session_state[step_key] = []
            st.session_state[param_key] = []
            st.session_state[previous_factory_key] = selected_factory

        with c_step:
            st.session_state[step_key] = _normalise_selection(st.session_state.get(step_key, []), available_steps)
            selected_steps = st.multiselect("站点", options=available_steps, key=step_key)

        available_params = get_params_for_factory_steps(indicator_df, selected_factory, selected_steps)
        steps_signature_key = "spc_steps_for_param_autoselect"
        steps_signature = _filter_signature(selected_factory, selected_steps, [])
        if st.session_state.get(steps_signature_key) != steps_signature:
            st.session_state[param_key] = available_params
            st.session_state[steps_signature_key] = steps_signature
        st.session_state[param_key] = _normalise_selection(st.session_state.get(param_key, []), available_params)

        with c_param:
            selected_params = st.multiselect(
                "参数名称",
                options=available_params,
                key=param_key,
                disabled=not selected_steps,
            )

        current_signature = _filter_signature(selected_factory, selected_steps, selected_params)
        applied_signature_key = "spc_applied_filter_signature"
        can_query = bool(selected_factory and selected_steps and selected_params)
        with c_query:
            if st.button("查询", type="primary", width="stretch", disabled=not can_query):
                st.session_state[applied_signature_key] = current_signature

    should_render = bool(can_query and st.session_state.get(applied_signature_key) == current_signature)
    return selected_factory, selected_params, selected_steps, should_render


def filter_spc_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply frontend CPM/CPK filters to any report frame with factory/step/param columns."""
    if report_df.empty:
        return report_df

    df = report_df.copy()
    if selected_factory and "factory" in df.columns:
        df = df[df["factory"].astype(str) == str(selected_factory)]
    if selected_params and "param_name" in df.columns:
        df = df[df["param_name"].astype(str).isin(selected_params)]
    if selected_steps and "step_id" in df.columns:
        df = df[df["step_id"].astype(str).isin(selected_steps)]
    return df.reset_index(drop=True)


def _excel_bytes(sheet_map: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet_name, df in sheet_map.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def render_sheet_oos_decoration_admin(
    decoration_result: SheetOosDecorationResult,
    *,
    show_expander: bool = True,
    report_name: str = "SPC",
    key_prefix: str = "spc",
) -> None:
    """Render the Sheet OOS decorator, optionally inside a parent admin panel."""
    detail_df = decoration_result.detail_df
    decoration_df = decoration_result.decoration_df
    detail_download_df = detail_df if not detail_df.empty else pd.DataFrame(columns=OOS_DETAIL_COLUMNS)
    decoration_download_df = decoration_df if not decoration_df.empty else pd.DataFrame(columns=OOS_DECORATION_COLUMNS)

    container = (
        st.expander(f"开发者后台：{report_name} 超规片数据修饰", expanded=False)
        if show_expander
        else nullcontext()
    )
    with container:
        st.caption(f"明细文件：{decoration_result.detail_path}")
        st.caption(f"修饰文件：{decoration_result.decoration_path}")
        c_detail, c_decoration, c_upload = st.columns([1, 1, 1.2])

        with c_detail:
            st.markdown("#### 下载超规片明细")
            st.download_button(
                label="下载明细表",
                data=_excel_bytes({"超规片明细": detail_download_df}),
                file_name=decoration_result.detail_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_oos_detail_download",
                use_container_width=True,
            )

        with c_decoration:
            st.markdown("#### 下载修饰表")
            st.download_button(
                label="下载修饰表",
                data=_excel_bytes({"修饰表": decoration_download_df}),
                file_name=decoration_result.decoration_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_oos_decoration_download",
                use_container_width=True,
            )

        with c_upload:
            st.markdown("#### 上传修饰表")
            uploaded_file = st.file_uploader(
                "上传包含 flag 字段的 Excel",
                type=["xlsx"],
                key=f"{key_prefix}_oos_decoration_upload",
                label_visibility="collapsed",
            )
            if uploaded_file is not None:
                uploaded_bytes = uploaded_file.getvalue()
                if st.button(
                    "确认覆盖并刷新",
                    type="primary",
                    key=f"{key_prefix}_oos_decoration_upload_btn",
                    use_container_width=True,
                ):
                    try:
                        uploaded_df = pd.read_excel(BytesIO(uploaded_bytes), engine="openpyxl")
                        required_columns = {*OOS_KEY_COLUMNS, "flag"}
                        missing_columns = required_columns - set(uploaded_df.columns)
                        if missing_columns:
                            st.error(f"修饰表缺少必要字段：{', '.join(sorted(missing_columns))}")
                            return

                        decoration_result.decoration_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(decoration_result.decoration_path, "wb") as file:
                            file.write(uploaded_bytes)
                        st.success("修饰表已覆盖，正在刷新缓存。")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"保存修饰表失败：{exc}")


def render_cpk_decoration_admin(
    decoration_result: CpkDecorationResult,
    *,
    show_expander: bool = True,
) -> None:
    """Render the opt-in CPK decoration file controls."""
    detail_download_df = (
        decoration_result.detail_df
        if not decoration_result.detail_df.empty
        else pd.DataFrame(columns=CPK_DETAIL_COLUMNS)
    )
    decoration_download_df = (
        decoration_result.decoration_df
        if not decoration_result.decoration_df.empty
        else pd.DataFrame(columns=CPK_DECORATION_COLUMNS)
    )
    container = st.expander("开发者后台：SPC CPK 修饰", expanded=False) if show_expander else nullcontext()
    with container:
        st.caption("默认 flag=False，CPK 显示真实计算值；启用后显示修饰表中的 cpk_corrected。")
        st.caption(f"明细文件：{decoration_result.detail_path}")
        st.caption(f"修饰文件：{decoration_result.decoration_path}")
        c_detail, c_decoration, c_upload = st.columns([1, 1, 1.2])

        with c_detail:
            st.markdown("#### 下载 CPK 明细")
            st.download_button(
                label="下载明细表",
                data=_excel_bytes({"CPK明细": detail_download_df}),
                file_name=decoration_result.detail_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="spc_cpk_detail_download",
                use_container_width=True,
            )

        with c_decoration:
            st.markdown("#### 下载 CPK 修饰表")
            st.download_button(
                label="下载修饰表",
                data=_excel_bytes({"CPK修饰表": decoration_download_df}),
                file_name=decoration_result.decoration_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="spc_cpk_decoration_download",
                use_container_width=True,
            )

        with c_upload:
            st.markdown("#### 上传 CPK 修饰表")
            uploaded_file = st.file_uploader(
                "上传包含 flag 字段的 Excel",
                type=["xlsx"],
                key="spc_cpk_decoration_upload",
                label_visibility="collapsed",
            )
            if uploaded_file is not None:
                uploaded_bytes = uploaded_file.getvalue()
                if st.button(
                    "确认覆盖并刷新",
                    type="primary",
                    key="spc_cpk_decoration_upload_btn",
                    use_container_width=True,
                ):
                    try:
                        uploaded_df = pd.read_excel(BytesIO(uploaded_bytes), engine="openpyxl")
                        required_columns = {*CPK_KEY_COLUMNS, "cpk_corrected", "flag"}
                        missing_columns = required_columns - set(uploaded_df.columns)
                        if missing_columns:
                            st.error(f"修饰表缺少必要字段：{', '.join(sorted(missing_columns))}")
                            return

                        decoration_result.decoration_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(decoration_result.decoration_path, "wb") as file:
                            file.write(uploaded_bytes)
                        st.success("CPK 修饰表已覆盖，正在刷新缓存。")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"保存 CPK 修饰表失败：{exc}")


def render_spc_decoration_admin(
    sheet_oos_decoration_result: SheetOosDecorationResult | None,
    cpk_decoration_result: CpkDecorationResult | None,
) -> None:
    """Render the one admin expander containing isolated OOS and CPK decorators."""
    with st.expander("开发者后台：SPC 数据修饰", expanded=False):
        oos_tab, cpk_tab = st.tabs(["超规片修饰", "CPK修饰"])
        with oos_tab:
            if sheet_oos_decoration_result is None:
                st.info("当前没有可管理的超规片修饰数据。")
            else:
                render_sheet_oos_decoration_admin(sheet_oos_decoration_result, show_expander=False)
        with cpk_tab:
            if cpk_decoration_result is None:
                st.info("当前没有可管理的 CPK 修饰数据。")
            else:
                render_cpk_decoration_admin(cpk_decoration_result, show_expander=False)


def _display_period_label(period_type: str, period_label: str) -> str:
    return f"{PERIOD_LABELS.get(period_type, period_type)} | {period_label}"


def _empty_period_points_frame(value_column: str = "sheet_mean") -> pd.DataFrame:
    return pd.DataFrame(columns=["period_type", "period_label", "display_label", "period_sort", value_column])


def _add_display_labels(axis_df: pd.DataFrame) -> pd.DataFrame:
    axis_df = axis_df.copy()
    axis_df["display_label"] = [
        _display_period_label(period_type, period_label)
        for period_type, period_label in zip(axis_df["period_type"], axis_df["period_label"])
    ]
    return axis_df


def _period_axis_with_display(end_date: date, sheet_features_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if sheet_features_df is not None and not sheet_features_df.empty:
        axis_df = build_available_period_axis(sheet_features_df, end_date).copy()
    else:
        axis_df = build_period_axis(end_date).copy()
    return _add_display_labels(axis_df)


def _infer_period_axis_end_date(sheet_features_df: pd.DataFrame, period_capability_df: pd.DataFrame) -> date:
    for source_df, column in [
        (period_capability_df, "period_end"),
        (sheet_features_df, "sheet_start_time"),
    ]:
        if source_df.empty or column not in source_df.columns:
            continue
        max_value = pd.to_datetime(source_df[column], errors="coerce").max()
        if pd.notna(max_value):
            return max_value.date()
    return date.today()


def _period_points(
    source_df: pd.DataFrame,
    period_axis_df: pd.DataFrame,
    value_column: str,
) -> pd.DataFrame:
    if source_df.empty or "sheet_start_time" not in source_df.columns or value_column not in source_df.columns:
        return _empty_period_points_frame(value_column)

    df = source_df.copy()
    df["sheet_start_time"] = pd.to_datetime(df["sheet_start_time"], errors="coerce")
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    df = df.dropna(subset=["sheet_start_time", value_column]).copy()
    if df.empty:
        return _empty_period_points_frame(value_column)

    frames: list[pd.DataFrame] = []
    month_df = df.copy()
    month_df["period_type"] = "month"
    month_df["period_label"] = month_df["sheet_start_time"].dt.strftime("%Y-%m")
    frames.append(month_df)

    week_df = df.copy()
    iso_week = week_df["sheet_start_time"].dt.isocalendar()
    week_df["period_type"] = "week"
    week_df["period_label"] = iso_week.year.astype(str) + "-W" + iso_week.week.astype(str).str.zfill(2)
    frames.append(week_df)

    day_df = df.copy()
    day_df["period_type"] = "day"
    day_df["period_label"] = day_df["sheet_start_time"].dt.strftime("%Y-%m-%d")
    frames.append(day_df)

    points_df = pd.concat(frames, ignore_index=True)
    return points_df.merge(
        period_axis_df[["period_type", "period_label", "period_sort", "display_label"]],
        on=["period_type", "period_label"],
        how="inner",
    )


def _sheet_period_points(sheet_features_df: pd.DataFrame, period_axis_df: pd.DataFrame) -> pd.DataFrame:
    return _period_points(sheet_features_df, period_axis_df, "sheet_mean")


def _measurement_period_points(raw_measurements_df: pd.DataFrame, period_axis_df: pd.DataFrame) -> pd.DataFrame:
    return _period_points(raw_measurements_df, period_axis_df, "param_value")


def _resolve_chart_type(*dataframes: pd.DataFrame) -> str:
    """Read the backend-provided chart type, defaulting safely to a box chart."""
    for dataframe in dataframes:
        if dataframe.empty or "chart_type" not in dataframe.columns:
            continue
        chart_types = dataframe["chart_type"].dropna().astype(str).str.lower()
        if not chart_types.empty and chart_types.iloc[0] == CHART_TYPE_LINE:
            return CHART_TYPE_LINE
    return CHART_TYPE_BOX


def _add_spec_line(fig: go.Figure, y_value: object, label: str, color: str, row: int) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=_format_spec_line_label(label, y_value),
        annotation_position="top right",
        row=row,
        col=1,
    )


def _format_spec_value(value: object) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    float_value = float(numeric_value)
    absolute_value = abs(float_value)
    if float_value != 0.0 and (absolute_value < 0.001 or absolute_value >= 1_000_000):
        return f"{float_value:.4g}"
    value_text = f"{float_value:.3f}".rstrip("0").rstrip(".")
    return value_text if value_text else "0"


def _format_spec_line_label(label: str, value: object) -> str:
    return f"{label}: {_format_spec_value(value)}"


def _resolve_target_value(spec_row: pd.Series) -> float | None:
    target = spec_row.get("target")
    if pd.notna(target):
        return float(target)
    usl = spec_row.get("usl")
    lsl = spec_row.get("lsl")
    if pd.notna(usl) and pd.notna(lsl):
        return float((float(usl) + float(lsl)) / 2.0)
    return None


def _resolve_cl_value(spec_row: pd.Series) -> float | None:
    ucl = spec_row.get("ucl")
    lcl = spec_row.get("lcl")
    if pd.notna(ucl) and pd.notna(lcl):
        return float((float(ucl) + float(lcl)) / 2.0)
    return _resolve_target_value(spec_row)


def _add_plain_spec_line(fig: go.Figure, y_value: object, label: str, color: str) -> None:
    if pd.isna(y_value):
        return
    fig.add_hline(
        y=float(y_value),
        line_dash="dash",
        line_color=color,
        line_width=1.4,
        annotation_text=_format_spec_line_label(label, y_value),
        annotation_position="top right",
    )


def _apply_measurement_spec_lines(fig: go.Figure, spec_df: pd.DataFrame, row: int | None = None) -> None:
    if spec_df.empty:
        return
    spec_source = spec_df.dropna(subset=["usl", "lsl"]).head(1)
    if spec_source.empty:
        return
    spec_row = spec_source.iloc[0]
    line_func = (
        (lambda value, label, color: _add_spec_line(fig, value, label, color, row=row))
        if row is not None
        else (lambda value, label, color: _add_plain_spec_line(fig, value, label, color))
    )
    line_func(spec_row.get("usl"), "USL", "#dc2626")
    if float(spec_row.get("lsl")) == 0.0:
        line_func(spec_row.get("ucl"), "UCL", "#16a34a")
        return

    line_func(spec_row.get("lsl"), "LSL", "#dc2626")
    line_func(spec_row.get("ucl"), "UCL", "#16a34a")
    line_func(spec_row.get("lcl"), "LCL", "#16a34a")
    target_value = _resolve_target_value(spec_row)
    if target_value is not None:
        line_func(target_value, "Target", "#f97316")
    cl_value = _resolve_cl_value(spec_row)
    if cl_value is not None:
        line_func(cl_value, "CL", "#16a34a")


def _resolve_measurement_y_range(data_values: object, spec_df: pd.DataFrame) -> list[float] | None:
    if spec_df.empty:
        return None
    spec_source = spec_df.dropna(subset=["usl", "lsl"]).head(1)
    if spec_source.empty:
        return None

    spec_row = spec_source.iloc[0]
    usl = pd.to_numeric(pd.Series([spec_row.get("usl")]), errors="coerce").iloc[0]
    lsl = pd.to_numeric(pd.Series([spec_row.get("lsl")]), errors="coerce").iloc[0]
    if pd.isna(usl) or pd.isna(lsl) or usl <= lsl:
        return None

    values = pd.to_numeric(pd.Series(data_values), errors="coerce").dropna()
    if values.empty:
        return [float(lsl), float(usl)]

    lower = min(float(lsl), float(values.min()))
    upper = max(float(usl), float(values.max()))
    if lower == float(lsl) and upper == float(usl):
        return [float(lsl), float(usl)]

    span = upper - lower
    padding = span * 0.06 if span > 0 else max(abs(upper), 1.0) * 0.06
    return [float(lower - padding), float(upper + padding)]


def _format_metric_value(value: object) -> str:
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return "-"
    return f"{float(numeric_value):.3f}"


def _create_period_capability_table(period_capability_df: pd.DataFrame) -> pd.DataFrame:
    """Return one CPM/CPK row per recent month, week, or day period."""
    period_column = "周期"
    metric_columns = ["CPM", "CPK"]
    required_cols = {"period_type", "period_label"}
    if period_capability_df.empty or not required_cols.issubset(period_capability_df.columns):
        return pd.DataFrame(columns=[period_column, *metric_columns])

    df = period_capability_df.copy()
    if "period_sort" not in df.columns:
        df["period_sort"] = df["period_label"].astype(str)
    for col in ["cpm", "cpk"]:
        if col not in df.columns:
            df[col] = pd.NA

    frames: list[pd.DataFrame] = []
    for period_type in ["month", "week", "day"]:
        type_df = df[df["period_type"] == period_type].copy()
        if type_df.empty:
            continue
        type_df = type_df.sort_values(["period_sort", "period_label"]).drop_duplicates(
            ["period_type", "period_label"],
            keep="last",
        )
        frames.append(type_df.tail(PERIOD_WINDOW_LIMITS[period_type]))

    if not frames:
        return pd.DataFrame(columns=[period_column, *metric_columns])

    selected_df = pd.concat(frames, ignore_index=True).copy()
    records: list[dict[str, str]] = []
    seen_periods: set[str] = set()

    for _, row in selected_df.iterrows():
        period_type = row.get("period_type")
        period_label = str(row.get("period_label", ""))
        period_prefix = PERIOD_LABELS.get(str(period_type), str(period_type))
        period_name = f"{period_prefix} {period_label}".strip()
        if not period_name or period_name in seen_periods:
            continue
        seen_periods.add(period_name)
        records.append(
            {
                period_column: period_name,
                "CPM": _format_metric_value(row.get("cpm")),
                "CPK": _format_metric_value(row.get("cpk")),
            }
        )

    return pd.DataFrame(records, columns=[period_column, *metric_columns])


def _create_period_overview_chart(
    sheet_features_df: pd.DataFrame,
    period_capability_df: pd.DataFrame,
    title: str,
    raw_measurements_df: pd.DataFrame | None = None,
    period_box_source: str = "sheet_mean",
) -> go.Figure:
    """Create Figure1: month/week/day box distributions."""
    fig = go.Figure()

    axis_end_date = _infer_period_axis_end_date(sheet_features_df, period_capability_df)
    period_axis_df = _period_axis_with_display(axis_end_date, sheet_features_df)
    use_point_values = period_box_source == "point_value" and raw_measurements_df is not None
    if use_point_values:
        points_df = _measurement_period_points(raw_measurements_df, period_axis_df)
        value_column = "param_value"
        value_label = "Point Value"
    else:
        points_df = _sheet_period_points(sheet_features_df, period_axis_df)
        value_column = "sheet_mean"
        value_label = "Sheet Mean"
    ordered_labels = period_axis_df["display_label"].tolist()
    for period_type in ["month", "week", "day"]:
        type_points = points_df[points_df["period_type"] == period_type]
        labels = period_axis_df[period_axis_df["period_type"] == period_type]["display_label"].tolist()
        for label in labels:
            y_values = type_points[type_points["display_label"] == label][value_column]
            if y_values.empty:
                continue
            fig.add_trace(
                create_box_distribution_trace(
                    x_values=[label] * len(y_values),
                    y_values=y_values,
                    name=label,
                    color=PERIOD_COLORS.get(period_type, "#2563eb"),
                    fillcolor=PERIOD_FILL_COLORS.get(period_type, "rgba(37, 99, 235, 0.18)"),
                    showlegend=False,
                    width=0.42,
                    hovertemplate=f"{label}<br>{value_label}=%{{y:.4f}}<extra></extra>",
                ),
            )

    spec_source = (
        sheet_features_df.dropna(subset=["usl", "lsl"]).head(1)
        if {"usl", "lsl"}.issubset(sheet_features_df.columns)
        else pd.DataFrame()
    )
    _apply_measurement_spec_lines(fig, spec_source)
    if not spec_source.empty:
        y_range = _resolve_measurement_y_range(points_df[value_column], spec_source)
        if y_range is not None:
            fig.update_yaxes(range=y_range)

    fig.update_layout(
        title=title,
        height=450,
        margin={"l": 32, "r": 24, "t": 58, "b": 82},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        boxmode="group",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        yaxis={"title": value_label},
    )
    fig.update_xaxes(categoryorder="array", categoryarray=ordered_labels, tickangle=-35)
    return fig


def _resolve_chamber_column(df: pd.DataFrame) -> str:
    for column in ["unit_id", "chamber", "chamber_id", "sub_equip_id", "eqp_id", "main_eqp_type", "site_name"]:
        if column in df.columns:
            return column
    return ""


def _derive_chamber_label(value: object) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    value_text = str(value).strip()
    if not value_text:
        return "UNKNOWN"
    if "-" in value_text:
        chamber = value_text.split("-", 1)[0].strip()
        return chamber or "UNKNOWN"
    return value_text[:6] if len(value_text) > 6 else value_text


def _sheet_id_order(df: pd.DataFrame) -> list[str]:
    if "sheet_id" not in df.columns:
        return []
    return df["sheet_id"].dropna().astype(str).drop_duplicates().tolist()


def _create_sheet_points_box_chart(
    raw_measurements_df: pd.DataFrame,
    sort_mode: str,
    title: str,
    spec_df: pd.DataFrame | None = None,
    chart_type: str = CHART_TYPE_BOX,
) -> go.Figure:
    """Create Figure2: point-level boxes or point-line trends by chamber/site or pass time."""
    fig = go.Figure()
    if raw_measurements_df.empty or "param_value" not in raw_measurements_df.columns:
        fig.update_layout(title=title, height=420)
        return fig

    df = raw_measurements_df.copy()
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df["sheet_start_time"] = pd.to_datetime(df.get("sheet_start_time"), errors="coerce")
    df = df.dropna(subset=["param_value"]).copy()
    if df.empty:
        fig.update_layout(title=title, height=420)
        return fig

    if sort_mode == "按过货时间排序":
        sorted_df = df.sort_values(["sheet_start_time", "sheet_id"], na_position="last")
        group_labels = _sheet_id_order(sorted_df)
        if chart_type == CHART_TYPE_LINE:
            trend_points = sorted_df.assign(sheet_id=lambda frame: frame["sheet_id"].astype(str))
            fig.add_trace(
                create_point_line_trace(
                    x_values=trend_points["sheet_id"],
                    y_values=trend_points["param_value"],
                    name="Point Value",
                    color="#1d4ed8",
                    hovertemplate="Sheet=%{x}<br>Param Value=%{y:.4f}<extra></extra>",
                )
            )
        else:
            for sheet_id in group_labels:
                y_values = sorted_df[sorted_df["sheet_id"].astype(str) == sheet_id]["param_value"]
                fig.add_trace(
                    create_box_distribution_trace(
                        y_values=y_values,
                        name=sheet_id,
                        color="#1d4ed8",
                        showlegend=False,
                    )
                )
    else:
        chamber_col = _resolve_chamber_column(df)
        if chamber_col == "unit_id":
            df["chamber_label"] = df[chamber_col].apply(_derive_chamber_label)
        else:
            df["chamber_label"] = df[chamber_col].fillna("UNKNOWN").astype(str) if chamber_col else "UNKNOWN"
        df["chamber_label"] = df["chamber_label"].fillna("UNKNOWN").astype(str)
        sorted_df = df.sort_values(["chamber_label", "sheet_start_time", "sheet_id"], na_position="last")
        sheet_order = _sheet_id_order(sorted_df)
        chamber_order = sorted(sorted_df["chamber_label"].dropna().astype(str).unique().tolist())
        chamber_colors = {
            chamber: SHEET_BOX_PALETTE[index % len(SHEET_BOX_PALETTE)]
            for index, chamber in enumerate(chamber_order)
        }
        if chart_type == CHART_TYPE_LINE:
            for chamber in chamber_order:
                trend_points = sorted_df[
                    sorted_df["chamber_label"] == chamber
                ].assign(
                    sheet_id=lambda frame: frame["sheet_id"].astype(str)
                )
                fig.add_trace(
                    create_point_line_trace(
                        x_values=trend_points["sheet_id"],
                        y_values=trend_points["param_value"],
                        name=chamber,
                        color=chamber_colors.get(chamber, SHEET_BOX_PALETTE[0]),
                        hovertemplate=f"Chamber={chamber}<br>Sheet=%{{x}}<br>Param Value=%{{y:.4f}}<extra></extra>",
                    )
                )
        else:
            shown_chambers: set[str] = set()
            for sheet_id in sheet_order:
                sheet_rows = sorted_df[sorted_df["sheet_id"].astype(str) == sheet_id]
                if sheet_rows.empty:
                    continue
                chamber = str(sheet_rows["chamber_label"].iloc[0])
                y_values = sheet_rows["param_value"]
                color = chamber_colors.get(chamber, SHEET_BOX_PALETTE[0])
                fig.add_trace(
                    create_box_distribution_trace(
                        x_values=[sheet_id] * len(y_values),
                        y_values=y_values,
                        name=chamber,
                        color=color,
                        legendgroup=chamber,
                        showlegend=chamber not in shown_chambers,
                    )
                )
                shown_chambers.add(chamber)

    if spec_df is not None:
        _apply_measurement_spec_lines(fig, spec_df)
        y_range = _resolve_measurement_y_range(df["param_value"], spec_df)
        if y_range is not None:
            fig.update_yaxes(range=y_range)

    fig.update_layout(
        title=title,
        height=430,
        margin={"l": 32, "r": 24, "t": 56, "b": 80},
        xaxis_title=None,
        yaxis_title="Param Value",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )
    fig.update_xaxes(tickangle=-45)
    return fig


def _create_sheet_points_box_charts(
    raw_measurements_df: pd.DataFrame,
    title_prefix: str,
    spec_df: pd.DataFrame | None = None,
    chart_type: str = CHART_TYPE_BOX,
) -> tuple[go.Figure, go.Figure]:
    chamber_fig = _create_sheet_points_box_chart(
        raw_measurements_df=raw_measurements_df,
        sort_mode="按腔室排序",
        title=f"{title_prefix} | Sheet点位分布 By腔室",
        spec_df=spec_df,
        chart_type=chart_type,
    )
    time_fig = _create_sheet_points_box_chart(
        raw_measurements_df=raw_measurements_df,
        sort_mode="按过货时间排序",
        title=f"{title_prefix} | Sheet点位分布 By过货时间",
        spec_df=spec_df,
        chart_type=chart_type,
    )
    return chamber_fig, time_fig


def _build_indicator_render_payload(
    label: str,
    chart_type: str,
    indicator_features_df: pd.DataFrame,
    indicator_capability_df: pd.DataFrame,
    indicator_raw_df: pd.DataFrame,
    period_box_source: str,
) -> dict[str, object]:
    """[RenderGate 阶段1] 纯计算：构建单个指标的全部图表与表格，禁止触碰 st.*。"""
    cpk_values = (
        pd.to_numeric(indicator_capability_df["cpk"], errors="coerce").dropna()
        if "cpk" in indicator_capability_df.columns
        else pd.Series(dtype="float64")
    )
    cpm_values = (
        pd.to_numeric(indicator_capability_df["cpm"], errors="coerce").dropna()
        if "cpm" in indicator_capability_df.columns
        else pd.Series(dtype="float64")
    )
    fig1 = _create_period_overview_chart(
        sheet_features_df=indicator_features_df,
        period_capability_df=indicator_capability_df,
        raw_measurements_df=indicator_raw_df,
        period_box_source=period_box_source,
        title=f"{label} | 月周天分布",
    )
    chamber_fig, time_fig = _create_sheet_points_box_charts(
        raw_measurements_df=indicator_raw_df,
        title_prefix=label,
        spec_df=indicator_features_df,
        chart_type=chart_type,
    )
    return {
        "label": label,
        "cpk_median": _format_metric_value(cpk_values.median() if not cpk_values.empty else pd.NA),
        "cpk_min": _format_metric_value(cpk_values.min() if not cpk_values.empty else pd.NA),
        "cpm_median": _format_metric_value(cpm_values.median() if not cpm_values.empty else pd.NA),
        "cpm_min": _format_metric_value(cpm_values.min() if not cpm_values.empty else pd.NA),
        "capability_table": _create_period_capability_table(indicator_capability_df),
        "fig1": fig1,
        "chamber_fig": chamber_fig,
        "time_fig": time_fig,
    }


def _render_indicator_payload(payload: dict[str, object]) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用，不做任何重计算。"""
    with st.expander(payload["label"], expanded=True):
        metric_cols = st.columns(4)
        metric_cols[0].metric("中位CPK", payload["cpk_median"])
        metric_cols[1].metric("最小CPK", payload["cpk_min"])
        metric_cols[2].metric("中位CPM", payload["cpm_median"])
        metric_cols[3].metric("最小CPM", payload["cpm_min"])

        period_col, capability_col = st.columns([1.15, 1], gap="large")
        with period_col:
            st.plotly_chart(payload["fig1"], width="stretch")
        with capability_col:
            capability_table = payload["capability_table"]
            if not capability_table.empty:
                st.dataframe(
                    capability_table,
                    hide_index=True,
                    use_container_width=True,
                    height=min(420, 38 + 35 * len(capability_table)),
                )

        st.plotly_chart(payload["chamber_fig"], width="stretch")
        st.plotly_chart(payload["time_fig"], width="stretch")


def render_spc_indicator_sections(
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
) -> None:
    """Render one expander per monitoring indicator with Task2 distribution figures.

    两阶段渲染：先在 RenderGate 统一 spinner 下构建全部图表，再集中回流渲染，
    避免图表随计算进度一张一张跳出导致页面抖动卡顿。
    """
    if sheet_features_df.empty:
        st.info("当前筛选条件下无 CPM/CPK 数据。")
        return

    gate = RenderGate()
    grouped = sheet_features_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_features_df in grouped:
        label = f"{factory} | {step_id} | {param_name}"
        if {"factory", "step_id", "param_name"}.issubset(period_capability_df.columns):
            indicator_capability_df = period_capability_df[
                (period_capability_df["factory"].astype(str) == str(factory))
                & (period_capability_df["step_id"].astype(str) == str(step_id))
                & (period_capability_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_capability_df = pd.DataFrame()
        if {"factory", "step_id", "param_name"}.issubset(raw_measurements_df.columns):
            indicator_raw_df = raw_measurements_df[
                (raw_measurements_df["factory"].astype(str) == str(factory))
                & (raw_measurements_df["step_id"].astype(str) == str(step_id))
                & (raw_measurements_df["param_name"].astype(str) == str(param_name))
            ].copy()
        else:
            indicator_raw_df = pd.DataFrame()
        chart_type = _resolve_chart_type(
            indicator_capability_df,
            indicator_features_df,
            indicator_raw_df,
        )
        gate.stage(
            partial(
                _build_indicator_render_payload,
                label=label,
                chart_type=chart_type,
                indicator_features_df=indicator_features_df,
                indicator_capability_df=indicator_capability_df,
                indicator_raw_df=indicator_raw_df,
                period_box_source=period_box_source,
            )
        )

    for payload in gate.collect():
        _render_indicator_payload(payload)


def render_cpk_alert_indicator_sections(
    alerts_df: pd.DataFrame,
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
) -> None:
    """Render every alerted indicator directly, without requiring filter interaction."""
    if alerts_df.empty:
        return

    alert_capability_df = filter_spc_report_by_alerts(period_capability_df, alerts_df)
    alert_sheet_features_df = filter_spc_report_by_alerts(sheet_features_df, alerts_df)
    alert_raw_measurements_df = filter_spc_report_by_alerts(raw_measurements_df, alerts_df)
    if alert_sheet_features_df.empty:
        st.warning("预警指标暂无可绘制的 Sheet 数据。")
        return

    indicator_count = (
        alert_sheet_features_df.groupby(["factory", "step_id", "param_name"]).ngroups
        if {"factory", "step_id", "param_name"}.issubset(alert_sheet_features_df.columns)
        else 0
    )
    with st.expander(f"🚨 自动预警指标图像（{indicator_count} 个指标）", expanded=False):
        st.caption("以下图像由 CPK 预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        render_spc_indicator_sections(
            period_capability_df=alert_capability_df,
            sheet_features_df=alert_sheet_features_df,
            raw_measurements_df=alert_raw_measurements_df,
            period_box_source=period_box_source,
        )
