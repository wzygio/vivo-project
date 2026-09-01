from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from functools import partial
from io import BytesIO
import hashlib

import pandas as pd
import streamlit as st

from app.components.page_header import build_product_cache_signature
from app.manager.render_gate import RenderGate
from app.charts.inline_domain import (
    PERIOD_LABELS,
    PERIOD_WINDOW_LIMITS,
    create_period_overview_chart,
    create_sheet_points_box_chart,
    create_sheet_points_box_charts,
    resolve_chart_type,
)
from app.sections.inline_domain.shared import (
    INLINE_FACTORY_OPTIONS,
    apply_report_filter,
    excel_bytes,
    get_available_factories as _shared_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory as _shared_steps_for_factory,
    render_cascade_filters,
    render_sheet_oos_decoration_admin,
)
from app.sections.inline_domain.shared.alert_center import (
    build_sheet_oos_alert_display,
    filter_report_by_alert_keys,
)
from app.utils.step_labels import format_step_label
from src.inline_domain.core.spc.spc_calculator import get_period_window_start
from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts
from src.inline_domain.core.shared.sheet_oos_decoration import (
    SheetOosDecorationResult,
)
from src.inline_domain.core.spc.cpk_decoration import (
    CPK_DECORATION_COLUMNS,
    CPK_KEY_COLUMNS,
    CpkDecorationResult,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.utils.excel_tools import replace_workbook_sheet

SPC_FACTORY_OPTIONS = INLINE_FACTORY_OPTIONS
CPK_ALERT_THRESHOLD = 1.33
CPK_ALERT_COLUMNS = ["厂别", "站点", "参数名称", "超规周次", "CPK值"]
CPM_ALERT_THRESHOLD = 1.33
CPM_ALERT_COLUMNS = ["厂别", "站点", "参数名称", "超规周次", "CPM值"]
CPK_ALERT_KEY_COLUMN_MAP = {
    "厂别": "factory",
    "站点": "step_id",
    "参数名称": "param_name",
}
SPC_OOS_ALERT_COLUMN_MAP = {
    "factory": "厂别",
    "step_id": "站点",
    "param_name": "参数名称",
    "sheet_id": "Sheet ID",
    "sheet_start_time": "超规时间",
    "oos_type": "超规类型",
}
SPC_OOS_ALERT_COLUMNS = ["厂别", "站点", "参数名称", "Sheet ID", "超规时间", "超规类型"]

# 公共管线已下沉至 app.sections.inline_domain.shared；以下别名为兼容既有调用/测试保留。
_excel_bytes = excel_bytes
_resolve_chart_type = resolve_chart_type
_create_period_overview_chart = create_period_overview_chart
_create_sheet_points_box_chart = create_sheet_points_box_chart
_create_sheet_points_box_charts = create_sheet_points_box_charts


def get_default_spc_start_date(end_date: date) -> date:
    """Return the first day needed by the CPM/CPK Task2 report."""
    return get_period_window_start(end_date)


def _build_weekly_capability_alerts(
    period_capability_df: pd.DataFrame,
    *,
    metric_column: str,
    value_label: str,
    alert_columns: list[str],
    threshold: float,
    decorated_column: str | None = None,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """Return below-threshold weekly capability records from the previous full week."""
    required_columns = {"factory", "step_id", "param_name", "period_type", "period_label", metric_column}
    if period_capability_df.empty or not required_columns.issubset(period_capability_df.columns):
        return pd.DataFrame(columns=alert_columns)

    reference_day = pd.Timestamp(reference_date or date.today()).normalize()
    current_week_start = reference_day - pd.Timedelta(days=reference_day.weekday())
    previous_week_start = current_week_start - pd.Timedelta(days=7)
    iso_week = previous_week_start.isocalendar()
    target_week_label = f"{iso_week.year}-W{iso_week.week:02d}"

    capability_df = period_capability_df[
        period_capability_df["period_type"].astype(str).eq("week")
    ].copy()
    if capability_df.empty:
        return pd.DataFrame(columns=alert_columns)

    capability_df[metric_column] = pd.to_numeric(capability_df[metric_column], errors="coerce")
    below_threshold = capability_df[metric_column].lt(threshold)
    if decorated_column is not None:
        is_decorated = (
            capability_df[decorated_column].fillna(False).astype(bool)
            if decorated_column in capability_df.columns
            else pd.Series(False, index=capability_df.index, dtype=bool)
        )
        below_threshold &= ~is_decorated
    alert_rows = capability_df[
        below_threshold
        & capability_df["period_label"].astype(str).eq(target_week_label)
    ].copy()
    if alert_rows.empty:
        return pd.DataFrame(columns=alert_columns)

    alerts_df = alert_rows.rename(
        columns={
            "factory": "厂别",
            "step_id": "站点",
            "param_name": "参数名称",
            "period_label": "超规周次",
            metric_column: value_label,
        }
    )[alert_columns]
    alerts_df["超规周次"] = alerts_df["超规周次"].astype(str)
    return alerts_df.sort_values(
        ["厂别", "站点", "参数名称", value_label],
        ascending=[True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def build_weekly_cpk_alerts(
    period_capability_df: pd.DataFrame,
    threshold: float = CPK_ALERT_THRESHOLD,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """Return below-threshold, undecorated CPK records from the previous full week."""
    return _build_weekly_capability_alerts(
        period_capability_df,
        metric_column="cpk",
        value_label="CPK值",
        alert_columns=CPK_ALERT_COLUMNS,
        threshold=threshold,
        decorated_column="cpk_decorated",
        reference_date=reference_date,
    )


def build_weekly_cpm_alerts(
    period_capability_df: pd.DataFrame,
    threshold: float = CPM_ALERT_THRESHOLD,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """Return below-threshold CPM records from the previous full week."""
    return _build_weekly_capability_alerts(
        period_capability_df,
        metric_column="cpm",
        value_label="CPM值",
        alert_columns=CPM_ALERT_COLUMNS,
        threshold=threshold,
        reference_date=reference_date,
    )


def _render_capability_alert_section(
    alerts_df: pd.DataFrame,
    *,
    metric_label: str,
    value_label: str,
    has_capability_data: bool,
    threshold: float,
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    step_desc_map: dict[str, str] | None = None,
    memo_state_key: str,
    signature_base: str,
    chart_key_prefix: str,
) -> None:
    """Render one expander per metric: 预警表在上，对应的自动预警指标图像在下。

    父 Expander 无论是否有超规都默认展开；每个指标的图像仍保留独立的子折叠面板。
    """
    has_alerts = not alerts_df.empty
    with st.expander(
        f"{metric_label}预警中心（{metric_label} < {threshold:.2f}）",
        expanded=True,
    ):
        if has_alerts:
            st.error(f"检测到 {len(alerts_df)} 条 {metric_label} 预警，请关注。")
            display_alerts_df = alerts_df
            if step_desc_map:
                display_alerts_df = alerts_df.copy()
                display_alerts_df["站点"] = display_alerts_df["站点"].map(
                    lambda step: format_step_label(step, step_desc_map)
                )
            st.dataframe(
                display_alerts_df,
                column_config={value_label: st.column_config.NumberColumn(value_label, format="%.3f")},
                hide_index=True,
                use_container_width=True,
            )
        elif has_capability_data:
            st.success(f"未发现低于 {threshold:.2f} 的 {metric_label}。")
        else:
            st.info(f"当前产品暂无可计算的 {metric_label} 数据。")

        if not has_alerts:
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
        st.markdown(f"**🚨 {metric_label}自动预警指标图像（{indicator_count} 个指标）**")
        st.caption(
            f"以下图像由 {metric_label} 预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。"
        )
        render_spc_indicator_sections(
            period_capability_df=alert_capability_df,
            sheet_features_df=alert_sheet_features_df,
            raw_measurements_df=alert_raw_measurements_df,
            period_box_source=period_box_source,
            memo_signature=_alert_charts_signature(
                alerts_df, alert_sheet_features_df, base=signature_base
            ),
            memo_state_key=memo_state_key,
            chart_key_prefix=chart_key_prefix,
            step_desc_map=step_desc_map,
        )


def render_cpk_alert_section(
    alerts_df: pd.DataFrame,
    *,
    has_capability_data: bool,
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    threshold: float = CPK_ALERT_THRESHOLD,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render the CPK alert center: 预警表 + CPK 超规指标的自动预警图像。"""
    _render_capability_alert_section(
        alerts_df,
        metric_label="CPK",
        value_label="CPK值",
        has_capability_data=has_capability_data,
        threshold=threshold,
        period_capability_df=period_capability_df,
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
        period_box_source=period_box_source,
        step_desc_map=step_desc_map,
        memo_state_key="spc_alert_charts_memo",
        signature_base="spc_alert_charts",
        chart_key_prefix="spc_alert",
    )


def render_cpm_alert_section(
    alerts_df: pd.DataFrame,
    *,
    has_capability_data: bool,
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    threshold: float = CPM_ALERT_THRESHOLD,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render the CPM alert center: 预警表 + CPM 超规指标的自动预警图像。"""
    _render_capability_alert_section(
        alerts_df,
        metric_label="CPM",
        value_label="CPM值",
        has_capability_data=has_capability_data,
        threshold=threshold,
        period_capability_df=period_capability_df,
        sheet_features_df=sheet_features_df,
        raw_measurements_df=raw_measurements_df,
        period_box_source=period_box_source,
        step_desc_map=step_desc_map,
        memo_state_key="spc_cpm_alert_charts_memo",
        signature_base="spc_cpm_alert_charts",
        chart_key_prefix="spc_cpm_alert",
    )


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


def build_spc_sheet_oos_alerts(
    sheet_oos_decoration_result: SheetOosDecorationResult | None,
    reference_date: date | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return last-ISO-week Sheet OOS alerts (flag=FALSE) as a Chinese display table."""
    if sheet_oos_decoration_result is None:
        return pd.DataFrame(columns=SPC_OOS_ALERT_COLUMNS)

    alerts_df = build_sheet_oos_alerts(
        sheet_oos_decoration_result.decoration_df,
        time_column="sheet_start_time",
        reference_date=reference_date,
    )
    display_df = build_sheet_oos_alert_display(
        alerts_df,
        column_map=SPC_OOS_ALERT_COLUMN_MAP,
        output_columns=SPC_OOS_ALERT_COLUMNS,
    )
    if not display_df.empty and "超规时间" in display_df.columns:
        display_df["超规时间"] = display_df["超规时间"].astype(str)
    return display_df


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    """Return available factories with known factory order preserved."""
    return _shared_available_factories(report_df, SPC_FACTORY_OPTIONS)


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    """Return stations available under the selected factory."""
    return _shared_steps_for_factory(report_df, selected_factory)


def get_params_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    """Return parameters available under the selected factory and stations."""
    return get_options_for_factory_steps(report_df, selected_factory, selected_steps, "param_name")


def render_spc_filters(
    indicator_df: pd.DataFrame,
    *,
    step_desc_map: dict[str, str] | None = None,
) -> tuple[str, list[str], list[str], bool]:
    """Render CPM/CPK filters and return selected factory, params, steps, and query state."""
    return render_cascade_filters(
        indicator_df,
        key_prefix="spc",
        third_label="参数名称",
        third_column="param_name",
        third_kind="param",
        factory_options=SPC_FACTORY_OPTIONS,
        step_desc_map=step_desc_map,
    )


def filter_spc_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_params: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """Apply frontend CPM/CPK filters to any report frame with factory/step/param columns."""
    return apply_report_filter(
        report_df,
        selected_factory,
        selected_params,
        selected_steps,
        third_column="param_name",
    )


def render_cpk_decoration_admin(
    decoration_result: CpkDecorationResult,
    *,
    show_expander: bool = True,
) -> None:
    """Render the opt-in CPK decoration file controls."""
    decoration_download_df = (
        decoration_result.decoration_df
        if not decoration_result.decoration_df.empty
        else pd.DataFrame(columns=CPK_DECORATION_COLUMNS)
    )
    container = st.expander("开发者后台：SPC CPK 修饰", expanded=False) if show_expander else nullcontext()
    with container:
        st.caption("默认 flag=False，CPK 显示基于修饰后点位的计算值；启用后显示修饰表中的 cpk_corrected。")
        st.caption(f"修饰文件：{decoration_result.decoration_path}")
        c_decoration, c_upload = st.columns([1, 1.2])

        with c_decoration:
            st.markdown("#### 下载 CPK 修饰表")
            st.download_button(
                label="下载修饰表",
                data=_excel_bytes({"CPK修饰表": decoration_download_df}),
                file_name=f"{decoration_result.decoration_sheet}_{decoration_result.decoration_path.name}",
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
                if st.button(
                    "确认覆盖并刷新",
                    type="primary",
                    key="spc_cpk_decoration_upload_btn",
                    use_container_width=True,
                ):
                    try:
                        uploaded_df = pd.read_excel(BytesIO(uploaded_file.getbuffer()))
                        required_columns = {*CPK_KEY_COLUMNS, "cpk_corrected", "flag"}
                        missing_columns = required_columns - set(uploaded_df.columns)
                        if missing_columns:
                            st.error(f"修饰表缺少必要字段：{', '.join(sorted(missing_columns))}")
                            return

                        replace_workbook_sheet(
                            decoration_result.decoration_path,
                            decoration_result.decoration_sheet,
                            uploaded_df,
                        )
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


def _build_indicator_chart_key(chart_key_prefix: str, label: object, chart_slot: str) -> str:
    """Return a stable key that is unique to a page section, indicator, and chart."""
    indicator_digest = hashlib.sha256(str(label).encode("utf-8")).hexdigest()[:16]
    return f"{chart_key_prefix}_{indicator_digest}_{chart_slot}"


def _render_indicator_payload(
    payload: dict[str, object],
    chart_key_prefix: str = "spc_report",
) -> None:
    """[RenderGate 阶段2] 集中渲染：仅执行 st.* 调用，不做任何重计算。"""
    with st.expander(payload["label"], expanded=True):
        metric_cols = st.columns(4)
        metric_cols[0].metric("中位CPK", payload["cpk_median"])
        metric_cols[1].metric("最小CPK", payload["cpk_min"])
        metric_cols[2].metric("中位CPM", payload["cpm_median"])
        metric_cols[3].metric("最小CPM", payload["cpm_min"])

        period_col, capability_col = st.columns([1.15, 1], gap="large")
        with period_col:
            st.plotly_chart(
                payload["fig1"],
                width="stretch",
                key=_build_indicator_chart_key(chart_key_prefix, payload["label"], "period"),
            )
        with capability_col:
            capability_table = payload["capability_table"]
            if not capability_table.empty:
                st.dataframe(
                    capability_table,
                    hide_index=True,
                    width="stretch",
                    height=min(420, 38 + 35 * len(capability_table)),
                )

        st.plotly_chart(
            payload["chamber_fig"],
            width="stretch",
            key=_build_indicator_chart_key(chart_key_prefix, payload["label"], "chamber"),
        )
        st.plotly_chart(
            payload["time_fig"],
            width="stretch",
            key=_build_indicator_chart_key(chart_key_prefix, payload["label"], "time"),
        )


def render_spc_indicator_sections(
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    memo_signature: str | None = None,
    memo_state_key: str = "spc_alert_charts_memo",
    chart_key_prefix: str = "spc_report",
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render one expander per monitoring indicator with Task2 distribution figures.

    两阶段渲染：先在 RenderGate 统一 spinner 下构建全部图表，再集中回流渲染，
    避免图表随计算进度一张一张跳出导致页面抖动卡顿。

    传入 memo_signature 时（自动预警区），构建结果按签名缓存在 session_state：
    同一版数据重复 rerun 只渲染不重建；签名含产品缓存 revision，
    点"刷新缓存"后签名必变、必重建。
    """
    if sheet_features_df.empty:
        st.info("当前筛选条件下无 CPM/CPK 数据。")
        return

    gate = RenderGate()
    line_param_name_contains = ConfigLoader.get_spc_line_chart_param_name_contains()
    grouped = sheet_features_df.groupby(["factory", "step_id", "param_name"], sort=True)
    for (factory, step_id, param_name), indicator_features_df in grouped:
        label = f"{factory} | {format_step_label(step_id, step_desc_map)} | {param_name}"
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
        chart_type = _resolve_chart_type(param_name, line_param_name_contains)
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

    memo_signature_with_chart_config = (
        None
        if memo_signature is None
        else (
            f"{memo_signature}|chart-config="
            f"{hashlib.sha256('|'.join(line_param_name_contains).encode('utf-8')).hexdigest()[:16]}"
        )
    )
    payloads = (
        gate.collect()
        if memo_signature_with_chart_config is None
        else gate.collect_memoized(memo_state_key, memo_signature_with_chart_config)
    )
    for payload in payloads:
        _render_indicator_payload(payload, chart_key_prefix=chart_key_prefix)


def _alert_charts_signature(
    alerts_df: pd.DataFrame,
    alert_sheet_features_df: pd.DataFrame,
    base: str = "spc_alert_charts",
) -> str:
    """自动预警图表的构建签名：产品缓存 revision + 预警内容指纹。

    点"刷新缓存"会 bump 产品 revision，签名必变、图表必重建；
    同一版数据重复 rerun 时签名稳定，命中 memo 直接复用构建结果。
    """
    product_code = ""
    if "prod_code" in alert_sheet_features_df.columns and not alert_sheet_features_df.empty:
        product_code = str(alert_sheet_features_df["prod_code"].iloc[0])
    if product_code:
        base = build_product_cache_signature(base, product_code)
    else:
        base = f"{base}|product=unknown"
    fingerprint = hashlib.sha256(
        f"{len(alerts_df)}|{alerts_df.astype(str).to_csv(index=False)}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{base}|alerts={fingerprint}"


def render_sheet_oos_alert_indicator_sections(
    alerts_df: pd.DataFrame,
    period_capability_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    raw_measurements_df: pd.DataFrame,
    period_box_source: str = "point_value",
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render every Sheet-OOS-alerted indicator directly, without requiring filter interaction."""
    if alerts_df.empty:
        return

    alert_capability_df = filter_report_by_alert_keys(
        period_capability_df, alerts_df, CPK_ALERT_KEY_COLUMN_MAP
    )
    alert_sheet_features_df = filter_report_by_alert_keys(
        sheet_features_df, alerts_df, CPK_ALERT_KEY_COLUMN_MAP
    )
    alert_raw_measurements_df = filter_report_by_alert_keys(
        raw_measurements_df, alerts_df, CPK_ALERT_KEY_COLUMN_MAP
    )
    if alert_sheet_features_df.empty:
        st.warning("预警指标暂无可绘制的 Sheet 数据。")
        return

    indicator_count = (
        alert_sheet_features_df.groupby(["factory", "step_id", "param_name"]).ngroups
        if {"factory", "step_id", "param_name"}.issubset(alert_sheet_features_df.columns)
        else 0
    )
    with st.expander(f"🚨 单片异常预警指标图像（{indicator_count} 个指标）", expanded=False):
        st.caption("以下图像由单片异常预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        render_spc_indicator_sections(
            period_capability_df=alert_capability_df,
            sheet_features_df=alert_sheet_features_df,
            raw_measurements_df=alert_raw_measurements_df,
            period_box_source=period_box_source,
            memo_signature=_alert_charts_signature(
                alerts_df, alert_sheet_features_df, base="spc_oos_alert_charts"
            ),
            memo_state_key="spc_oos_alert_charts_memo",
            chart_key_prefix="spc_oos_alert",
            step_desc_map=step_desc_map,
        )
