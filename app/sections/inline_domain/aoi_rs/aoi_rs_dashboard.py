"""AOI_RS 报表 Dashboard：筛选（厂别/站点/Code名称）与三张图渲染。

图表口径：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：
   值 = Σcode_qty ÷ 同 period 同站点过货 distinct sheet/glass 数，按 RS Code 分线，
   规格线取 type_flag=MWD_RATIO。
2. By Lot 别点线图：每 lot Σcode_qty，规格线 LOT_RATIO。
3. By Sheet 别点线图：每 sheet/glass Σcode_qty，规格线 SHEET_ID/GLASS_ID。

公共筛选与绘图管线位于 ``app.sections.inline_domain.shared``，本模块只保留
RS 业务差异：code 显示名（rs_code + code_desc）与按图表口径的单值规格线。
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.charts.inline_domain import (
    AoiSpecLine,
    CODE_PALETTE,
    code_color_map,
    create_aoi_period_trend_chart,
    create_aoi_point_chart,
)
from app.sections.inline_domain.shared import (
    INLINE_FACTORY_OPTIONS,
    apply_report_filter,
    get_available_factories as _shared_available_factories,
    get_options_for_factory_steps,
    get_steps_for_factory as _shared_steps_for_factory,
    render_cascade_filters,
)
from app.sections.inline_domain.shared.alert_center import (
    build_sheet_oos_alert_display,
    filter_report_by_alert_keys,
)
from app.utils.step_labels import format_step_label
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    attach_spec_values,
    build_period_throughput_df,
    build_period_trend_df,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DECORATION_FILE_NAME,
    AOI_RS_OOS_KEY_COLUMNS,
)
from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts
from src.inline_domain.core.shared.sheet_oos_decoration import load_sheet_oos_decoration
from src.inline_domain.core.spc.spc_calculator import get_period_window_start

logger = logging.getLogger(__name__)

AOI_RS_FACTORY_OPTIONS = INLINE_FACTORY_OPTIONS

# 单片异常预警（PRD-2026-08-25 §4）：中文展示列与过滤键映射
AOI_RS_ALERT_COLUMNS = ["厂别", "站点", "RS Code", "图类型", "点位ID", "超规时间", "实测值", "规格上限"]
AOI_RS_ALERT_COLUMN_MAP = {
    "factory": "厂别",
    "step_id": "站点",
    "rs_code": "RS Code",
    "chart_kind": "图类型",
    "point_id": "点位ID",
    "sheet_start_time": "超规时间",
    "value": "实测值",
    "spec": "规格上限",
}
AOI_RS_ALERT_KEY_MAP = {"厂别": "factory", "站点": "step_id", "RS Code": "rs_code"}
# 过货帧无 rs_code，仅按 厂别+站点 过滤
AOI_RS_ALERT_PASS_THROUGH_KEY_MAP = {"厂别": "factory", "站点": "step_id"}
AOI_RS_ALERT_CHART_KIND_LABELS = {"lot": "By Lot", "sheet": "By Sheet"}


def get_default_aoi_rs_start_date(end_date: date) -> date:
    """固定窗口起点：上一自然月 1 日。"""
    return get_period_window_start(end_date)


# ---------------------------------------------------------------------------
# 筛选
# ---------------------------------------------------------------------------


def get_available_factories(report_df: pd.DataFrame) -> list[str]:
    return _shared_available_factories(report_df, AOI_RS_FACTORY_OPTIONS)


def get_steps_for_factory(report_df: pd.DataFrame, selected_factory: str) -> list[str]:
    return _shared_steps_for_factory(report_df, selected_factory)


def get_codes_for_factory_steps(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_steps: list[str],
) -> list[str]:
    return get_options_for_factory_steps(report_df, selected_factory, selected_steps, "rs_code")


def render_aoi_rs_filters(
    indicator_df: pd.DataFrame,
    *,
    step_desc_map: dict[str, str] | None = None,
) -> tuple[str, list[str], list[str], bool]:
    """渲染厂别/站点/Code名称筛选与查询门控，返回 (factory, codes, steps, should_render)。"""
    return render_cascade_filters(
        indicator_df,
        key_prefix="aoi_rs",
        third_label="Code名称",
        third_column="rs_code",
        third_kind="code",
        factory_options=AOI_RS_FACTORY_OPTIONS,
        step_desc_map=step_desc_map,
    )


def filter_aoi_rs_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_codes: list[str],
    selected_steps: list[str],
) -> pd.DataFrame:
    """对含 factory/rs_code/step_id 列的数据框应用前端筛选。"""
    return apply_report_filter(
        report_df,
        selected_factory,
        selected_codes,
        selected_steps,
        third_column="rs_code",
    )


# ---------------------------------------------------------------------------
# 图表
# ---------------------------------------------------------------------------


def _code_color_map(codes: list[str]) -> dict[str, str]:
    return code_color_map(codes)


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
    return create_aoi_period_trend_chart(
        trend_df=trend_df,
        throughput_df=throughput_df,
        spec_lines=[AoiSpecLine(spec_value, "规格", CODE_PALETTE[0])],
        code_name=code_name,
        title=title,
        line_value_label="RS/片",
        bar_unit_name="过货量",
        y_title="平均每片 RS 个数",
    )


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
    colors = (
        code_color_map(sorted(point_df["rs_code"].astype(str).unique().tolist()))
        if not point_df.empty and "rs_code" in point_df.columns
        else {}
    )
    spec_lines = {
        code: [AoiSpecLine(value, "规格", colors.get(code, CODE_PALETTE[0]))]
        for code, value in code_specs.items()
    }
    return create_aoi_point_chart(
        point_df=point_df,
        id_col=id_col,
        code_column="rs_code",
        code_specs=spec_lines,
        title=title,
        y_title=y_title,
        y_col=y_col,
        code_names=code_names,
    )


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


def _alert_chart_key(chart_key_prefix: str, factory: str, step_id: str, code: str, slot: str) -> str:
    """图表 key：按（厂别/站点/Code）摘要 + 槽位生成，预警区用独立前缀与主筛选区隔离。"""
    digest = hashlib.sha256(f"{factory}|{step_id}|{code}".encode("utf-8")).hexdigest()[:16]
    return f"{chart_key_prefix}_{digest}_{slot}"


def render_aoi_rs_indicator_sections(
    *,
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    end_date: date,
    chart_key_prefix: str = "aoi_rs_report",
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """按（厂别+站点）分组，组内每个 Code 一个默认展开的 Expander，并列渲染三张图。

    lot/sheet 点帧由 service 层完成超规修饰后传入，本层仅做筛选与渲染（D4）。
    chart_key_prefix 用于区分主筛选区与单片异常预警区，避免 plotly key 冲突。
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
        mwd_specs = _code_spec_map(group_indicators, spec_df, "mwd")
        lot_specs = _code_spec_map(group_indicators, spec_df, "lot")
        sheet_specs = _code_spec_map(group_indicators, spec_df, "sheet")

        for indicator in group_indicators.itertuples(index=False):
            code = str(indicator.rs_code)
            code_name = code_names.get(code, code)
            with st.expander(f"{code_name} | 站点 {format_step_label(step_id, step_desc_map)}", expanded=True):
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
                        key=_alert_chart_key(chart_key_prefix, factory, step_id, code, "trend"),
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
                        key=_alert_chart_key(chart_key_prefix, factory, step_id, code, "lot"),
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
                        key=_alert_chart_key(chart_key_prefix, factory, step_id, code, "sheet"),
                    )


# ---------------------------------------------------------------------------
# 单片异常预警（Sheet OOS，PRD-2026-08-25 §4）
# ---------------------------------------------------------------------------


def _load_aoi_rs_sheet_oos_decoration(product_dir: Path, prod_code: str) -> pd.DataFrame | None:
    """只读加载 AOI_RS 超规修饰明细工作簿；读取失败（含企业加密 COM 回退失败）降级为 None。

    预警是纯只读消费，绝不触发工作簿写入；返回 None 表示"预警数据不可用"，
    页面据此展示降级提示，不阻断报表主体。
    """
    try:
        return load_sheet_oos_decoration(
            product_dir,
            AOI_RS_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_RS_OOS_KEY_COLUMNS,
        )
    except Exception as exc:  # SheetOosDecorationReadError 及 COM 侧其他异常
        logger.warning(
            "[AOI_RS] Sheet OOS 预警明细加载失败，单片异常预警降级为空：%s (%s)",
            product_dir / AOI_RS_OOS_DECORATION_FILE_NAME,
            exc,
        )
        return None


@st.cache_data(show_spinner=False)
def load_cached_aoi_rs_sheet_oos_decoration(
    file_mtime_ns: int,
    file_size: int,
    prod_code: str,
    product_dir_str: str,
) -> pd.DataFrame | None:
    """按（文件 mtime_ns, size, 产品）缓存的工作簿只读加载，普通 rerun 不重复启动 COM。"""
    del file_mtime_ns, file_size  # 仅作为缓存键参与
    return _load_aoi_rs_sheet_oos_decoration(Path(product_dir_str), prod_code)


def build_aoi_rs_sheet_oos_alerts(
    decoration_df: pd.DataFrame | None,
    *,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """筛选上一 ISO 周内 flag=FALSE 的明细并转为中文展示表（按超规时间倒序）。

    None / 空表 / 全部历史行无时间时返回带固定列结构的空表。
    """
    if decoration_df is None or decoration_df.empty:
        return pd.DataFrame(columns=AOI_RS_ALERT_COLUMNS)

    alerts_df = build_sheet_oos_alerts(
        decoration_df,
        time_column="sheet_start_time",
        reference_date=reference_date,
    )
    display_df = build_sheet_oos_alert_display(
        alerts_df,
        column_map=AOI_RS_ALERT_COLUMN_MAP,
        output_columns=AOI_RS_ALERT_COLUMNS,
    )
    if "图类型" in display_df.columns:
        display_df["图类型"] = display_df["图类型"].astype(str).replace(
            AOI_RS_ALERT_CHART_KIND_LABELS
        )
    if "超规时间" in display_df.columns:
        display_df["超规时间"] = display_df["超规时间"].astype(str)
    return display_df


def render_aoi_rs_sheet_oos_alert_indicator_sections(
    alerts_df: pd.DataFrame,
    *,
    rs_details_df: pd.DataFrame,
    pass_through_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
    end_date: date,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """按预警键（厂别+站点+RS Code）自动过滤并渲染命中指标的图像，无需筛选器操作。"""
    if alerts_df.empty:
        return

    alert_rs_details_df = filter_report_by_alert_keys(rs_details_df, alerts_df, AOI_RS_ALERT_KEY_MAP)
    alert_pass_through_df = filter_report_by_alert_keys(
        pass_through_df, alerts_df, AOI_RS_ALERT_PASS_THROUGH_KEY_MAP
    )
    alert_indicators_df = filter_report_by_alert_keys(indicators_df, alerts_df, AOI_RS_ALERT_KEY_MAP)
    alert_lot_points_df = filter_report_by_alert_keys(lot_points_df, alerts_df, AOI_RS_ALERT_KEY_MAP)
    alert_sheet_points_df = filter_report_by_alert_keys(
        sheet_points_df, alerts_df, AOI_RS_ALERT_KEY_MAP
    )
    if alert_indicators_df.empty:
        st.warning("单片异常预警指标暂无可绘制的 AOI RS 数据。")
        return

    indicator_count = (
        alert_indicators_df.groupby(["factory", "step_id", "rs_code"]).ngroups
        if {"factory", "step_id", "rs_code"}.issubset(alert_indicators_df.columns)
        else 0
    )
    with st.expander(f"🚨 单片异常预警指标图像（{indicator_count} 个指标）", expanded=False):
        st.caption("以下图像由单片异常预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        render_aoi_rs_indicator_sections(
            rs_details_df=alert_rs_details_df,
            pass_through_df=alert_pass_through_df,
            spec_df=spec_df,
            indicators_df=alert_indicators_df,
            lot_points_df=alert_lot_points_df,
            sheet_points_df=alert_sheet_points_df,
            end_date=end_date,
            chart_key_prefix="aoi_rs_alert",
            step_desc_map=step_desc_map,
        )
