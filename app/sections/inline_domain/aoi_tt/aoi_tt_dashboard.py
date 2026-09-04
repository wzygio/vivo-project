"""AOI_TT 报表 Dashboard：筛选（厂别/站点/Code名称）与三张图渲染。

图表口径（与 AOI_RS 布局一致，规格线为 USL/UCL 双上限）：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：
   值 = Σtt_qty ÷ 同 period 同站点检测 distinct sheet/glass 数，按 TT 分线，
   叠加 USL/UCL 规格线，双 Y 轴检测片数柱状。
2. By Lot 别点线图：每 lot 的 Lot 内平均每片 TT 个数（Σtt_qty ÷ Lot 内检测片数），叠加 USL/UCL。
3. By Sheet 别点线图：每 sheet/glass 的 TT 个数，叠加 USL/UCL。

另含单片异常（Sheet OOS）预警：只读加载修饰工作簿中上一 ISO 周 flag=FALSE 明细，
并按预警键自动过滤出图（见 docs/PRD/PRD-2026-08-25-Inline自动预警中心.md）。

公共筛选与绘图管线位于 ``app.sections.inline_domain.shared``，本模块只保留
TT 业务差异：USL/UCL 双上限规格线（虚线/点线）与检测片数文案。
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.charts.inline_domain import (
    AoiSpecLine,
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
from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import (
    PARTICLE_SIZE_OPTIONS,
    attach_spec_values,
    build_lot_point_df,
    build_period_throughput_df,
    build_period_trend_df,
    build_sheet_point_df,
)
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import (
    AOI_TT_OOS_DECORATION_FILE_NAME,
    AOI_TT_OOS_KEY_COLUMNS,
)
from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
    load_sheet_oos_decoration,
)
from src.inline_domain.core.spc.spc_calculator import get_period_window_start

logger = logging.getLogger(__name__)

AOI_TT_FACTORY_OPTIONS = INLINE_FACTORY_OPTIONS
USL_COLOR = "#dc2626"
UCL_COLOR = "#f59e0b"

# 单片异常预警展示列（中文）与过滤键映射（中文列 → 报表英文列）
AOI_TT_ALERT_COLUMN_MAP = {
    "factory": "厂别",
    "step_id": "站点",
    "tt_name": "TT名称",
    "sheet_id": "Sheet ID",
    "start_time": "超规时间",
    "tt_qty": "TT数量",
    "usl": "规格上限",
}
AOI_TT_ALERT_OUTPUT_COLUMNS = ["厂别", "站点", "TT名称", "Sheet ID", "超规时间", "TT数量", "规格上限"]
AOI_TT_ALERT_KEY_MAP = {"厂别": "factory", "站点": "step_id", "TT名称": "tt_name"}


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
) -> tuple[str, list[str], list[str], list[str], bool]:
    """渲染厂别/站点/Code名称与 Particle Size 筛选。"""
    selected_particle_sizes: list[str] = []

    def render_particle_size_filter(factory: str) -> None:
        nonlocal selected_particle_sizes
        particle_size_options = (
            ["Total"] if factory == "OLED" else list(PARTICLE_SIZE_OPTIONS)
        )
        selected_particle_sizes = st.multiselect(
            "Particle Size",
            options=particle_size_options,
            default=particle_size_options,
            key="aoi_tt_particle_size_filter",
        )

    factory, codes, steps, should_render = render_cascade_filters(
        indicator_df,
        key_prefix="aoi_tt",
        third_label="Code名称",
        third_column="tt_name",
        third_kind="code",
        factory_options=AOI_TT_FACTORY_OPTIONS,
        step_desc_map=step_desc_map,
        additional_filter_renderer=render_particle_size_filter,
    )
    return factory, codes, steps, selected_particle_sizes, should_render


def filter_aoi_tt_report(
    report_df: pd.DataFrame,
    selected_factory: str,
    selected_codes: list[str],
    selected_steps: list[str],
    selected_particle_sizes: list[str] | None = None,
) -> pd.DataFrame:
    """对含 factory/tt_name/step_id 列的数据框应用前端筛选。"""
    filtered = apply_report_filter(
        report_df,
        selected_factory,
        selected_codes,
        selected_steps,
        third_column="tt_name",
    )
    if selected_particle_sizes is not None and "particle_size" in filtered.columns:
        filtered = filtered[filtered["particle_size"].isin(selected_particle_sizes)]
    return filtered


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


def _render_particle_size_charts(
    *,
    particle_size: str,
    code: str,
    trend_df: pd.DataFrame,
    throughput_df: pd.DataFrame,
    lot_df: pd.DataFrame,
    sheet_df: pd.DataFrame,
    usl: float | None,
    ucl: float | None,
) -> None:
    """Render the three AOI TT charts for one particle-size slice."""
    st.markdown(f"**Particle Size：{particle_size}**")
    trend_points = trend_df[
        (trend_df["tt_name"].astype(str) == code)
        & (trend_df["particle_size"].astype(str) == particle_size)
    ]
    lot_points = lot_df[
        (lot_df["tt_name"].astype(str) == code)
        & (lot_df["particle_size"].astype(str) == particle_size)
    ]
    sheet_points = sheet_df[
        (sheet_df["tt_name"].astype(str) == code)
        & (sheet_df["particle_size"].astype(str) == particle_size)
    ]

    c_trend, c_lot, c_sheet = st.columns(3)
    with c_trend:
        st.plotly_chart(
            create_aoi_tt_trend_chart(
                trend_df=trend_points,
                throughput_df=throughput_df,
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
                point_df=lot_points,
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
                point_df=sheet_points,
                id_col="sheet_id",
                code_specs={code: (usl, ucl)},
                title="By Sheet（每片的 TT 个数）",
                y_title="TT 个数",
            ),
            width="stretch",
        )


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
                available_sizes = set(
                    step_trend.loc[
                        step_trend["tt_name"].astype(str) == code,
                        "particle_size",
                    ].astype(str)
                )
                ordered_sizes = [size for size in PARTICLE_SIZE_OPTIONS if size in available_sizes]
                for particle_size in ordered_sizes:
                    _render_particle_size_charts(
                        particle_size=particle_size,
                        code=code,
                        trend_df=step_trend,
                        throughput_df=step_throughput,
                        lot_df=step_lot,
                        sheet_df=step_sheet,
                        usl=usl,
                        ucl=ucl,
                    )


# ---------------------------------------------------------------------------
# 单片异常（Sheet OOS）预警：上一 ISO 周 flag=FALSE 明细 + 命中指标自动出图
# ---------------------------------------------------------------------------


@st.cache_data(show_spinner=False, max_entries=3)
def _load_aoi_tt_oos_decoration_cached(
    prod_code: str,
    file_mtime_ns: int,
    file_size: int,
) -> pd.DataFrame | None:
    """只读加载当前产品的 Sheet OOS 修饰明细；缓存键含文件 (mtime_ns, size)。

    读取失败（含企业加密文件 COM 回退失败）降级返回 None，绝不阻断页面、
    也绝不触发工作簿写入。
    """
    product_dir = resolve_product_resource_dir(prod_code)
    try:
        return load_sheet_oos_decoration(
            product_dir,
            AOI_TT_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_TT_OOS_KEY_COLUMNS,
        )
    except SheetOosDecorationReadError:
        logger.warning("[AOI_TT] Sheet OOS decoration unreadable for %s, alerts degraded", prod_code)
        return None
    except Exception:  # noqa: BLE001 - 预警只读消费，任何异常都降级
        logger.exception("[AOI_TT] failed to load Sheet OOS decoration for %s", prod_code)
        return None


def load_aoi_tt_oos_decoration(prod_code: str) -> pd.DataFrame | None:
    """加载当前产品的 Sheet OOS 修饰明细（用于单片异常预警）。

    文件不存在时直接返回 None；存在时按 (mtime_ns, size) 命中 st.cache_data，
    普通 rerun 不会重复读取工作簿。
    """
    decoration_path = (
        resolve_product_resource_dir(prod_code) / AOI_TT_OOS_DECORATION_FILE_NAME
    )
    try:
        stat = decoration_path.stat()
    except OSError:
        return None
    return _load_aoi_tt_oos_decoration_cached(prod_code, stat.st_mtime_ns, stat.st_size)


def build_aoi_tt_sheet_oos_alerts(
    decoration_df: pd.DataFrame | None,
    reference_date: date | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """构建单片异常预警展示表：上一 ISO 周内 flag=FALSE 的明细，中文列、按时间倒序。"""
    if decoration_df is None or decoration_df.empty:
        return pd.DataFrame(columns=AOI_TT_ALERT_OUTPUT_COLUMNS)
    alerts = build_sheet_oos_alerts(
        decoration_df,
        time_column="start_time",
        reference_date=reference_date,
    )
    display = build_sheet_oos_alert_display(
        alerts,
        column_map=AOI_TT_ALERT_COLUMN_MAP,
        output_columns=AOI_TT_ALERT_OUTPUT_COLUMNS,
    )
    if "超规时间" in display.columns:
        display["超规时间"] = display["超规时间"].astype(str)
    return display


def render_aoi_tt_sheet_oos_alert_indicator_sections(
    alerts_df: pd.DataFrame,
    *,
    tt_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    end_date: date,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """按预警键自动过滤并渲染命中指标的图像（无需手动筛选）。

    ``render_aoi_tt_indicator_sections`` 的 st.plotly_chart 未显式传 key，预警区与
    手动筛选区即使渲染同一指标，Streamlit 也会按调用位置生成不同内部 key，无冲突风险。
    """
    if alerts_df.empty:
        return

    alert_tt_details_df = filter_report_by_alert_keys(tt_details_df, alerts_df, AOI_TT_ALERT_KEY_MAP)
    alert_indicators_df = filter_report_by_alert_keys(indicators_df, alerts_df, AOI_TT_ALERT_KEY_MAP)
    if alert_indicators_df.empty or alert_tt_details_df.empty:
        st.warning("预警指标暂无可绘制的 AOI TT 数据。")
        return

    indicator_count = (
        alert_indicators_df.groupby(["factory", "step_id", "tt_name"]).ngroups
        if {"factory", "step_id", "tt_name"}.issubset(alert_indicators_df.columns)
        else 0
    )
    with st.expander(f"🚨 单片异常预警指标图像（{indicator_count} 个指标）", expanded=False):
        st.caption("以下图像由单片异常预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        render_aoi_tt_indicator_sections(
            tt_details_df=alert_tt_details_df,
            spec_df=spec_df,
            indicators_df=alert_indicators_df,
            end_date=end_date,
            step_desc_map=step_desc_map,
        )
