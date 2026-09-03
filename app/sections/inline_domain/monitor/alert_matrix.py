"""自动预警看板"产品 × 监控参数"矩阵 UI（PRD §4.2，Phase 4）。

- 矩阵本体只渲染四态指示灯（PRD D1：不自动渲染图像）；点击单元格仅把
  ``detail_key`` 写入 session_state，详情由 ``alert_matrix_detail`` 懒加载；
- 交互采用 st.button 网格（而非 dataframe 单元格选择）：按钮原生支持
  ``help`` tooltip（⬜ 悬浮查看失败原因），且 AppTest / Playwright 均可
  稳定定位点击，简单可靠；
- 本模块只消费 payload dict，不参与 payload 计算（RenderGate 两阶段：
  计算集中在 ``get_cached_alert_matrix``，矩阵本体无图像、一次性渲染）；
- 矩阵整体加载失败降级为 info 提示，不阻断页面其余部分。
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

import streamlit as st

from app.sections.inline_domain.monitor.alert_matrix_cache import get_cached_alert_matrix
from app.sections.inline_domain.monitor.alert_matrix_service import (
    CELL_STATE_ALERT,
    CELL_STATE_ERROR,
    CELL_STATE_NO_DATA,
    CELL_STATE_OK,
)

logger = logging.getLogger(__name__)

# 选中单元格的 session_state 键；alert_matrix_detail 按同一键读取（懒加载触发源）。
MATRIX_SELECTION_STATE_KEY = "alert_matrix_selected_cell"

CELL_STATE_ICONS = {
    CELL_STATE_OK: "🟢",
    CELL_STATE_ALERT: "🔴",
    CELL_STATE_NO_DATA: "⚪",
    CELL_STATE_ERROR: "⬜",
}

_STATE_HELP = {
    CELL_STATE_OK: "达标：上一周期无预警",
    CELL_STATE_ALERT: "有预警：点击查看预警明细与图像",
    CELL_STATE_NO_DATA: "无数据",
    CELL_STATE_ERROR: "加载失败",
}

MODULE_GROUP_LABELS = {
    "aoi_rs": "AOI_RS",
    "aoi_tt": "AOI_TT",
    "spc": "SPC",
    "ctq": "CTQ",
    "yield": "Yield",
    "qtime": "Q-Time",
}

_MATRIX_BUTTON_KEY_PREFIX = "matrix_cell"
_ROW_LABEL_WIDTH = 2.2


def _select_cell(detail_key: str) -> None:
    """st.button on_click 回调：仅记录选中单元格，不产生任何详情计算。"""
    st.session_state[MATRIX_SELECTION_STATE_KEY] = detail_key


def _cell_help(cell: Mapping[str, str]) -> str:
    """单元格 tooltip：状态说明 +（如有）降级原因，⬜ 的 message 在此可见。"""
    state = cell.get("state", CELL_STATE_ERROR)
    message = (cell.get("message") or "").strip()
    base = _STATE_HELP.get(state, str(state))
    return f"{base}：{message}" if message else base


def matrix_cell_button_key(row_key: str, prod_code: str) -> str:
    """单元格按钮 key（测试与 E2E 定位锚点）。"""
    return f"{_MATRIX_BUTTON_KEY_PREFIX}_{row_key}_{prod_code}"


def _render_legend(rows: Sequence[Mapping[str, Any]], week: Mapping[str, str]) -> None:
    st.caption(
        "🟢 达标（无预警）｜🔴 有预警（点击查看详情）｜⚪ 无数据｜⬜ 加载失败（悬停查看原因）"
    )
    iso_scope_note = (
        f"上一 ISO 周（{week.get('start', '?')} ~ {week.get('end', '?')}，不含本周）"
    )
    # period 制行（yield 良率波动）时间口径不同，图例中逐行注明（PRD §3.1-2）。
    other_scopes = "；".join(
        f"「{row['display_name']}」{row['time_scope']}"
        for row in rows
        if row.get("time_scope") and row["time_scope"] != "上一 ISO 周"
    )
    caption = f"时间口径：{iso_scope_note}"
    if other_scopes:
        caption += f"；{other_scopes}"
    st.caption(caption)


def render_alert_matrix_section(payload: Mapping[str, Any]) -> None:
    """渲染矩阵标题、图例与四态按钮网格。点击仅写 session_state，不产生计算。"""
    products = list(payload.get("products", []))
    rows = list(payload.get("rows", []))
    cells = payload.get("cells", {})
    week = payload.get("reference_week", {})
    week_label = week.get("label", "")

    title = "#### 🚦 预警矩阵"
    if week_label:
        title += f"（上一周 {week_label}）"
    st.markdown(title)

    if not products or not rows:
        st.info("预警矩阵暂无可展示的数据。")
        return

    _render_legend(rows, week)

    column_widths = [_ROW_LABEL_WIDTH] + [1.0] * len(products)
    header_columns = st.columns(column_widths)
    header_columns[0].markdown("**监控参数**")
    for column, prod_code in zip(header_columns[1:], products):
        column.markdown(f"**{prod_code}**")

    previous_group: str | None = None
    for row in rows:
        group = str(row.get("module_group", ""))
        if group != previous_group:
            # 同模块行相邻（注册表顺序保证），以模块名小标题做可视分组。
            st.caption(MODULE_GROUP_LABELS.get(group, group))
            previous_group = group
        line_columns = st.columns(column_widths)
        line_columns[0].markdown(str(row.get("display_name") or row.get("row_key", "")))
        row_key = str(row.get("row_key", ""))
        for column, prod_code in zip(line_columns[1:], products):
            cell = cells.get((row_key, prod_code)) or {}
            state = cell.get("state", CELL_STATE_ERROR)
            detail_key = cell.get("detail_key") or f"{row_key}|{prod_code}"
            column.button(
                CELL_STATE_ICONS.get(state, CELL_STATE_ICONS[CELL_STATE_ERROR]),
                key=matrix_cell_button_key(row_key, prod_code),
                help=_cell_help(cell),
                on_click=_select_cell,
                args=(detail_key,),
                width="stretch",
            )


def render_alert_matrix_board(
    *,
    db_manager: Any = None,
    step_desc_map: dict[str, str] | None = None,
    detail_loaders: Mapping[str, Any] | None = None,
) -> None:
    """页首矩阵区入口：payload 经 L2 缓存集中计算后一次性渲染，再按选中单元格懒加载详情。

    矩阵整体失败（如签名采集异常）降级为 info 提示，不阻断页面其余部分。
    """
    try:
        with st.spinner("正在加载预警矩阵..."):
            payload = get_cached_alert_matrix()
    except Exception as exc:  # noqa: BLE001 - 矩阵区整体降级是契约要求
        logger.exception("[alert-matrix] 矩阵 payload 加载失败: %s", exc)
        st.info(f"预警矩阵暂时不可用（{exc}），下方看板功能不受影响。")
        return

    render_alert_matrix_section(payload)

    # 延迟导入：详情模块汇集各域渲染依赖，仅在矩阵渲染时才引入。
    from app.sections.inline_domain.monitor.alert_matrix_detail import (
        render_alert_matrix_detail,
    )

    render_alert_matrix_detail(
        payload,
        db_manager=db_manager,
        step_desc_map=step_desc_map,
        loaders=detail_loaders,
    )
