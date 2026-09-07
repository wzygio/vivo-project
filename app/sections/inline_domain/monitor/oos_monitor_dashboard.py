"""Streamlit presentation for the shared Inline OOS history read model."""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pandas as pd
import streamlit as st
from pydantic import BaseModel, Field

from src.inline_domain.application.monitor.oos_monitor_service import OosMonitorViewModel

SCOPE_OPTIONS = ("ALL", "SPC", "CTQ", "AOI_TT", "AOI_RS")
MONITOR_QUERY_SIGNATURE_KEY = "monitor_query_signature"


class MonitorFilterState(BaseModel):
    selected_products: list[str] = Field(default_factory=list)
    selected_factories: list[str] = Field(default_factory=list)
    data_type_filter: str = "ALL"


def _filter_signature(filter_state: MonitorFilterState) -> tuple[object, ...]:
    return (
        filter_state.data_type_filter,
        tuple(sorted(filter_state.selected_products)),
        tuple(sorted(filter_state.selected_factories)),
    )


def render_monitor_query_button() -> bool:
    return st.button(
        "查询",
        type="primary",
        key="btn_monitor_query_submit",
        help="按当前筛选条件读取共享超规历史；不会重新执行全量量测分析。",
    )


def render_monitor_query_gate(
    filter_state: MonitorFilterState, *, clicked: bool = False
) -> bool:
    signature = _filter_signature(filter_state)
    if clicked:
        st.session_state[MONITOR_QUERY_SIGNATURE_KEY] = signature
    return st.session_state.get(MONITOR_QUERY_SIGNATURE_KEY) == signature


def selected_scopes(data_type_filter: str) -> tuple[str, ...]:
    normalized = str(data_type_filter).strip().lower()
    return ("spc", "ctq", "aoi_tt", "aoi_rs") if normalized == "all" else (normalized,)


def render_oos_monitor_control_panel(
    available_products: Sequence[str],
    available_factories: Sequence[str],
    *,
    action_renderer: Callable[[], None] | None = None,
) -> MonitorFilterState:
    columns = st.columns([1.0, 2.6, 1.6, 0.8], vertical_alignment="bottom")
    with columns[0]:
        scope = st.selectbox(
            "监控类型",
            options=SCOPE_OPTIONS,
            key="spc_data_type_filter",
            help="四类已计算的 Sheet OOS 事实；不包含 OOC、SOOS 和报废。",
        )
    with columns[1]:
        products = st.multiselect(
            "产品型号",
            options=available_products,
            default=available_products,
            key="monitor_products",
        )
    with columns[2]:
        factories = st.multiselect(
            "厂别",
            options=available_factories,
            default=available_factories,
            key="monitor_factories",
        )
    if action_renderer is not None:
        with columns[3]:
            action_renderer()
    return MonitorFilterState(
        selected_products=list(products),
        selected_factories=list(factories),
        data_type_filter=str(scope),
    )


def render_oos_monitor_results(
    view: OosMonitorViewModel,
    *,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    detail = view.detail_df
    oos_count = int(detail["alarm_type"].eq("OOS").sum()) if not detail.empty else 0
    ooc_count = int(detail["alarm_type"].eq("OOC").sum()) if not detail.empty else 0
    metric_columns = st.columns(3)
    metric_columns[0].metric("OOS", f"{oos_count:,}")
    metric_columns[1].metric("OOC", f"{ooc_count:,}")
    metric_columns[2].metric("SOOS", "0")

    if detail.empty:
        st.warning("所选范围内没有已确认的 OOS/OOC 预警记录。")
        return

    st.markdown("#### 超规趋势")
    trend_data = view.trend_df.assign(
        series=view.trend_df["scope"].str.upper() + "-" + view.trend_df["alarm_type"]
    )
    trend = trend_data.pivot_table(
        index="event_date", columns="series", values="oos_count", aggfunc="sum", fill_value=0
    )
    st.line_chart(trend, height=320)

    st.markdown("#### Top 10 站点")
    station = view.station_df.copy()
    descriptions = step_desc_map or {}
    station["站点"] = station["step_id"].astype(str).map(
        lambda value: f"{value} {descriptions.get(value, '')}".strip()
    )
    station["series"] = station["scope"].str.upper() + "-" + station["alarm_type"]
    station_chart = station.pivot_table(
        index="站点", columns="series", values="oos_count", aggfunc="sum", fill_value=0
    )
    st.bar_chart(station_chart, horizontal=True, height=360)

    st.markdown("#### 超规明细")
    display = view.detail_df.rename(
        columns={
            "alarm_type": "预警类型",
            "scope": "类型",
            "factory": "厂别",
            "prod_code": "产品",
            "step_id": "站点",
            "metric_name": "参数",
            "item_id": "Sheet/点位",
            "event_time": "时间",
            "observed_value": "实测值",
            "upper_limit": "上限",
            "lower_limit": "下限",
            "limit_type": "越界方向",
        }
    )
    st.dataframe(
        display[
            ["时间", "预警类型", "类型", "产品", "厂别", "站点", "参数", "Sheet/点位", "实测值", "上限", "下限", "越界方向"]
        ],
        width="stretch",
        hide_index=True,
    )


def render_oos_refresh_status(status_df: pd.DataFrame) -> None:
    """Admin-only caller renders the product × scope freshness table."""
    st.markdown("#### 数据最后更新时间（管理员）")
    status = status_df.copy()
    status["refreshed_at"] = pd.to_datetime(status["refreshed_at"], errors="coerce")
    status["refreshed_at"] = status["refreshed_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    status["refreshed_at"] = status["refreshed_at"].fillna("从未刷新")
    status["source"] = status["source"].map(
        {"history": "长期历史", "workbook_fallback": "当前工作簿", "missing": "无数据"}
    ).fillna(status["source"])
    st.dataframe(
        status.rename(
            columns={
                "prod_code": "产品",
                "scope": "类型",
                "alarm_type": "预警类型",
                "source": "来源",
                "refreshed_at": "最后更新时间",
            }
        ),
        width="stretch",
        hide_index=True,
    )


__all__ = [
    "render_monitor_query_button",
    "render_monitor_query_gate",
    "render_oos_monitor_control_panel",
    "render_oos_monitor_results",
    "render_oos_refresh_status",
    "selected_scopes",
]
