# app/sections/parts_dashboard.py
"""
[前端组件] 关键备件报表的积木式渲染模块。
"""

import streamlit as st
import pandas as pd
from typing import List


def render_parts_header(title: str):
    st.title(title)


def render_parts_refresh_button() -> bool:
    col_title, col_refresh = st.columns([6, 1])
    with col_refresh:
        clicked = st.button("🔄 刷新数据", use_container_width=True)
    return clicked


def render_factory_filter(available_factories: List[str]) -> str:
    if not available_factories:
        st.warning("无可用的厂别数据。")
        return ""
    return st.selectbox("🔽 厂别筛选", options=available_factories, index=0)


def render_parts_metrics(
    total_count: int,
    real_count: int,
    simulated_count: int,
    over_count: int,
    warning_count: int,
    normal_count: int,
    last_update: str,
):
    """渲染概览统计卡片（6 列）。"""
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("总备件数", total_count)
    with col2:
        st.metric("📡 真实数据", real_count)
    with col3:
        st.metric("🔶 模拟数据", simulated_count)
    with col4:
        st.metric("🔴 超规", over_count)
    with col5:
        st.metric("⚠️ 预警", warning_count)
    with col6:
        st.metric("最后更新", last_update if last_update else "—")


def render_parts_table_selectable(df: pd.DataFrame) -> dict:
    if df.empty:
        st.info("当前筛选条件下没有数据。")
        return {"selection": {"rows": []}}

    column_config = {
        "使用进度": st.column_config.ProgressColumn(
            "使用进度 (%)", help="测量值 / 寿命规格 x 100%",
            format="%.0f%%", min_value=0, max_value=100,
        ),
        "预警状态": st.column_config.TextColumn("预警状态"),
        "测量值": st.column_config.NumberColumn(
            "测量值", help="最新测量值（含模拟补全）", format="%.0f",
        ),
        "寿命规格": st.column_config.NumberColumn(
            "寿命规格", help="备件额定寿命", format="%.0f",
        ),
        "测量时间": st.column_config.TextColumn("测量时间"),
    }

    column_order = [
        "厂别", "备件类型", "设备类型", "膜层", "制程",
        "寿命规格", "站点", "机台号-腔室",
        "测量值", "使用进度", "预警状态", "测量时间",
    ]
    valid_columns = [col for col in column_order if col in df.columns]

    return st.dataframe(
        df, column_config=column_config, column_order=valid_columns,
        hide_index=True, use_container_width=True,
        on_select="rerun", selection_mode="single-row",
    )