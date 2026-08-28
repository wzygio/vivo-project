# app/sections/parts_dashboard.py
"""
[前端组件] 关键备件报表的积木式渲染模块。
"""

import streamlit as st
import pandas as pd
from typing import List


PARTS_TABLE_COLUMN_ORDER = (
    "厂别", "备件类型", "设备类型", "膜层", "制程",
    "寿命规格", "站点", "机台号-腔室",
    "测量值", "使用进度", "预警状态", "测量时间",
)


def render_parts_header(title: str):
    """渲染页面标题行。"""
    st.title(title)


def render_parts_refresh_button() -> bool:
    """渲染数据刷新按钮。"""
    col_title, col_refresh = st.columns([6, 1])
    with col_refresh:
        clicked = st.button("🔄 刷新数据", width="stretch")
    return clicked


def render_factory_filter(available_factories: List[str]) -> str:
    """渲染旧版厂别筛选器，保留给外部兼容调用。"""
    if not available_factories:
        st.warning("无可用的厂别数据。")
        return ""
    return st.selectbox("🔽 厂别筛选", options=available_factories, index=0)


def render_parts_metrics(
    total_count: int,
    over_count: int,
    warning_count: int,
    normal_count: int,
    last_update: str,
):
    """
    渲染概览统计卡片（5 列 metric）。

    Args:
        total_count: 总备件数
        over_count: 超规数 (>100%)
        warning_count: 预警数 (>90%)
        normal_count: 正常数
        last_update: 最后更新时间字符串
    """
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("总备件数", total_count)

    with col2:
        st.metric("🔴 超规", over_count)

    with col3:
        st.metric("⚠️ 预警", warning_count)

    with col4:
        st.metric("✅ 正常", normal_count)

    with col5:
        st.metric("最后更新", last_update if last_update else "—")


def render_parts_table(df: pd.DataFrame):
    """
    渲染备件数据表（静态，备用）。
    """
    if df.empty:
        st.info("当前筛选条件下没有数据。")
        return

    column_config = {
        "使用进度": st.column_config.ProgressColumn(
            "使用进度 (%)",
            help="测量值 / 寿命规格 x 100%",
            format="%.0f%%",
            min_value=0,
            max_value=100,
        ),
        "预警状态": st.column_config.TextColumn("预警状态"),
        "测量值": st.column_config.NumberColumn(
            "测量值",
            help="从数据库查询的最新测量值",
            format="%.0f",
        ),
        "寿命规格": st.column_config.NumberColumn(
            "寿命规格",
            help="备件额定寿命",
            format="%.0f",
        ),
        "测量时间": st.column_config.TextColumn(
            "测量时间",
            help="最近一次数据采集时间",
        ),
    }

    valid_columns = [col for col in PARTS_TABLE_COLUMN_ORDER if col in df.columns]

    st.dataframe(
        df,
        column_config=column_config,
        column_order=valid_columns,
        hide_index=True,
        width="stretch",
    )


def render_parts_table_selectable(df: pd.DataFrame) -> dict:
    """
    渲染可交互、支持单行点击选中的关键备件数据表。

    Args:
        df: 要展示的 DataFrame

    Returns:
        dict: 用户选中行状态字典
    """
    if df.empty:
        st.info("当前筛选条件下没有数据。")
        return {"selection": {"rows": []}}

    column_config = {
        "使用进度": st.column_config.ProgressColumn(
            "使用进度 (%)",
            help="测量值 / 寿命规格 x 100%",
            format="%.0f%%",
            min_value=0,
            max_value=100,
        ),
        "预警状态": st.column_config.TextColumn("预警状态"),
        "测量值": st.column_config.NumberColumn(
            "测量值",
            help="从数据库查询的最新测量值",
            format="%.0f",
        ),
        "寿命规格": st.column_config.NumberColumn(
            "寿命规格",
            help="备件额定寿命",
            format="%.0f",
        ),
        "测量时间": st.column_config.TextColumn(
            "测量时间",
            help="最近一次数据采集时间",
        ),
    }

    valid_columns = [col for col in PARTS_TABLE_COLUMN_ORDER if col in df.columns]

    selected_rows = st.dataframe(
        df,
        column_config=column_config,
        column_order=valid_columns,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        key="parts_life_table",
    )

    return selected_rows
