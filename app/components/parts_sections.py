# src/vivo_project/app/components/parts_sections.py
"""
[前端组件] 关键备件报表的积木式渲染模块。

遵循项目前端风格规范：
- 通用组件和 sections 汇总到 components/ 目录
- 采用积木式组合（Composable Sections）模式
- 每个 render_xxx_section() 函数独立渲染一个 UI 区块
"""

import streamlit as st
import pandas as pd
from typing import List


def render_parts_header(title: str):
    """
    渲染页面标题行。
    
    Args:
        title: 页面标题字符串
    """
    st.title(title)


def render_parts_refresh_button() -> bool:
    """
    渲染数据刷新按钮。
    
    Returns:
        bool: 是否点击了刷新按钮
    """
    col_title, col_refresh = st.columns([6, 1])
    with col_refresh:
        clicked = st.button("🔄 刷新数据", use_container_width=True)
    return clicked


def render_factory_filter(available_factories: List[str]) -> str:
    """
    渲染厂别下拉筛选器。
    
    Args:
        available_factories: 可用的厂别列表
    
    Returns:
        str: 选中的厂别名称
    """
    if not available_factories:
        st.warning("无可用的厂别数据。")
        return ""
    
    # 默认选中第一个厂别
    default_idx = 0
    selected = st.selectbox(
        "🔽 厂别筛选",
        options=available_factories,
        index=default_idx,
    )
    return selected


def render_parts_metrics(
    total_count: int,
    warning_count: int,
    normal_count: int,
    last_update: str,
):
    """
    渲染概览统计卡片（4 列 metric）。
    
    Args:
        total_count: 总备件数
        warning_count: 超预警数
        normal_count: 正常数
        last_update: 最后更新时间字符串
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("总备件数", total_count)
    
    with col2:
        st.metric(
            "⚠️ 超预警",
            warning_count,
            delta_color="inverse",
        )
    
    with col3:
        st.metric("✅ 正常", normal_count)
    
    with col4:
        st.metric("最后更新", last_update if last_update else "—")


def render_parts_table(df: pd.DataFrame):
    """
    渲染备件数据表（带使用进度 ProgressColumn 进度条）。
    
    Args:
        df: 要展示的 DataFrame
    """
    if df.empty:
        st.info("当前筛选条件下没有数据。")
        return
    
    column_config = {
        "使用进度": st.column_config.ProgressColumn(
            "使用进度 (%)",
            help="实际数据 / 寿命规格 × 100%",
            format="%.0f%%",
            min_value=0,
            max_value=100,
        ),
        "预警状态": st.column_config.TextColumn(
            "预警状态",
            help="⚠️ 超预警 或 ✅ 正常",
        ),
        "实际数据": st.column_config.NumberColumn(
            "实际数据",
            help="从数据库查询的最新测量值",
            format="%.0f",
        ),
        "寿命规格": st.column_config.NumberColumn(
            "寿命规格 (HR)",
            help="备件额定寿命（小时）",
            format="%.0f",
        ),
        "预警值": st.column_config.NumberColumn(
            "预警值 (%)",
            help="触发预警的使用进度百分比阈值",
            format="%.0f%%",
        ),
        "测量时间": st.column_config.TextColumn(
            "测量时间",
            help="最近一次数据采集时间",
        ),
        "参数名称": st.column_config.TextColumn(
            "参数名称",
            help="数据库中的原始参数名",
        ),
    }
    
    # 定义列顺序（仅展示用户关心的列）
    column_order = [
        "厂别",
        "膜层",
        "制程",
        "机台",
        "腔室",
        "备件类型",
        "寿命规格",
        "预警值",
        "实际数据",
        "使用进度",
        "预警状态",
        "测量时间",
        "参数名称",
        "站点",
        "机台编号",
    ]
    
    # 只保留 column_order 中确实存在的列
    valid_columns = [col for col in column_order if col in df.columns]
    
    st.dataframe(
        df,
        column_config=column_config,
        column_order=valid_columns,
        hide_index=True,
        use_container_width=True,
    )
