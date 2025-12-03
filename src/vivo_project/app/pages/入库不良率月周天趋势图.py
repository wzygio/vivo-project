# src/vivo_project/app/pages/2_📈_入库不良率趋势图.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui, render_page_header, calculate_warning_lines
from vivo_project.app.charts.mwd_chart import create_and_update_chart, create_single_trend_chart, slice_recent_data

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📊 入库不良率月周天趋势图")


# --- 3. 数据加载 ---
mwd_group_data = YieldAnalysisService.get_mwd_trend_data()
mwd_code_data = YieldAnalysisService.get_code_level_trend_data()
# warning_lines_cache = calculate_warning_lines(mwd_code_data)
warning_lines_cache = YieldAnalysisService.load_static_warning_lines()

# ==============================================================================
#                      辅助函数
# ==============================================================================
# [颜色和顺序] 定义全局颜色和堆叠顺序
COLOR_MAP = {
    'Array_Line': "#1930ff",  # Plotly默认的蓝色
    'OLED_Mura': "#ff2828",   # Plotly默认的红色
    'Array_Pixel': "#6fb9ff"   # Plotly默认的浅蓝色
}
STACKING_ORDER = CONFIG['processing']['target_defect_groups']
CATEGORY_ORDERS_MAP = {"defect_group": STACKING_ORDER}

# ==============================================================================
#                      By-Group 月/周/天不良率趋势图
# ==============================================================================
st.header("📅 By-Group 月/周/天不良率趋势图")

if mwd_group_data is not None:
    col1_filter, _, _ = st.columns(3)
    with col1_filter:
        target_defect_groups_config = CONFIG['processing']['target_defect_groups']
        group_options = ["全部Group"] + target_defect_groups_config
        selected_group = st.selectbox("请选择Group(默认为全部):", options=group_options)

    df_monthly, df_weekly, df_daily = None, None, None
    if selected_group == "全部Group":
        df_monthly = mwd_group_data.get('monthly')
        df_weekly = mwd_group_data.get('weekly')
        df_daily = mwd_group_data.get('daily')
    else:
        df_monthly, df_weekly, df_daily = [
            data[data['defect_group'] == selected_group] if data is not None else None
            for data in (mwd_group_data.get('monthly'), mwd_group_data.get('weekly'), mwd_group_data.get('daily'))
        ]


    # --- [核心修改] 前端切片：只展示最近的数据 ---
    # 月度：近3个月
    df_monthly = slice_recent_data(df_monthly, n_recent=3)
    # 周度：近3周 (或4周，您自己决定)
    df_weekly = slice_recent_data(df_weekly, n_recent=3)
    # 日度：近7天
    df_daily = slice_recent_data(df_daily, n_recent=7)
    # ---------------------------------------------

    max_rate = 0
    all_dfs = [df_monthly, df_weekly, df_daily]
    for df in all_dfs:
        if df is not None and not df.empty:
            bar_heights = df.groupby('time_period')['defect_rate'].sum()
            valid_max = bar_heights[np.isfinite(bar_heights)].max()
            if pd.notna(valid_max): max_rate = max(max_rate, valid_max)
    if not np.isfinite(max_rate) or max_rate == 0: max_rate = 0.1
    y_axis_range = [0, max_rate * 1.35]

    warning_line_value = max_rate * 1.3
        
    warning_line_value = None
    col1, col2, col3 = st.columns(3)
    with col1:
        fig_m = create_and_update_chart(df_monthly, "月度趋势", False, True, y_axis_range, COLOR_MAP, CATEGORY_ORDERS_MAP, warning_line_value)
        if fig_m: st.plotly_chart(fig_m, use_container_width=True)
    with col2:
        fig_w = create_and_update_chart(df_weekly, "周度趋势", False, False, y_axis_range, COLOR_MAP, CATEGORY_ORDERS_MAP, warning_line_value)
        if fig_w: st.plotly_chart(fig_w, use_container_width=True)
    with col3:
        fig_d = create_and_update_chart(df_daily, "日度趋势", True, False, y_axis_range, COLOR_MAP, CATEGORY_ORDERS_MAP, warning_line_value)
        if fig_d: st.plotly_chart(fig_d, use_container_width=True)
else:
    st.error("未能加载月/周/天趋势数据，请检查后台日志。")

st.divider()

# # ==============================================================================
# #                      当月至今每日不良率趋势图
# # ==============================================================================
# current_month = datetime.now().month
# st.header(f"📈 {current_month}月至今每日不良率趋势图")

# if current_month_trend_data is not None and not current_month_trend_data.empty:
#     max_rate = current_month_trend_data.groupby('time_period')['defect_rate'].sum().max()
#     y_axis_range_oct = [0, max_rate * 1.2] if max_rate > 0 else [0, 0.1]
    
#     fig_oct = create_and_update_chart(
#         df=current_month_trend_data,
#         title=f"{current_month}月至今每日趋势",
#         show_legend=True, show_yticklabels=True, y_range=y_axis_range_oct,
#         color_map=COLOR_MAP, category_orders_map=CATEGORY_ORDERS_MAP
#     )
#     if fig_oct:
#         fig_oct.update_xaxes(tickangle=-45)
#         st.plotly_chart(fig_oct, use_container_width=True)
# else:
#     st.warning(f"未能加载{current_month}月至今的趋势数据。")

# st.divider()

# ==============================================================================
#                      By-Code 不良率趋势图
# ==============================================================================
st.header("📈 ByCode 月/周/天不良率趋势图")

if mwd_code_data:
    source_df_for_selector = pd.concat(
        [df for df in mwd_code_data.values() if df is not None and not df.empty], 
        ignore_index=True
    )

    selected_code_info_trend = create_code_selection_ui(
        source_data=source_df_for_selector,
        target_defect_groups=CONFIG['processing']['target_defect_groups'],
        key_prefix="trend_focus",
        rate_threshold=0.0001
    )

    if selected_code_info_trend.get("code"):
        group = selected_code_info_trend["group"]
        code_desc = selected_code_info_trend["code"]
        
        df_m, df_w, df_d = None, None, None
        df_m, df_w, df_d = [
            data[data['defect_desc'] == code_desc] if data is not None else None
            for data in (mwd_code_data.get('monthly'), mwd_code_data.get('weekly'), mwd_code_data.get('daily'))
        ]

        # --- [核心修改] 前端切片：只展示最近的数据 ---
        # 必须先切片再计算 Y 轴范围，否则 Y 轴会被以前的高点撑大
        df_m = slice_recent_data(df_m, n_recent=3)
        df_w = slice_recent_data(df_w, n_recent=3)
        df_d = slice_recent_data(df_d, n_recent=7)
        # ---------------------------------------------
        
        max_rate = 0
        for df in [df_m, df_w, df_d]:
            if df is not None and not df.empty:
                max_rate = max(max_rate, df['defect_rate'].max())
        y_axis_range = [0, max_rate * 1.35] if max_rate > 0 else [0, 0.01]

        # warning_line_value = warning_lines_cache.get(code_desc)
            
        st.markdown(f"#### **{code_desc}月周天趋势图**")
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        with chart_col1:
            fig_m = create_single_trend_chart(df_m, "月度趋势", y_axis_range, warning_line_value)
            if fig_m: st.plotly_chart(fig_m, use_container_width=True)
        with chart_col2:
            fig_w = create_single_trend_chart(df_w, "周度趋势", y_axis_range, warning_line_value)
            if fig_w: st.plotly_chart(fig_w, use_container_width=True)
        with chart_col3:
            fig_d = create_single_trend_chart(df_d, "日度趋势", y_axis_range, warning_line_value)
            if fig_d: st.plotly_chart(fig_d, use_container_width=True)
else:
    st.warning("未能加载Code级月/周/天趋势数据，请检查后台日志。")

st.divider()


