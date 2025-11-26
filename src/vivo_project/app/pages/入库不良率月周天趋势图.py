# src/vivo_project/app/pages/2_📈_入库不良率趋势图.py

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.app.app_setup import AppSetup
AppSetup.initialize_app()

from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui


# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

st.title("📊 入库不良率趋势图")

if st.button("🔄 刷新数据"):
    st.cache_data.clear()
    st.rerun()


# --- 3. 数据加载 ---

mwd_group_data = YieldAnalysisService.get_mwd_trend_data()
mwd_code_data = YieldAnalysisService.get_code_level_trend_data()

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

def create_and_update_chart(df, title, show_legend, show_yticklabels, y_range, color_map, category_orders_map, warning_line_value=None):
    """(已升级) 绘制Group堆叠图，带警戒线"""
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None

    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)
    total_rates = df.groupby('time_period', observed=False)['defect_rate'].sum().reset_index()
    total_rates.rename(columns={'defect_rate': 'total_defect_rate'}, inplace=True)

    fig = px.bar(
        df, x='time_period', y='defect_rate', color='defect_group', 
        title=title,
        color_discrete_map=color_map,
        category_orders=category_orders_map,
        labels={"time_period": "时间", "defect_rate": "不良率", "defect_group": "Group"},
        hover_data={'defect_rate': ':.2%'}
    )
    
    fig.add_trace(
        go.Scatter(
            x=total_rates['time_period'], y=total_rates['total_defect_rate'],
            mode='text', text=[f'{rate:.2%}' for rate in total_rates['total_defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False,
        )
    )
    
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"警戒线: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    fig.update_layout(
        yaxis_range=y_range, yaxis_tickformat='.2%', showlegend=show_legend,
        xaxis_title=None, yaxis_title=None, title_font_size=16
    )
    fig.update_yaxes(showticklabels=show_yticklabels)
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    return fig

def create_single_trend_chart(df, title, y_range, warning_line_value=None):
    """(已升级) 绘制Code单柱图，带警戒线和柱顶标签"""
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None
    
    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)

    fig = px.bar(
        df, x='time_period', y='defect_rate', title=title,
        labels={"time_period": "时间", "defect_rate": "不良率"}
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['time_period'], y=df['defect_rate'], mode='text',
            text=[f'{rate:.2%}' for rate in df['defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False
        )
    )
    
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"警戒线: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    fig.update_traces(hovertemplate='<b>%{x}</b><br>不良率: %{y:.2%}', marker_color='#54a24b')
    fig.update_layout(
        yaxis_range=y_range, yaxis_tickformat='.2%', showlegend=False,
        xaxis_title=None, yaxis_title=None, title_font_size=16
    )
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    return fig

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


    max_rate = 0
    all_dfs = [df_monthly, df_weekly, df_daily]
    for df in all_dfs:
        if df is not None and not df.empty:
            bar_heights = df.groupby('time_period')['defect_rate'].sum()
            valid_max = bar_heights[np.isfinite(bar_heights)].max()
            if pd.notna(valid_max): max_rate = max(max_rate, valid_max)
    if not np.isfinite(max_rate) or max_rate == 0: max_rate = 0.1
    y_axis_range = [0, max_rate * 1.2]

    warning_line_value = None
    if df_monthly is not None and not df_monthly.empty:
        monthly_total_rates = df_monthly.groupby('time_period')['defect_rate'].sum()
        best_month_rate = monthly_total_rates.min()
        if best_month_rate > 0:
            warning_line_value = best_month_rate * 1.1
        

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


        max_rate = 0
        for df in [df_m, df_w, df_d]:
            if df is not None and not df.empty:
                max_rate = max(max_rate, df['defect_rate'].max())
        y_axis_range = [0, max_rate * 1.25] if max_rate > 0 else [0, 0.01]

        warning_line_value = None
        if df_m is not None and not df_m.empty:
            best_month_rate = df_m['defect_rate'].min()
            if best_month_rate > 0:
                warning_line_value = best_month_rate * 1.1

        warning_line_value = None
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


