import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# --- [新增] 辅助函数：截取最近N个周期 ---
def slice_recent_data(df, n_recent=3, time_col='time_period'):
    """保留 DataFrame 中 time_col 列最近的 n_recent 个唯一值对应的数据"""
    if df is None or df.empty:
        return df
    unique_periods = sorted(df[time_col].unique())
    if len(unique_periods) > n_recent:
        recent_periods = unique_periods[-n_recent:]
        return df[df[time_col].isin(recent_periods)]
    return df



# -----------------------------------------------------------------------------
#  Group 级图表绘制
# -----------------------------------------------------------------------------
def create_group_trend_chart_panel(
    df: pd.DataFrame, 
    title: str, 
    show_legend: bool, 
    show_yticklabels: bool, 
    y_range: list, 
    color_map: dict, 
    category_orders_map: dict, 
    warning_line_value: float = None # type: ignore
) -> go.Figure | None:
    """
    [升级版] 绘制 Group 级堆叠柱状图 (支持双轴：左轴不良率，右轴入库量)
    """
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None

    # 确保排序正确
    df = df.copy()
    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)
    
    # 计算总不良率用于 Label
    total_rates = df.groupby('time_period', observed=False)['defect_rate'].sum().reset_index()
    total_rates.rename(columns={'defect_rate': 'total_defect_rate'}, inplace=True)

    # --- [新增] 提取入库量数据 (用于右轴) ---
    # 因为 melt 过，total_panels 在每个 group 行都是重复的，所以按时间去重即可
    has_panel_count = 'total_panels' in df.columns
    df_panels = None
    if has_panel_count:
        df_panels = df[['time_period', 'total_panels']].drop_duplicates().sort_values('time_period')
    # --------------------------------------

    # 1. 基础柱状图 (左轴)
    # 使用 Plotly Express 创建基础图对象
    fig = px.bar(
        df, x='time_period', y='defect_rate', color='defect_group', 
        title=title,
        color_discrete_map=color_map,
        category_orders=category_orders_map,
        labels={"time_period": "时间", "defect_rate": "不良率", "defect_group": "Group"},
        hover_data={'defect_rate': ':.2%'}
    )
    
    # 2. [新增] 添加入库量折线 (右轴)
    if has_panel_count and df_panels is not None:
        fig.add_trace(
            go.Scatter(
                x=df_panels['time_period'], 
                y=df_panels['total_panels'],
                name='入库数',
                mode='lines+markers',
                yaxis='y2', # 指定使用第二个 Y 轴
                line=dict(color='gray', width=1, dash='dot'), # 灰色虚线，避免抢视觉
                marker=dict(symbol='circle', size=4, color='gray'),
                hovertemplate='入库数: %{y}<extra></extra>', # 优化浮窗
                showlegend=False # 可以在这里设为 True，但在小图上建议隐藏
            )
        )

    # 3. 添加总计数值标签 (左轴)
    fig.add_trace(
        go.Scatter(
            x=total_rates['time_period'], y=total_rates['total_defect_rate'],
            mode='text', text=[f'{rate:.2%}' for rate in total_rates['total_defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False,
        )
    )
    
    # 4. 添加spec
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    # 5. 布局调整 (含双轴配置)
    layout_update = dict(
        yaxis_range=y_range, 
        yaxis_tickformat='.2%', 
        showlegend=show_legend,
        xaxis_title=None, 
        yaxis_title=None, 
        title_font_size=16,
        # --- [新增] 右轴配置 ---
        yaxis2=dict(
            title=None, # 为了简洁不显示标题，或者设为 "入库数"
            overlaying='y', # 覆盖在第一个 y 轴上
            side='right',   # 放在右侧
            showgrid=False, # 不显示网格，保持清爽
            showticklabels=False, # 可以选择是否显示右轴刻度，False 更简洁
            rangemode='tozero' # 强制从0开始
        )
    )
    
    fig.update_layout(overwrite=False, **layout_update)
    fig.update_yaxes(showticklabels=show_yticklabels, secondary_y=False) # 仅控制左轴标签
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    
    return fig

def create_group_trend_chart(
    df: pd.DataFrame, 
    title: str, 
    show_legend: bool, 
    show_yticklabels: bool, 
    y_range: list, 
    color_map: dict, 
    category_orders_map: dict, 
    warning_line_value: float = None # type: ignore
) -> go.Figure | None:
    """绘制 Group 级堆叠柱状图 (带总计 Scatter 和 spec)"""
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None

    # 确保排序正确
    df = df.copy()
    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)
    
    # 计算总计用于 Label
    total_rates = df.groupby('time_period', observed=False)['defect_rate'].sum().reset_index()
    total_rates.rename(columns={'defect_rate': 'total_defect_rate'}, inplace=True)

    # 1. 基础柱状图
    fig = px.bar(
        df, x='time_period', y='defect_rate', color='defect_group', 
        title=title,
        color_discrete_map=color_map,
        category_orders=category_orders_map,
        labels={"time_period": "时间", "defect_rate": "不良率", "defect_group": "Group"},
        hover_data={'defect_rate': ':.2%'}
    )
    
    # 2. 添加总计数值标签
    fig.add_trace(
        go.Scatter(
            x=total_rates['time_period'], y=total_rates['total_defect_rate'],
            mode='text', text=[f'{rate:.2%}' for rate in total_rates['total_defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False,
        )
    )
    
    # 3. 添加spec
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    # 4. 布局调整
    fig.update_layout(
        yaxis_range=y_range, yaxis_tickformat='.2%', showlegend=show_legend,
        xaxis_title=None, yaxis_title=None, title_font_size=16
    )
    fig.update_yaxes(showticklabels=show_yticklabels)
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    
    return fig

# -----------------------------------------------------------------------------
#  Code 级图表绘制
# -----------------------------------------------------------------------------
def create_code_trend_chart(
    df: pd.DataFrame, 
    title: str, 
    y_range: list, 
    warning_line_value: float = None # type: ignore
) -> go.Figure | None:
    """绘制 Code 级单柱状图 (带数值标签和spec)"""
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None
    
    df = df.copy()
    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)

    # 1. 基础柱状图
    fig = px.bar(
        df, x='time_period', y='defect_rate', title=title,
        labels={"time_period": "时间", "defect_rate": "不良率"}
    )
    
    # 2. 添加数值标签
    fig.add_trace(
        go.Scatter(
            x=df['time_period'], y=df['defect_rate'], mode='text',
            text=[f'{rate:.2%}' for rate in df['defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False
        )
    )
    
    # 3. 添加spec
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    # 4. 布局调整
    fig.update_traces(hovertemplate='<b>%{x}</b><br>不良率: %{y:.2%}', marker_color='#54a24b')
    fig.update_layout(
        yaxis_range=y_range, yaxis_tickformat='.2%', showlegend=False,
        xaxis_title=None, yaxis_title=None, title_font_size=16
    )
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    
    return fig

def create_and_update_chart(df, title, show_legend, show_yticklabels, y_range, color_map, category_orders_map, warning_line_value=None):
    """(已升级) 绘制Group堆叠图，带spec"""
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
            annotation_text=f"spec: {warning_line_value:.2%}", 
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
    """(已升级) 绘制Code单柱图，带spec和柱顶标签"""
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
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    fig.update_traces(hovertemplate='<b>%{x}</b><br>不良率: %{y:.2%}', marker_color='#54a24b')
    fig.update_layout(
        yaxis_range=y_range, yaxis_tickformat='.2%', showlegend=False,
        xaxis_title=None, yaxis_title=None, title_font_size=16
    )
    fig.update_xaxes(type='category', tickangle=-45 if "日度" in title else 0)
    return fig
