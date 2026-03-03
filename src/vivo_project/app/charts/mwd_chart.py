import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Optional
from datetime import datetime, timedelta


# -----------------------------------------------------------------------------
#  绘图辅助函数
# -----------------------------------------------------------------------------
# --- [新增] 图表数据空洞补齐函数 ---
def pad_chart_dataframe(df: pd.DataFrame, title: str, group_col: str = None) -> pd.DataFrame: # type: ignore
    """
    智能推断并补齐 DataFrame 中缺失的时间周期，填充 0 值。
    解决“入库为0的日期在图表中被跳过而不是下沉为0”的问题。
    """
    if df is None or df.empty:
        return df
    
    df_padded = df.copy()
    periods = df_padded['time_period'].unique()
    if len(periods) <= 1:
        return df_padded
        
    full_periods = []
    
    try:
        if "日度" in title:
            # 解析 %m-%d 格式
            year = datetime.now().year
            dates = [datetime.strptime(f"{year}-{p}", "%Y-%m-%d") for p in periods]
            min_d, max_d = min(dates), max(dates)
            while min_d <= max_d:
                full_periods.append(min_d.strftime("%m-%d"))
                min_d += timedelta(days=1)
                
        elif "月度" in title:
            # 解析 YYYY-MM月 格式
            dates = [datetime.strptime(p, "%Y-%m月") for p in periods]
            min_d, max_d = min(dates), max(dates)
            while min_d <= max_d:
                full_periods.append(min_d.strftime("%Y-%m月"))
                # 月份加1
                if min_d.month == 12:
                    min_d = min_d.replace(year=min_d.year+1, month=1)
                else:
                    min_d = min_d.replace(month=min_d.month+1)
        else:
            return df_padded # 周度补齐涉及跨年 ISO 历法，保持现状较安全
        
        # 找出缺失的 periods
        missing_periods = [p for p in full_periods if p not in periods]
        if not missing_periods:
            return df_padded
            
        new_rows = []
        groups = df_padded[group_col].unique() if group_col and group_col in df_padded.columns else [None]
        
        # 填充 0 值行
        for p in missing_periods:
            for g in groups:
                row = {'time_period': p, 'defect_rate': 0.0, 'total_panels': 0}
                if g is not None:
                    row[group_col] = g
                new_rows.append(row)
                
        if new_rows:
            df_padded = pd.concat([df_padded, pd.DataFrame(new_rows)], ignore_index=True)
            # ✅ [关键修复] 必须进行物理排序，否则 Plotly 的折线会来回乱穿插
            df_padded = df_padded.sort_values(by='time_period').reset_index(drop=True)
            
    except Exception as e:
        # 解析失败则退回原状，不影响页面渲染
        pass
        
    return df_padded

def slice_recent_data(df, n_recent=3, time_col='time_period'):
    """保留 DataFrame 中 time_col 列最近的 n_recent 个唯一值对应的数据"""
    if df is None or df.empty:
        return df
        
    # [核心修复 1]：剔除缺失值 (NaN)，防止其作为 float 与 str 混合排序时引发 TypeError
    df_clean = df.dropna(subset=[time_col]).copy()
    if df_clean.empty:
        return df_clean
        
    # [核心修复 2]：强制转为字符串并排序，提供双重类型保险
    unique_periods = sorted(df_clean[time_col].astype(str).unique())
    
    if len(unique_periods) > n_recent:
        recent_periods = unique_periods[-n_recent:]
        return df_clean[df_clean[time_col].astype(str).isin(recent_periods)]
        
    return df_clean

# -----------------------------------------------------------------------------
#  Group 级图表绘制
# -----------------------------------------------------------------------------
def create_group_trend_chart(
    df: pd.DataFrame, 
    title: str, 
    show_legend: bool, 
    show_yticklabels: bool, 
    y_range: list, 
    color_map: dict, 
    category_orders_map: dict, 
    warning_line_value: Optional[float] = None,
    show_input_count: bool = False # [新增] 控制开关
) -> go.Figure | None:
    """
    [全能版 V2] 绘制 Group 级堆叠柱状图
    修复了 Pylance 类型检查报错，改为直接调用 update_layout。
    """
    if df is None or df.empty:
        st.info(f"无 {title.replace('趋势','')} 数据。")
        return None

    # 确保排序正确
    df = pad_chart_dataframe(df, title, group_col='defect_group')
    df = df.copy()
    df['time_period'] = pd.Categorical(df['time_period'], categories=sorted(df['time_period'].unique()), ordered=True)
    
    # 计算总计用于 Label
    total_rates = df.groupby('time_period', observed=False)['defect_rate'].sum().reset_index()
    total_rates.rename(columns={'defect_rate': 'total_defect_rate'}, inplace=True)

    # --- [准备] 检查是否有入库量数据 ---
    has_panel_count = 'total_panels' in df.columns
    df_panels = None
    if has_panel_count:
        df_panels = df[['time_period', 'total_panels']].drop_duplicates().sort_values('time_period')

    # --- 1. 基础柱状图 (左轴) ---
    fig = px.bar(
        df, x='time_period', y='defect_rate', color='defect_group', 
        title=title,
        color_discrete_map=color_map,
        category_orders=category_orders_map,
        labels={"time_period": "时间", "defect_rate": "不良率", "defect_group": "Group"},
        hover_data={'defect_rate': ':.2%', 'total_panels': True} if has_panel_count else {'defect_rate': ':.2%'} 
    )
    
    # --- 2. [可选] 添加入库量折线 (右轴) ---
    if show_input_count and has_panel_count and df_panels is not None:
        fig.add_trace(
            go.Scatter(
                x=df_panels['time_period'], 
                y=df_panels['total_panels'],
                name='入库数',
                # [修改点 1] 模式增加 'text'，表示要显示文本
                mode='lines+markers+text',
                # [修改点 2] 指定要显示的文本内容（直接绑定入库数列）
                text=df_panels['total_panels'],
                # [修改点 3] 文本显示位置（top center 表示显示在数据点上方，避免遮挡线条）
                textposition='top center',
                # [修改点 4] (可选) 文本格式模板，例如只显示数字，或者加单位
                # texttemplate='%{y}', 
                
                yaxis='y2', # 保持右轴逻辑不变
                line=dict(color='#7f7f7f', width=1.5, dash='dot'),
                marker=dict(symbol='circle', size=5, color='#7f7f7f'),
                hovertemplate='入库数: %{y}<extra></extra>',
                showlegend=False
            )
        )

    # --- 3. 添加总计数值标签 ---
    fig.add_trace(
        go.Scatter(
            x=total_rates['time_period'], y=total_rates['total_defect_rate'],
            mode='text', text=[f'{rate:.2%}' for rate in total_rates['total_defect_rate']],
            textposition='top center', textfont=dict(color='black', size=10), showlegend=False,
        )
    )
    
    # --- 4. 添加 Spec ---
    if warning_line_value is not None and warning_line_value > 0:
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    # --- 5. 布局调整 (直接调用，修复类型报错) ---
    # 先应用通用设置
    fig.update_layout(
        yaxis_range=y_range, 
        yaxis_tickformat='.2%', 
        showlegend=show_legend,
        xaxis_title=None, 
        yaxis_title=None, 
        title_font_size=16
    )

    # 再单独应用右轴设置 (如果需要)
    if show_input_count and has_panel_count:
        fig.update_layout(
            yaxis2=dict(
                title=None, 
                overlaying='y', 
                side='right', 
                showgrid=False, 
                showticklabels=False, 
                rangemode='tozero'
            )
        )

    fig.update_yaxes(showticklabels=show_yticklabels, secondary_y=False)
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
    
    df = pad_chart_dataframe(df, title, group_col='defect_group')
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
    if warning_line_value is not None:  # 去掉 >0 的硬性限制
        fig.add_hline(
            y=warning_line_value, line_dash="dash", line_color="red", line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}", 
            annotation_position="bottom right", annotation_font_color="red"
        )
    
    # --- [关键修改] 动态调整 Y 轴范围 ---
    # 确保 Y 轴上限至少能覆盖规格线的 1.1 倍，防止标签被切掉
    final_y_max = y_range[1]
    if warning_line_value is not None:
        final_y_max = max(final_y_max, warning_line_value * 1.1)

    # 4. 布局调整
    fig.update_traces(hovertemplate='<b>%{x}</b><br>不良率: %{y:.2%}', marker_color='#54a24b')
    fig.update_layout(
        yaxis_range=[0, final_y_max],  # 使用计算后的范围
        yaxis_tickformat='.2%', 
        showlegend=False,
        xaxis_title=None, 
        yaxis_title=None, 
        title_font_size=16
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

@st.cache_data(ttl="1h")
def prepare_union_data_for_filter(
    mwd_data: dict, 
    lot_data: dict, 
    mapping_data: pd.DataFrame
) -> pd.DataFrame:
    """
    [核心策略]：并集筛选 (Union Strategy)
    分别从 Trend, Lot, Mapping 中提取满足各自门槛的 Code，合并为一个主表。
    用于欺骗筛选器组件，使其能同时展示所有维度的关注点。
    """
    candidates = {} # {(group, code): max_rate}

    # 1. 提取 Trend 候选者 (门槛 > 0.01%)
    # mwd_data 是 dict {'monthly': df, ...}
    if mwd_data:
        trend_df = pd.concat([df for df in mwd_data.values() if df is not None], ignore_index=True)
        if not trend_df.empty:
            # 按 Code 分组取最大不良率
            valid_trend = trend_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].max()
            valid_trend = valid_trend[valid_trend > 0.0001] # 0.01%
            for (grp, code), rate in valid_trend.items():
                candidates[(grp, code)] = max(candidates.get((grp, code), 0), rate)

    # 2. 提取 Lot 候选者 (门槛 > 0.02%)
    # lot_data['code_level_details'] 是 dict {group: df}
    if lot_data and lot_data.get('code_level_details'):
        lot_dfs = lot_data['code_level_details'].values()
        if lot_dfs:
            lot_full = pd.concat(lot_dfs, ignore_index=True)
            if not lot_full.empty:
                valid_lot = lot_full.groupby(['defect_group', 'defect_desc'])['defect_rate'].max()
                valid_lot = valid_lot[valid_lot > 0.0002] # 0.02%
                for (grp, code), rate in valid_lot.items():
                    candidates[(grp, code)] = max(candidates.get((grp, code), 0), rate)

    # 3. 提取 Mapping 候选者 (门槛 > 10 count)
    if mapping_data is not None and not mapping_data.empty:
        # Mapping 只有 count，没有 rate
        counts = mapping_data.groupby(['defect_group', 'defect_desc']).size()
        valid_map = counts[counts > 10]
        for (grp, code), _ in valid_map.items(): # type: ignore
            # 如果该 Code 仅在 Mapping 中出现，给一个默认权重以便排序
            # 如果它也在 Trend/Lot 中，保留原有的 rate
            if (grp, code) not in candidates:
                candidates[(grp, code)] = 0.0001 

    # 4. 构建最终 DataFrame
    if not candidates:
        return pd.DataFrame(columns=['defect_group', 'defect_desc', 'defect_rate'])
    
    rows = [{'defect_group': k[0], 'defect_desc': k[1], 'defect_rate': v} for k, v in candidates.items()]
    return pd.DataFrame(rows)