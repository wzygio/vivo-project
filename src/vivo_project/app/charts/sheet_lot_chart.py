import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# -----------------------------------------------------------------------------
#  Lot 级图表 (ByCode 查询 Lot 集中性)
# -----------------------------------------------------------------------------
def create_lot_defect_chart(df, xaxis_label, sorted_lot_ids, warning_line_value=None):
    """创建Lot集中性图表"""
    fig = px.bar(
        df, x='lot_id', y='defect_rate',
        labels={
            'lot_id': xaxis_label,
            'defect_rate': '不良率',
            'array_input_time': '阵列投入时间',
            'warehousing_time': '入库时间',
            'defect_panel_count': '不良panel数',
            'week_label': '周别'
        },
        hover_data={
            "warehousing_time": "|%Y/%m/%d",
            "array_input_time": "|%Y/%m/%d %H:%M",
            "defect_panel_count": True,
            "defect_rate": ":.2%",
            "week_label": True
        },
        height=600,
        category_orders={"lot_id": sorted_lot_ids}
    )
    
    fig.update_traces(marker_color='#1f77b4')
    
    # 添加警戒线（如果提供）
    if warning_line_value is not None:
        fig.add_hline(
            y=warning_line_value,
            line_dash="dash",
            line_color="red",
            line_width=2,
            annotation_text=f"spec: {warning_line_value:.2%}",
            annotation_position="bottom right",
            annotation_font_color="red"
        )
    
    fig.update_layout(
        yaxis_tickformat='.2%',
        xaxis_tickangle=-45,
        clickmode='event+select'
    )
    
    return fig


# -----------------------------------------------------------------------------
#  Sheet 级图表 (ByLot 查询 Sheet 堆叠图)
# -----------------------------------------------------------------------------
def create_sheet_stack_chart(
    df_wide: pd.DataFrame, 
    xaxis_label: str, 
    sorted_sheet_ids: list, 
    color_map: dict
) -> go.Figure:
    """
    接收 Sheet 宽表，内部执行 Melt 操作并绘制堆叠图。
    """
    # 1. 准备长表 (Melt)
    id_cols = ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'total_defect_rate']
    value_cols = [col for col in df_wide.columns if col.endswith('_rate') and col not in ['pass_rate', 'total_defect_rate']]
    
    df_melted = df_wide.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name='defect_group',
        value_name='defect_rate'
    )
    
    # 清理 Group 名称
    df_melted['defect_group'] = df_melted['defect_group'].str.replace('_rate', '').str.replace('array_', 'Array_').replace('oled_mura', 'OLED_Mura')
    
    # 2. 绘图
    fig = px.bar(
        df_melted,
        x='sheet_id',
        y='defect_rate',
        color='defect_group',
        labels={'sheet_id': xaxis_label, 'defect_rate': '不良率', 'defect_group': '不良Group'},
        hover_data={
            "defect_rate": ":.2%",
            "defect_group": True,
            "array_input_time": "|%Y-%m-%d"
        },
        height=600,
        category_orders={"sheet_id": sorted_sheet_ids},
        color_discrete_map=color_map
    )
    
    # 3. 添加总计数值标签 (Trace)
    df_wide_text = df_wide.drop_duplicates(subset=['sheet_id'])
    fig.add_trace(
        go.Scatter(
            x=df_wide_text['sheet_id'], 
            y=df_wide_text['total_defect_rate'],
            mode='text', 
            text=[f'{r:.2%}' for r in df_wide_text['total_defect_rate']],
            textposition='top center', 
            textfont=dict(color='black', size=10), 
            showlegend=False,
        )
    )
    
    fig.update_layout(
        yaxis_tickformat='.2%', 
        xaxis_tickangle=-90,
        barmode='stack'
    )
    return fig

# -----------------------------------------------------------------------------
#  Mapping 级图表 (热力图与坐标解析)
# -----------------------------------------------------------------------------
def parse_panel_id_to_coords(panel_id: str) -> tuple | None:
    """解析 Panel ID 为 (row, col) 坐标"""
    if not isinstance(panel_id, str) or len(panel_id) < 15: return None
    row_code, col_code = panel_id[11:13], panel_id[13:15]
    row_map = {'1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4, '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9}
    
    try:
        col_char = col_code[0]
        col_map_index = ord(col_char) - ord('A')
        row_index = row_map.get(row_code)
        if row_index is not None and 0 <= col_map_index < 19:
            return (row_index, col_map_index)
    except:
        return None
    return None

def create_mapping_heatmap(matrix_df: pd.DataFrame, title: str, global_max_value: int) -> go.Figure:
    """绘制 Mapping 热力图"""
    fig = px.imshow(
        matrix_df, text_auto=True, aspect="auto", color_continuous_scale='Reds',
        labels=dict(x="列 (Column)", y="行 (Row)", color="不良数"), title=title,
        zmin=0, zmax=max(1, global_max_value)
    )
    row_labels = ['1A', '1B', '1C', '1D', '1E', '2A', '2B', '2C', '2D', '2E']
    col_labels = [f"{chr(ord('A') + i)}0" for i in range(19)]
    
    fig.update_layout(
        xaxis=dict(tickmode='array', tickvals=list(range(19)), ticktext=col_labels),
        yaxis=dict(tickmode='array', tickvals=list(range(10)), ticktext=row_labels),
        xaxis_side='top', height=450
    )
    fig.update_yaxes(autorange="reversed")
    return fig


