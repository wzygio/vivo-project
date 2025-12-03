import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


def create_lot_chart(df, xaxis_label, sorted_lot_ids, warning_line_value=None):
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
            annotation_text=f"",
            # annotation_text=f"警戒线: {warning_line_value:.2%}",
            annotation_position="bottom right",
            annotation_font_color="red"
        )
    
    fig.update_layout(
        yaxis_tickformat='.2%',
        xaxis_tickangle=-45,
        clickmode='event+select'
    )
    
    return fig
