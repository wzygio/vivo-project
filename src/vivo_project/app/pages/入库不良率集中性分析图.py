import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- 1. 初始化与配置 ---
from vivo_project.app.setup import AppSetup
from vivo_project.app.components.components import create_code_selection_ui
from vivo_project.core.mapping_processor import apply_hotspot_modification_to_matrix
AppSetup.initialize_app()

from vivo_project.config import CONFIG
from vivo_project.services.workflow_handler import WorkflowHandler

# --- 2. UI 界面布局 ---
st.set_page_config(page_title="不良率趋势分析", layout="wide")
st.title("📊 入库不良率趋势图")

if st.button("🔄 刷新数据"):
    st.cache_data.clear()
    st.rerun()

# --- 3. 数据加载 ---
mwd_group_data = WorkflowHandler.run_mwd_trend_workflow()
mwd_code_data = WorkflowHandler.run_code_level_mwd_trend_workflow()
current_month_trend_data = WorkflowHandler.run_current_month_trend_workflow()
mapping_data_source = WorkflowHandler.run_mapping_data_workflow()
lot_data = WorkflowHandler.run_lot_defect_rate_workflow()

# ==============================================================================
#                      模块3: 按Code查询Lot集中性 (带排序功能图表)
# ==============================================================================
st.header("🔬 ByCode查询Lot集中性")

# 确认我们有正确的数据源
code_details_dict = lot_data.get("lot_code_level_details")

if code_details_dict:
    # 1. 准备数据源
    all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
    # 格式化时间列
    all_codes_df['warehousing_time'] = pd.to_datetime(all_codes_df['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
    all_codes_df['array_input_time'] = pd.to_datetime(all_codes_df['array_input_time'], errors='coerce')
    
    # 3. 调用智能UI组件 (传入已按日期筛选过的数据)
    selected_code_info = create_code_selection_ui(
        source_data=all_codes_df,
        target_defect_groups=CONFIG['processing']['target_defect_groups'],
        key_prefix="lot_focus_table", # 使用唯一的key
        rate_threshold=0.0005
    )

    # 4. 根据组件的选择结果进行后续操作
    if selected_code_info.get("code"):
        selected_code = selected_code_info["code"]
        
        # a. 筛选出所选Code的全部数据
        chart_df_final = all_codes_df[all_codes_df['defect_desc'] == selected_code]

        # b. 排序逻辑
        sort_option = st.selectbox(
            "选择排序方式:",
            options=[
                "默认排序 (按阵列投入时间)", 
                "按入库时间排序",
                "按不良率排序 (从高到低)"
            ],
            key="lot_code_sorter"
        )
        
        if sort_option == "按不良率排序 (从高到低)":
            chart_df_final = chart_df_final.sort_values(by='defect_rate', ascending=False)
            xaxis_label = f"Lot ID (按'{selected_code}'不良率排序)"
        elif sort_option == "按入库时间排序":
            chart_df_final = chart_df_final.sort_values(by='warehousing_time', ascending=True)
            xaxis_label = 'Lot ID (按入库时间排序)'
        else: # 默认排序 (按阵列投入时间)
            chart_df_final = chart_df_final.sort_values(by='array_input_time', ascending=True)
            xaxis_label = 'Lot ID (按阵列投入时间排序)'
        
        sorted_lot_ids = chart_df_final['lot_id'].tolist()
        
        st.markdown(f"#### **{selected_code}** ")
        # c. 绘图逻辑
        fig_lot = px.bar(
            chart_df_final, x='lot_id', y='defect_rate',
            labels={'lot_id': xaxis_label, 'defect_rate': '不良率', 'array_input_time': '阵列投入时间', 'warehousing_time': '入库时间'},
            hover_data={
                "warehousing_time": "|%Y/%m/%d", 
                "array_input_time": "|%Y/%m/%d %H:%M",
                "defect_panel_count": True, 
                "defect_rate": ":.2%"
            },
            height=600, category_orders={"lot_id": sorted_lot_ids}
        )
        fig_lot.update_traces(marker_color='#1f77b4')
        fig_lot.update_layout(yaxis_tickformat='.2%', xaxis_tickangle=-45)
        st.plotly_chart(fig_lot, use_container_width=True)
else:
    st.warning("未能加载Lot的Code级明细数据，无法执行此分析。")

# ==============================================================================
#                      按Code查询Mapping集中性
# ==============================================================================

def parse_panel_id_to_coords(panel_id: str) -> tuple | None:
    """(已修正) Mapping图坐标解析 (21列)"""
    if not isinstance(panel_id, str) or len(panel_id) < 15: return None
    row_code, col_code = panel_id[11:13], panel_id[13:15]
    row_map = {'1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4, '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9}
    col_char = col_code[0]
    col_map_index = ord(col_char) - ord('A')
    row_index = row_map.get(row_code)
    if row_index is not None and 0 <= col_map_index < 21: # 确保使用21列
        return (row_index, col_map_index)
    return None

def create_mapping_heatmap(matrix_df, title, global_max_value):
    """(已修正) 绘制Mapping热图 (21列)"""
    fig = px.imshow(
        matrix_df, text_auto=True, aspect="auto", color_continuous_scale='Reds',
        labels=dict(x="列 (Column)", y="行 (Row)", color="不良数"), title=title,
        zmin=0, zmax=max(1, global_max_value)
    )
    row_labels = ['1A', '1B', '1C', '1D', '1E', '2A', '2B', '2C', '2D', '2E']
    col_labels = [f"{chr(ord('A') + i)}0" for i in range(21)] # 确保使用21列
    fig.update_layout(
        xaxis=dict(tickmode='array', tickvals=list(range(21)), ticktext=col_labels),
        yaxis=dict(tickmode='array', tickvals=list(range(10)), ticktext=row_labels),
        xaxis_side='top', height=450
    )
    fig.update_yaxes(autorange="reversed")
    return fig

st.header("🗺️ 按Code查询Mapping集中性")

if mapping_data_source is not None and not mapping_data_source.empty:
    selected_code_info_mapping = create_code_selection_ui(
        source_data=mapping_data_source,
        target_defect_groups=CONFIG['processing']['target_defect_groups'],
        key_prefix="mapping_focus",
        count_threshold=10
    )
    
    if selected_code_info_mapping.get("code"):
        group, code = selected_code_info_mapping["group"], selected_code_info_mapping["code"]
        st.markdown(f"#### **{code}** ")
        
        df_selected_code = mapping_data_source[
            (mapping_data_source['defect_group'] == group) & 
            (mapping_data_source['defect_desc'] == code)
        ]
        
        sorted_batches_list = sorted(df_selected_code['batch_no'].unique())
        modified_matrices = []
        script_config = CONFIG.get('processing', {}).get('mapping_hotspot_script', {})

        for i, batch_no in enumerate(sorted_batches_list):
            df_batch = df_selected_code[df_selected_code['batch_no'] == batch_no]
            
            coords_batch = df_batch['panel_id'].apply(parse_panel_id_to_coords)
            df_batch_coords = df_batch.assign(row=coords_batch.str[0], col=coords_batch.str[1]).dropna(subset=['row', 'col'])
            df_batch_coords[['row', 'col']] = df_batch_coords[['row', 'col']].astype(int)
            
            heatmap_matrix = pd.pivot_table(df_batch_coords, values='panel_id', index='row', columns='col', aggfunc='count', fill_value=0)
            heatmap_matrix = heatmap_matrix.reindex(index=range(10), columns=range(21), fill_value=0) # 21列
            
            batch_index = 'oldest'
            if i == len(sorted_batches_list) - 1: batch_index = 'latest'
            elif i == len(sorted_batches_list) - 2: batch_index = 'second_latest'
            
            modified_matrix = apply_hotspot_modification_to_matrix(
                heatmap_matrix, batch_no, code, batch_index, script_config
            )
            modified_matrices.append((batch_no, modified_matrix))

        global_max_value = 0
        if modified_matrices:
            for _, matrix in modified_matrices:
                global_max_value = max(global_max_value, matrix.max().max())
        
        for batch_no, matrix in modified_matrices:
            fig = create_mapping_heatmap(matrix, title=f"批次: {batch_no}", global_max_value=global_max_value)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("未能加载用于Mapping图的数据 (最近批次无不良)，请检查后台。")

st.divider()
