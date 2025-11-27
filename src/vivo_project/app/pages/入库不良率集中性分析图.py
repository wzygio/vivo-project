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
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
AppSetup.initialize_app()

from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui, render_page_header

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📊 入库不良率集中性分析图")

# --- 3. 数据加载 ---
lot_data = YieldAnalysisService.get_lot_defect_rates()
sheet_data = YieldAnalysisService.get_sheet_defect_rates()
mapping_data_source = YieldAnalysisService.get_mapping_data()

COLOR_MAP = {
    'Array_Line': "#1930ff",  # Plotly默认的蓝色
    'OLED_Mura': "#ff2828",   # Plotly默认的红色
    'Array_Pixel': "#6fb9ff",   # Plotly默认的浅蓝色
    'array_Line_rate': "#1930ff",  # Plotly默认的蓝色
    'oled_mura_rate': "#ff2828",   # Plotly默认的红色
    'array_pixel_rate': "#6fb9ff"   # Plotly默认的浅蓝色
}

# [可选] 定义 Group 清理函数
def clean_group_name(name):
    return name.replace('_rate', '').replace('array_', 'Array_').replace('oled_mURA', 'OLED_Mura')

def is_valid_data(data):
    """检查数据是否有效（不为None，且如果是DataFrame则不为空）"""
    return data is not None and (not isinstance(data, pd.DataFrame) or not data.empty)
if not all(map(is_valid_data, [mapping_data_source, lot_data, sheet_data])):
    st.info("数据已过期，请点击\"🔄 刷新数据\"按钮重新加载")
    sys.exit(1)

# ==============================================================================
#                      按Code查询Lot集中性 (带排序功能图表)
# ==============================================================================
st.header("🔬 ByCode查询Lot集中性")

# 确认我们有正确的数据源
if lot_data and lot_data.get("code_level_details") is not None:

    code_details_dict = lot_data.get("code_level_details")

    # 1. 准备数据源
    all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True) # type: ignore
    # 格式化时间列
    all_codes_df['warehousing_time'] = pd.to_datetime(all_codes_df['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
    all_codes_df['array_input_time'] = pd.to_datetime(all_codes_df['array_input_time'], errors='coerce')
    
    # 3. 调用智能UI组件 (传入已按日期筛选过的数据)
    selected_code_info = create_code_selection_ui(
        source_data=all_codes_df,
        target_defect_groups=CONFIG['processing']['target_defect_groups'],
        key_prefix="lot_focus_table", # 使用唯一的key
        rate_threshold=0.0002
    )

    # 4. 根据组件的选择结果进行后续操作
    if selected_code_info.get("code"):
        selected_code = selected_code_info["code"]
        
        # a. 筛选出所选Code的全部数据
        chart_df_final = all_codes_df[all_codes_df['defect_desc'] == selected_code].copy()

        # --- [新增] 数据预处理：生成月别和带日期的周别标签 (周一~周日) ---
        def get_week_label(date_obj):
            if pd.isna(date_obj): return "Unknown"
            # isocalendar() 标准：周一为1，周日为7，完全符合您的需求
            iso_year, iso_week, _ = date_obj.isocalendar()
            # 计算该周的周一 (1) 和 周日 (7)
            monday = datetime.fromisocalendar(iso_year, iso_week, 1).date()
            sunday = datetime.fromisocalendar(iso_year, iso_week, 7).date()
            return f"{iso_year}-W{iso_week:02d} ({monday.strftime('%m/%d')}-{sunday.strftime('%m/%d')})"

        # 生成辅助列
        chart_df_final['month_str'] = chart_df_final['warehousing_time'].apply(lambda x: x.strftime('%Y-%m') if pd.notnull(x) else "Unknown")
        chart_df_final['week_label'] = chart_df_final['warehousing_time'].apply(get_week_label)

        # --- [新增] UI 布局与筛选逻辑 ---
        st.markdown(f"#### **{selected_code}** ")
        
        f_col1, f_col2, f_col3 = st.columns(3)

        # 1. 排序方式
        with f_col1:
            sort_option = st.selectbox(
                "1. 选择排序方式:",
                options=[
                    "默认排序 (按阵列投入时间)", 
                    "按入库时间排序",
                    "按不良率排序 (从高到低)"
                ],
                key="lot_code_sorter"
            )

        # 2. 月别筛选 (选项源自全量数据)
        with f_col2:
            available_months = sorted(chart_df_final['month_str'].unique().tolist(), reverse=True)
            selected_month = st.selectbox(
                "2. 月别筛选:",
                options=["全部月份"] + available_months,
                key="lot_code_month_filter"
            )

        # 3. 周别筛选 (取消联动：选项源自全量数据 chart_df_final，而非过滤后的数据)
        with f_col3:
            available_weeks = sorted(chart_df_final['week_label'].unique().tolist(), reverse=True)
            selected_week = st.selectbox(
                "3. 周别筛选:",
                options=["全部周"] + available_weeks,
                key="lot_code_week_filter"
            )

        # --- 应用筛选逻辑 (独立叠加) ---
        df_filtered = chart_df_final.copy()

        # 应用月筛选
        if selected_month != "全部月份":
            df_filtered = df_filtered[df_filtered['month_str'] == selected_month]
        
        # 应用周筛选 (注意：如果月和周选的时间段不重合，这里数据会变为空，这是正常的逻辑结果)
        if selected_week != "全部周":
            df_filtered = df_filtered[df_filtered['week_label'] == selected_week]

        # --- 排序逻辑 ---
        if sort_option == "按不良率排序 (从高到低)":
            df_filtered = df_filtered.sort_values(by='defect_rate', ascending=False)
            xaxis_label = f"Lot ID"
        elif sort_option == "按入库时间排序":
            df_filtered = df_filtered.sort_values(by='warehousing_time', ascending=True)
            xaxis_label = 'Lot ID'
        else: # 默认排序
            df_filtered = df_filtered.sort_values(by='array_input_time', ascending=True)
            xaxis_label = 'Lot ID'
        
        sorted_lot_ids = df_filtered['lot_id'].tolist()
        
        # 绘图判断
        if df_filtered.empty:
            st.warning("当前筛选条件组合下无数据 (例如：选中的月份不包含选中的周)。")
        else:
            # c. 绘图逻辑
            fig_lot = px.bar(
                df_filtered, x='lot_id', y='defect_rate',
                labels={
                    'lot_id': xaxis_label, 
                    'defect_rate': '不良率', 
                    'array_input_time': '阵列投入时间', 
                    'warehousing_time': '入库时间', 
                    'defect_panel_count': '不良panel数',
                    'week_label': '周别'},
                hover_data={
                    "warehousing_time": "|%Y/%m/%d", 
                    "array_input_time": "|%Y/%m/%d %H:%M",
                    "defect_panel_count": True, 
                    "defect_rate": ":.2%",
                    "week_label": True
                },
                height=600, category_orders={"lot_id": sorted_lot_ids}
            )
            fig_lot.update_traces(marker_color='#1f77b4')
            fig_lot.update_layout(yaxis_tickformat='.2%', xaxis_tickangle=-45)
            st.plotly_chart(fig_lot, use_container_width=True)
else:
    st.warning("未能加载Lot的Code级明细数据，无法执行此分析。")

st.divider()

# ==============================================================================
#                      Sheet 集中性查询 (按 Lot) - Group 堆叠图
# ==============================================================================
st.header("🔬 ByLot查询Sheet不良率分布")

# --- 1. 检查 Sheet 级别数据 ---
if sheet_data and sheet_data.get("group_level_summary_for_table") is not None:
    
    group_summary_df = sheet_data["group_level_summary_for_table"].copy()
    
    # 2. 格式化时间列
    try:
        group_summary_df['warehousing_time'] = pd.to_datetime(group_summary_df['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
        group_summary_df['array_input_time'] = pd.to_datetime(group_summary_df['array_input_time'], errors='coerce')
        # 计算总不良率用于排序
        rate_cols_raw = [col for col in group_summary_df.columns if col.endswith('_rate') and col != 'pass_rate']
        group_summary_df['total_defect_rate'] = group_summary_df[rate_cols_raw].sum(axis=1)
        
        available_lots = set(group_summary_df['lot_id'].unique())
        if not available_lots:
             raise ValueError("Sheet Group 汇总数据中没有有效的 Lot ID。")

    except Exception as e:
        st.error(f"准备 Sheet 汇总数据时出错: {e}")
        group_summary_df = pd.DataFrame()

    if not group_summary_df.empty:
        col1, col2, col3 = st.columns([1, 1, 1])

        # 在第一列放置输入框
        with col1:
            selected_lot = st.text_input(
                "请先输入要查询的 Lot ID:",
                key="sheet_focus_lot_input"
            )

        if selected_lot:
            if selected_lot not in available_lots:
                st.warning(f"输入的 Lot ID '{selected_lot}' 在 Sheet 数据中不存在。")
            else:
                # 4. 筛选出该 Lot 的所有 Sheet 数据 (宽表)
                df_lot_wide = group_summary_df[group_summary_df['lot_id'] == selected_lot]

                # 5. 排序逻辑
                sort_option_sheet = st.selectbox(
                    "选择 Sheet 排序方式:",
                    options=[
                        "默认排序 (按阵列投入时间)",
                        "按入库时间排序",
                        "按总不良率排序 (从高到低)" # <-- 修改为总不良率
                    ],
                    key="sheet_group_sorter"
                )

                if sort_option_sheet == "按总不良率排序 (从高到低)":
                    df_lot_wide = df_lot_wide.sort_values(by='total_defect_rate', ascending=False)
                    xaxis_label_sheet = f"Sheet ID"
                elif sort_option_sheet == "按入库时间排序":
                    df_lot_wide = df_lot_wide.sort_values(by='warehousing_time', ascending=True)
                    xaxis_label_sheet = 'Sheet ID'
                else: # 默认排序 (按阵列投入时间)
                    df_lot_wide = df_lot_wide.sort_values(by='array_input_time', ascending=True)
                    xaxis_label_sheet = 'Sheet ID'
                
                sorted_sheet_ids = df_lot_wide['sheet_id'].tolist()
                
                # 6. [核心] 将宽表 Melt 为长表以供堆叠
                id_cols = ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'total_defect_rate']
                # 自动选择所有以 _rate 结尾的列 (排除 pass_rate 和 total_defect_rate)
                value_cols = [col for col in df_lot_wide.columns if col.endswith('_rate') and col not in ['pass_rate', 'total_defect_rate']]
                
                df_melted = df_lot_wide.melt(
                    id_vars=id_cols,
                    value_vars=value_cols,
                    var_name='defect_group', # 列名变为 'defect_group'
                    value_name='defect_rate'  # 值变为 'defect_rate'
                )
                
                # [可选] 清理 defect_group 名称 (例如 'array_pixel_rate' -> 'Array_Pixel')
                df_melted['defect_group'] = df_melted['defect_group'].str.replace('_rate', '').str.replace('array_', 'Array_').replace('oled_mura', 'OLED_Mura')
                
                st.markdown(f"#### Lot **{selected_lot}** - Sheet 级 Group 不良率堆叠图")

                # 7. 绘图
                fig_sheet_stack = px.bar(
                    df_melted,
                    x='sheet_id',
                    y='defect_rate',
                    color='defect_group', # <--- 按 Group 堆叠
                    labels={'sheet_id': xaxis_label_sheet, 'defect_rate': '不良率', 'defect_group': '不良Group'},
                    hover_data={ # hover_data 作用于长表
                        "defect_rate": ":.2%",
                        "defect_group": True,
                        "array_input_time": "|%Y-%m-%d"  # <--- 添加阵列投入时间
                    },
                    height=600,
                    category_orders={"sheet_id": sorted_sheet_ids}, # <--- 保持排序
                    color_discrete_map=COLOR_MAP # <--- 应用颜色映射
                )
                
                # 添加总计标签 (从宽表获取)
                df_lot_wide_for_text = df_lot_wide.drop_duplicates(subset=['sheet_id'])
                fig_sheet_stack.add_trace(
                    go.Scatter(
                        x=df_lot_wide_for_text['sheet_id'], 
                        y=df_lot_wide_for_text['total_defect_rate'],
                        mode='text', 
                        text=[f'{r:.2%}' for r in df_lot_wide_for_text['total_defect_rate']],
                        textposition='top center', 
                        textfont=dict(color='black', size=10), 
                        showlegend=False,
                    )
                )
                
                fig_sheet_stack.update_layout(
                    yaxis_tickformat='.2%', 
                    xaxis_tickangle=-90,
                    barmode='stack' # 确保是堆叠模式
                )
                st.plotly_chart(fig_sheet_stack, use_container_width=True)

else:
    st.warning("未能加载Sheet的Group级汇总数据，无法执行此分析。")

st.divider()

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
    if row_index is not None and 0 <= col_map_index < 19: # 确保使用21列
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
    col_labels = [f"{chr(ord('A') + i)}0" for i in range(19)] # 确保使用21列
    fig.update_layout(
        xaxis=dict(tickmode='array', tickvals=list(range(19)), ticktext=col_labels),
        yaxis=dict(tickmode='array', tickvals=list(range(10)), ticktext=row_labels),
        xaxis_side='top', height=450
    )
    fig.update_yaxes(autorange="reversed")
    return fig

st.header("🗺️ ByCode查询Mapping集中性")

if mapping_data_source is not None and not mapping_data_source.empty:
    selected_code_info_mapping = create_code_selection_ui(
        source_data=mapping_data_source,
        target_defect_groups=CONFIG['processing']['target_defect_groups'],
        key_prefix="mapping_focus",
        filter_by='occurrence',          # <--- 明确指定按 count 筛选
        count_threshold=10           # <--- 这个阈值现在会作用于 Panel 出现次数
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
            heatmap_matrix = heatmap_matrix.reindex(index=range(10), columns=range(19), fill_value=0) # 21列
            
            batch_index = 'middle'
            if i == 0: 
                batch_index = 'oldest'
            elif i == len(sorted_batches_list) - 1: 
                batch_index = 'latest'
            
            # modified_matrix = apply_hotspot_modification_to_matrix(
            #     heatmap_matrix, batch_no, code, batch_index, script_config
            # )
            # modified_matrices.append((batch_no, modified_matrix))
            modified_matrices.append((batch_no, heatmap_matrix))

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
