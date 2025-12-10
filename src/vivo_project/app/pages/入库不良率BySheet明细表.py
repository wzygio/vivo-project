# src/vivo_project/app/pages/入库不良率BySheet明细表.py
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup

@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui, render_page_header
from vivo_project.app.charts.sheet_details_chart import render_lot_id_filter, render_sheet_id_query

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📈 入库不良率BySheet明细表")

# --- 3. 加载数据 ---
all_data = YieldAnalysisService.get_sheet_defect_rates()

# ==============================================================================
#                      --- 模块1: Group不良率明细表 (By Sheet) ---
# ==============================================================================
if all_data:
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Group级别数据，请检查后台。")
        st.stop()
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(
        group_summary_df_full['warehousing_time'], 
        format='%Y%m%d', 
        errors='coerce'
    ).dt.date
    
    st.markdown("### 📄 Group不良率明细表（By Sheet）")

    col1, col2, col3 = st.columns(3)

    with col1:
        start_date = st.date_input("起始日期", value=(datetime.now().date() - relativedelta(months=3)))
    with col2:
        end_date = st.date_input("结束日期", value=datetime.now().date())
    
    df_filtered_by_date = group_summary_df_full[
        (group_summary_df_full['warehousing_time'] >= start_date) &
        (group_summary_df_full['warehousing_time'] <= end_date)
    ]

    with col3:
        lot_ids_in_range = set(df_filtered_by_date['lot_id'].unique())
        final_filtered_df = render_lot_id_filter(df_filtered_by_date, lot_ids_in_range)

    if final_filtered_df.empty:
        st.warning("在您选择的筛选条件下没有数据。")
    else:
        # 准备用于显示的副本 (乘以100)
        df_for_display_group = final_filtered_df.copy()
        rate_columns_to_convert = ["pass_rate", "array_pixel_rate", "array_line_rate", "oled_mura_rate"]
        
        for col in rate_columns_to_convert:
            if col in df_for_display_group.columns:
                df_for_display_group[col] = df_for_display_group[col] * 100
        
        st.dataframe(
            df_for_display_group,
            column_config={
                "sheet_id": st.column_config.TextColumn("Sheet ID"),
                "lot_id": st.column_config.TextColumn("Lot ID"),
                "warehousing_time": st.column_config.DateColumn("入库时间", format="YYYY/MM/DD"),
                "array_input_time": st.column_config.DatetimeColumn("阵列投入时间", format="YYYY/MM/DD"),
                "pass_rate": st.column_config.NumberColumn("过货率", format="%.2f%%"),
                "array_pixel_rate": st.column_config.NumberColumn("Array_Pixel不良率", format="%.2f%%"),
                "array_line_rate": st.column_config.NumberColumn("Array_Line不良率", format="%.2f%%"),
                "oled_mura_rate": st.column_config.NumberColumn("OLED_Mura不良率", format="%.2f%%"),
            },
            column_order=[
                "sheet_id", "lot_id", "warehousing_time", "array_input_time", "pass_rate",
                "array_pixel_rate", "array_line_rate", "oled_mura_rate"
            ],
            hide_index=True,
            use_container_width=True
        )
    st.divider()

    # ==============================================================================
    #                      --- 模块2: 按Sheet ID查询Code级别详情 ---
    # ==============================================================================
    st.markdown("### ✍️ By Sheet ID查询Code不良率")
    
    sheet_ids = final_filtered_df['sheet_id'].unique()
    
    if len(sheet_ids) > 0:
        code_details_dict = all_data.get("code_level_details")
        if code_details_dict is None:
            st.error("未能加载Code级别明细数据。")
            st.stop()
            
        render_sheet_id_query(sheet_ids, code_details_dict)
    else:
        st.info("在当前筛选条件下无Sheet可供查询。")

    st.divider()

# ==============================================================================
#                      --- 模块3: 按Code查询Sheet集中性 (Top 20) ---
# ==============================================================================
    st.header("🔬 ByCode查询Sheet集中性")
    
    code_details_dict = all_data.get("code_level_details")

    if code_details_dict:
        # 1. 准备数据源
        all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
        
        # 2. [关键] 预先筛选数据源，使其与主表保持一致
        # 'sheet_ids' 是在模块2开头从 'final_filtered_df' 中获取的
        df_in_scope = all_codes_df[all_codes_df['sheet_id'].isin(sheet_ids)]

        # 3. 调用智能UI组件
        selected_code_info = create_code_selection_ui(
            source_data=df_in_scope,
            target_defect_groups=CONFIG['processing']['target_defect_groups'],
            key_prefix="sheet_focus",  # 唯一的key
            rate_threshold=0.0005 # 沿用我们之前设置的阈值
        )

        # 4. 根据组件的选择结果进行后续操作
        if selected_code_info.get("code"):
            group = selected_code_info["group"]
            code = selected_code_info["code"]
            
            st.markdown(f"#### 不良Code **'{code}'** 的Top 20问题Sheet")
            
            # 从【已在范围内的】数据中筛选
            result_df = df_in_scope[
                (df_in_scope['defect_group'] == group) & 
                (df_in_scope['defect_desc'] == code)
            ]
            
            # 排序并取前20
            top_20_sheets = result_df.sort_values(by="defect_rate", ascending=False).head(20)
            
            if not top_20_sheets.empty:
                # 准备用于显示的数据
                df_for_display = top_20_sheets.copy()
                df_for_display['defect_rate'] *= 100
                
                st.dataframe(
                    df_for_display.reset_index(drop=True),
                    column_config={
                        "sheet_id": st.column_config.TextColumn("Sheet ID"),
                        "lot_id": st.column_config.TextColumn("Lot ID"),
                        "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                        "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                        "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                    },
                    column_order=("sheet_id", "lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("在当前筛选条件下，没有找到该Code对应的不良Sheet。")
    else:
        st.warning("未能加载Code级明细数据，无法执行此分析。")

else:
    st.error("未能从后台加载Sheet数据，请检查后台日志或刷新重试。")