# src/vivo_project/app/pages/入库不良率ByLot明细表.py

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

from vivo_project.application.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui, render_page_header
# [新增] 引入筛选器辅助函数
from vivo_project.app.charts.sheet_details_chart import render_lot_id_filter

# --- 2. UI 界面布局 ---
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📋 入库不良率ByLot明细表")


# --- 3. 加载数据 ---
all_data = YieldAnalysisService.get_lot_defect_rates()


if all_data:
    # ==============================================================================
    #                      --- 模块1: Group不良率明细表 (By Lot) ---
    # ==============================================================================
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Lot级别数据，请检查后台。")
        st.stop()
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(group_summary_df_full['warehousing_time'], format='%Y%m%d').dt.date
    
    st.markdown("### 📅 筛选条件") # 修改标题
    today = datetime.now().date()
    three_months_ago = today - relativedelta(months=3)

    col1, col2, col3 = st.columns(3) # 改为3列
    with col1:
        start_date = st.date_input("起始日期", value=three_months_ago, key="lot_start_date")
    with col2:
        end_date = st.date_input("结束日期", value=today, key="lot_end_date")

    # 1. 先按日期筛选
    filtered_group_summary_df = group_summary_df_full[
        (group_summary_df_full['warehousing_time'] >= start_date) &
        (group_summary_df_full['warehousing_time'] <= end_date)
    ]

    # 2. [新增] 再按 Lot ID 筛选
    with col3:
        # 获取当前日期范围内所有可用的 Lot ID
        lot_ids_in_range = set(filtered_group_summary_df['lot_id'].unique())
        # 调用复用的筛选函数
        final_filtered_df = render_lot_id_filter(filtered_group_summary_df, lot_ids_in_range)

    st.markdown("### 📄 Group不良率明细表 (By Lot)")

    if final_filtered_df.empty:
        st.warning("在您选择的筛选条件下没有数据。")
    else:
        # 准备用于显示的副本 (乘以100)
        df_for_display_group = final_filtered_df.copy()
        rate_columns_to_convert_group = ["pass_rate", "array_pixel_rate", "array_line_rate", "oled_mura_rate"]
        
        for col in rate_columns_to_convert_group:
            if col in df_for_display_group.columns:
                df_for_display_group[col] = df_for_display_group[col] * 100
        
        st.dataframe(
            df_for_display_group,
            column_config={
                "lot_id": st.column_config.TextColumn("Lot ID"),
                "warehousing_time": st.column_config.DateColumn("入库时间", format="YYYY/MM/DD"),
                "array_input_time": st.column_config.DatetimeColumn("阵列投入时间", format="YYYY/MM/DD"),
                "pass_rate": st.column_config.NumberColumn("过货率", format="%.2f%%"),
                "array_pixel_rate": st.column_config.NumberColumn("Array_Pixel不良率", format="%.2f%%"),
                "array_line_rate": st.column_config.NumberColumn("Array_Line不良率", format="%.2f%%"),
                "oled_mura_rate": st.column_config.NumberColumn("OLED_Mura不良率", format="%.2f%%"),
            },
            column_order=[ # 确保时间列在前
                "lot_id", "warehousing_time", "array_input_time", "pass_rate",
                "array_pixel_rate", "array_line_rate", "oled_mura_rate"
            ],
            hide_index=True,
            use_container_width=True
        )
    st.divider()

    # ==============================================================================
    #                      --- 模块2: 按Lot ID查询Code级别详情 ---
    # ==============================================================================
    st.markdown("### ✍️ By Lot ID查询Code级别详情")

    # 使用经过筛选后的 Lot 列表作为备选
    lot_ids = final_filtered_df['lot_id'].unique()
    
    if len(lot_ids) > 0:
        default_val = lot_ids[0]
        # [修改] 使用 text_area 支持多行输入
        selected_lots_str = st.text_area(
            "请在此输入或粘贴您想查询的Lot IDs (每行一个):",
            value=default_val,
            key="lot_text_area_input",
            height=100
        )

        if selected_lots_str:
            # [新增] 解析多行输入
            input_lots = [lot.strip() for lot in selected_lots_str.split('\n') if lot.strip()]
            
            # [新增] 校验有效性
            invalid_lots = [lot for lot in input_lots if lot not in lot_ids]
            valid_lots = [lot for lot in input_lots if lot in lot_ids]

            if invalid_lots:
                st.warning(f"以下 Lot ID 不存在于当前筛选范围内: {', '.join(invalid_lots)}")
            
            if not valid_lots:
                st.info("请输入有效的 Lot ID 进行查询。")
            else:
                code_details_dict = all_data.get("code_level_details")
                if code_details_dict is None:
                    st.error("未能加载Code级别明细数据。")
                    st.stop()

                target_defect_groups = CONFIG['processing']['target_defect_groups']
                
                for group_name in target_defect_groups:
                    st.subheader(group_name)
                    detail_df = code_details_dict.get(group_name)
                    
                    if detail_df is not None and not detail_df.empty:
                        # [修改] 使用 isin 进行多 Lot 筛选
                        filtered_df = detail_df[detail_df['lot_id'].isin(valid_lots)]
                        
                        if not filtered_df.empty:
                            df_for_display_code = filtered_df.copy()
                            if 'defect_rate' in df_for_display_code.columns:
                                df_for_display_code['defect_rate'] = df_for_display_code['defect_rate'] * 100

                            st.dataframe(
                                df_for_display_code.reset_index(drop=True),
                                column_config={
                                    "lot_id": st.column_config.TextColumn("Lot ID"), 
                                    "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                                    "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                                    "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                                },
                                column_order=("lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                                hide_index=True,
                                use_container_width=True
                            )
                        else:
                            st.info(f"所选 Lot IDs 下无该类型不良。")
                    else:
                        st.warning(f"未能加载 {group_name} 的明细数据。")
    else:
        st.info("在当前筛选条件下无Lot可供查询。")

    st.divider()

    # ==============================================================================
    #                      --- 模块3: 按Code查询Lot集中性 (Top 20) ---
    # ==============================================================================
    st.header("🔬 ByCode查询Lot集中性")
    
    code_details_dict = all_data.get("code_level_details")
    if code_details_dict:
        # 1. 准备数据源
        all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
        
        # 2. [关键] 预先筛选数据源，使其与主表保持一致
        # 'lot_ids' 是在模块2开头从 'final_filtered_df' 中获取的
        df_in_scope = all_codes_df[all_codes_df['lot_id'].isin(lot_ids)]

        # 3. 调用智能UI组件 (传入筛选后的范围)
        selected_code_info = create_code_selection_ui(
            source_data=df_in_scope,
            target_defect_groups=CONFIG['processing']['target_defect_groups'],
            key_prefix="lot_focus_table_filtered", # 使用新key避免冲突
            rate_threshold=0.0005 
        )

        # 4. 根据组件的选择结果进行后续操作
        if selected_code_info.get("code"):
            group = selected_code_info["group"]
            code = selected_code_info["code"]
            
            st.markdown(f"#### 不良Code **'{code}'** 的Top 20问题Lot")
            
            # a. 筛选出所选Code的全部数据 (在范围内)
            result_df = df_in_scope[
                (df_in_scope['defect_group'] == group) &
                (df_in_scope['defect_desc'] == code)
            ]
            
            # b. 排序并取前20
            top_20_lots = result_df.sort_values(by="defect_rate", ascending=False).head(20)
            
            if not top_20_lots.empty:
                # c. 准备用于显示的数据
                df_for_display = top_20_lots.copy()
                df_for_display['defect_rate'] *= 100
                
                # d. 显示表格
                st.dataframe(
                    df_for_display.reset_index(drop=True),
                    column_config={
                        "lot_id": st.column_config.TextColumn("Lot ID"),
                        "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                        "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                        "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                    },
                    column_order=("lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("在当前数据范围内，没有找到该Code对应的不良Lot。")
    else:
        st.warning("未能加载Lot的Code级明细数据，无法执行此分析。")

else:
    st.error("未能从后台加载Lot数据，请检查后台日志或刷新重试。")