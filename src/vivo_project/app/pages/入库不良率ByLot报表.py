# src/vivo_project/app/pages/入库不良率ByLot报表.py

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- 1. 初始化与配置 ---
from vivo_project.app.setup import AppSetup
from vivo_project.app.components.components import create_code_selection_ui
AppSetup.initialize_app()

from vivo_project.config import CONFIG
from vivo_project.services.workflow_handler import WorkflowHandler

# --- 2. UI 界面布局 ---
st.set_page_config(page_title="入库不良率ByLot报表", layout="wide")
st.title("📋 入库不良率ByLot报表")

if st.button("🔄 刷新数据"):
    st.cache_data.clear()
    st.rerun()

# --- 3. 加载数据 ---
# (数据已在WorkflowHandler层被缓存)
all_data = WorkflowHandler.run_lot_defect_rate_workflow()


if all_data:
# ==============================================================================
#                      --- 模块1: Group不良率明细表 (By Lot) ---
# ==============================================================================
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Lot级别数据，请检查后台。")
        st.stop()
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(group_summary_df_full['warehousing_time'], format='%Y%m%d').dt.date
    
    st.markdown("### 📅 日期范围选择")
    today = datetime.now().date()
    three_months_ago = today - relativedelta(months=3)

    col1, col2, _ = st.columns(3)
    with col1:
        start_date = st.date_input("起始日期", value=three_months_ago, key="lot_start_date")
    with col2:
        end_date = st.date_input("结束日期", value=today, key="lot_end_date")

    filtered_group_summary_df = group_summary_df_full[
        (group_summary_df_full['warehousing_time'] >= start_date) &
        (group_summary_df_full['warehousing_time'] <= end_date)
    ]

    st.markdown("### 📄 Group不良率明细表 (By Lot)")

    if filtered_group_summary_df.empty:
        st.warning("在您选择的日期范围内没有数据。")
    else:
        # 准备用于显示的副本 (乘以100)
        df_for_display_group = filtered_group_summary_df.copy()
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
    st.markdown("### ✍️ 按Lot ID查询Code级别详情")

    lot_ids = filtered_group_summary_df['lot_id'].unique()
    
    if len(lot_ids) > 0:
        default_lot_id = lot_ids[0]
        selected_lot = st.text_input(
            "请在此输入或粘贴您想查询的Lot ID:",
            value=default_lot_id,
            key="lot_text_input"
        )

        if selected_lot:
            if selected_lot not in lot_ids:
                st.warning(f"输入的Lot ID '{selected_lot}' 不存在于当前数据范围内。")
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
                        filtered_df = detail_df[detail_df['lot_id'] == selected_lot]
                        
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
                            st.info(f"此Lot ({selected_lot}) 下无该类型不良。")
                    else:
                        st.warning(f"未能加载 {group_name} 的明细数据。")
    else:
        st.info("在当前日期范围内无Lot可供查询。")

    st.divider()

# ==============================================================================
#                      --- 模块3: 按Code查询Lot集中性 (Top 20) ---
# ==============================================================================
    st.header("🔬 ByCode查询Lot集中性")
    
    code_details_dict = all_data.get("code_level_details")
    if code_details_dict:
        # 1. 准备数据源
        all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
        
        # 2. 调用智能UI组件
        selected_code_info = create_code_selection_ui(
            source_data=all_codes_df,
            target_defect_groups=CONFIG['processing']['target_defect_groups'],
            key_prefix="lot_focus_table", # 使用唯一的key
            rate_threshold=0.0005 # 沿用我们之前设置的阈值
        )

        # 3. 根据组件的选择结果进行后续操作
        if selected_code_info.get("code"):
            group = selected_code_info["group"]
            code = selected_code_info["code"]
            
            st.markdown(f"#### 不良Code **'{code}'** 的Top 20问题Lot")
            
            # a. 筛选出所选Code的全部数据
            result_df = all_codes_df[
                (all_codes_df['defect_group'] == group) &
                (all_codes_df['defect_desc'] == code)
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