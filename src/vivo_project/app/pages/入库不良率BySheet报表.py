# src/vivo_project/app/pages/入库不良率BySheet报表.py
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
st.set_page_config(page_title="入库不良率BySheet报表", layout="wide")
st.title("📈 入库不良率BySheet报表")

if st.button("🔄 刷新数据"):
    st.cache_data.clear()
    st.rerun()

# --- 3. 加载数据 ---
all_data = WorkflowHandler.run_sheet_defect_rate_workflow()

# ==============================================================================
#                      --- 模块1: Group不良率明细表 (By Sheet) ---
# ==============================================================================
if all_data:
    
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Group级别数据，请检查后台。")
        st.stop()
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(group_summary_df_full['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
    
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
        selected_lot = st.text_input("Lot ID (可选, 留空即为全选)")

    # 应用所有筛选
    final_filtered_df = df_filtered_by_date
    if selected_lot:
        if selected_lot in lot_ids_in_range:
            final_filtered_df = df_filtered_by_date[df_filtered_by_date['lot_id'] == selected_lot]
        else:
            st.warning(f"输入的Lot ID '{selected_lot}' 不存在于当前日期范围内。")
            final_filtered_df = pd.DataFrame(columns=df_filtered_by_date.columns)

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
                "array_input_time": st.column_config.DatetimeColumn("阵列投入时间", format="YYYY/MM/DD HH:mm"),
                "pass_rate": st.column_config.NumberColumn("过货率", format="%.2f%%"),
                "array_pixel_rate": st.column_config.NumberColumn("Array_Pixel不良率", format="%.2f%%"),
                "array_line_rate": st.column_config.NumberColumn("Array_Line不良率", format="%.2f%%"),
                "oled_mura_rate": st.column_config.NumberColumn("OLED_Mura不良率", format="%.2f%%"),
            },
            column_order=[ # 确保时间列在前
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
    
    st.markdown("### ✍️ 按Sheet ID查询Code不良率")

    sheet_ids = final_filtered_df['sheet_id'].unique() # <--- 关键：只从已筛选的DF中获取Sheet ID
    
    if len(sheet_ids) > 0:
        default_sheet_id = sheet_ids[0]
        selected_sheet = st.text_input(
            "请在此输入或粘贴您想查询的Sheet ID:",
            value=default_sheet_id
        )

        if selected_sheet:
            if selected_sheet not in sheet_ids:
                st.warning(f"输入的Sheet ID '{selected_sheet}' 不存在于当前数据范围内，请从上方主表中选择一个有效的ID进行复制粘贴。")
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
                        filtered_df = detail_df[detail_df['sheet_id'] == selected_sheet]
                        
                        if not filtered_df.empty:
                            df_for_display_code = filtered_df.copy()
                            if 'defect_rate' in df_for_display_code.columns:
                                df_for_display_code['defect_rate'] *= 100
                                
                            st.dataframe(
                                df_for_display_code.reset_index(drop=True),
                                column_config={
                                    "sheet_id": st.column_config.TextColumn("Sheet ID"),
                                    "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                                    "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                                    "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                                },
                                column_order=("sheet_id", "defect_desc", "defect_panel_count", "defect_rate"),
                                hide_index=True,
                                use_container_width=True
                            )
                        else:
                            st.info(f"此Sheet ({selected_sheet}) 下无该类型不良。")
                    else:
                        st.warning(f"未能加载 {group_name} 的明细数据。")
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