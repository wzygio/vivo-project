# src/vivo_project/app/components/table_sections.py
import streamlit as st
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from app.components.components import create_code_selection_ui
from app.charts.sheet_details_chart import render_lot_id_filter, render_sheet_id_query

# ==============================================================================
#  Lot 级明细区块 (For ByLot明细表)
# ==============================================================================
def render_lot_group_summary_section(all_data: dict) -> list:
    """渲染 Lot 级 Group 汇总表，返回过滤后的有效 Lot ID 列表"""
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Lot级别数据，请检查后台。")
        return []
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(group_summary_df_full['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
    
    st.markdown("### 📅 筛选条件")
    today = datetime.now().date()
    three_months_ago = today - relativedelta(months=3)

    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("起始日期", value=three_months_ago, key="lot_start_date")
    with col2:
        end_date = st.date_input("结束日期", value=today, key="lot_end_date")

    filtered_df = group_summary_df_full[
        (group_summary_df_full['warehousing_time'] >= start_date) &
        (group_summary_df_full['warehousing_time'] <= end_date)
    ]

    with col3:
        lot_ids_in_range = set(filtered_df['lot_id'].unique())
        final_filtered_df = render_lot_id_filter(filtered_df, lot_ids_in_range)

    st.markdown("### 📄 Group不良率明细表 (By Lot)")

    if final_filtered_df.empty:
        st.warning("在您选择的筛选条件下没有数据。")
        return []
        
    df_for_display = final_filtered_df.copy()
    rate_cols = ["pass_rate", "array_pixel_rate", "array_line_rate", "oled_mura_rate"]
    for col in rate_cols:
        if col in df_for_display.columns:
            df_for_display[col] = df_for_display[col] * 100
    
    st.dataframe(
        df_for_display,
        column_config={
            "lot_id": st.column_config.TextColumn("Lot ID"),
            "warehousing_time": st.column_config.DateColumn("入库时间", format="YYYY/MM/DD"),
            "array_input_time": st.column_config.DatetimeColumn("阵列投入时间", format="YYYY/MM/DD"),
            "pass_rate": st.column_config.NumberColumn("入库率", format="%.2f%%"),
            "array_pixel_rate": st.column_config.NumberColumn("Array_Pixel不良率", format="%.2f%%"),
            "array_line_rate": st.column_config.NumberColumn("Array_Line不良率", format="%.2f%%"),
            "oled_mura_rate": st.column_config.NumberColumn("OLED_Mura不良率", format="%.2f%%"),
        },
        column_order=["lot_id", "warehousing_time", "array_input_time", "pass_rate", "array_pixel_rate", "array_line_rate", "oled_mura_rate"],
        hide_index=True, use_container_width=True
    )
    st.divider()
    return final_filtered_df['lot_id'].unique().tolist()

def render_lot_code_details_section(all_data: dict, valid_lot_ids: list):
    """渲染指定 Lot 的 Code 级详情"""
    st.markdown("### ✍️ By Lot ID查询Code级别详情")
    if not valid_lot_ids:
        st.info("在当前筛选条件下无Lot可供查询。")
        st.divider()
        return

    default_val = valid_lot_ids[0]
    selected_lots_str = st.text_area("请在此输入或粘贴您想查询的Lot IDs (每行一个):", value=default_val, key="lot_text_area_input", height=100)

    if selected_lots_str:
        input_lots = [lot.strip() for lot in selected_lots_str.split('\n') if lot.strip()]
        invalid_lots = [lot for lot in input_lots if lot not in valid_lot_ids]
        valid_lots = [lot for lot in input_lots if lot in valid_lot_ids]

        if invalid_lots: st.warning(f"以下 Lot ID 不存在于当前筛选范围内: {', '.join(invalid_lots)}")
        
        if valid_lots:
            code_details_dict = all_data.get("code_level_details", {})
            for group_name in sorted(code_details_dict.keys()):
                st.subheader(group_name)
                detail_df = code_details_dict.get(group_name)
                
                if detail_df is not None and not detail_df.empty:
                    filtered_df = detail_df[detail_df['lot_id'].isin(valid_lots)]
                    if not filtered_df.empty:
                        df_display = filtered_df.copy()
                        df_display['defect_rate'] = df_display['defect_rate'] * 100
                        st.dataframe(
                            df_display.reset_index(drop=True),
                            column_config={
                                "lot_id": st.column_config.TextColumn("Lot ID"), 
                                "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                                "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                                "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                            },
                            column_order=("lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                            hide_index=True, use_container_width=True
                        )
                    else:
                        st.info(f"所选 Lot IDs 下无该类型不良。")
    st.divider()

def render_lot_top20_section(all_data: dict, valid_lot_ids: list):
    """渲染某 Code 下 Top 20 严重 Lot 的统计"""
    st.header("🔬 ByCode查询Lot集中性")
    code_details_dict = all_data.get("code_level_details")
    if not code_details_dict:
        st.warning("未能加载Lot的Code级明细数据，无法执行此分析。")
        return

    all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
    df_in_scope = all_codes_df[all_codes_df['lot_id'].isin(valid_lot_ids)]

    selected_code_info = create_code_selection_ui(source_data=df_in_scope, key_prefix="lot_focus_table_filtered")

    if selected_code_info.get("code"):
        group, code = selected_code_info["group"], selected_code_info["code"]
        st.markdown(f"#### 不良Code **'{code}'** 的Top 20问题Lot")
        
        result_df = df_in_scope[(df_in_scope['defect_group'] == group) & (df_in_scope['defect_desc'] == code)]
        top_20 = result_df.sort_values(by="defect_rate", ascending=False).head(20)
        
        if not top_20.empty:
            df_display = top_20.copy()
            df_display['defect_rate'] *= 100
            st.dataframe(
                df_display.reset_index(drop=True),
                column_config={
                    "lot_id": st.column_config.TextColumn("Lot ID"),
                    "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                    "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                    "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                },
                column_order=("lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                hide_index=True, use_container_width=True
            )
        else:
            st.info("在当前数据范围内，没有找到该Code对应的不良Lot。")

# ==============================================================================
#  Sheet 级明细区块 (For BySheet明细表)
# ==============================================================================
def render_sheet_group_summary_section(all_data: dict) -> list:
    """渲染 Sheet 级 Group 汇总表，返回过滤后的有效 Sheet ID 列表"""
    group_summary_df_full = all_data.get("group_level_summary_for_table")
    if group_summary_df_full is None or group_summary_df_full.empty:
        st.error("未能加载Group级别数据，请检查后台。")
        return []
        
    group_summary_df_full['warehousing_time'] = pd.to_datetime(group_summary_df_full['warehousing_time'], format='%Y%m%d', errors='coerce').dt.date
    st.markdown("### 📄 Group不良率明细表（By Sheet）")

    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("起始日期", value=(datetime.now().date() - relativedelta(months=3)), key="sheet_start_date")
    with col2:
        end_date = st.date_input("结束日期", value=datetime.now().date(), key="sheet_end_date")
    
    df_filtered_by_date = group_summary_df_full[
        (group_summary_df_full['warehousing_time'] >= start_date) &
        (group_summary_df_full['warehousing_time'] <= end_date)
    ]

    with col3:
        lot_ids_in_range = set(df_filtered_by_date['lot_id'].unique())
        final_filtered_df = render_lot_id_filter(df_filtered_by_date, lot_ids_in_range)

    if final_filtered_df.empty:
        st.warning("在您选择的筛选条件下没有数据。")
        return []
        
    df_for_display = final_filtered_df.copy()
    rate_cols = ["array_pixel_rate", "array_line_rate", "oled_mura_rate"]
    for col in rate_cols:
        if col in df_for_display.columns:
            df_for_display[col] = df_for_display[col] * 100
    
    st.dataframe(
        df_for_display,
        column_config={
            "sheet_id": st.column_config.TextColumn("Sheet ID"),
            "lot_id": st.column_config.TextColumn("Lot ID"),
            "warehousing_time": st.column_config.DateColumn("入库时间", format="YYYY/MM/DD"),
            "array_input_time": st.column_config.DatetimeColumn("阵列投入时间", format="YYYY/MM/DD"),
            "array_pixel_rate": st.column_config.NumberColumn("Array_Pixel不良率", format="%.2f%%"),
            "array_line_rate": st.column_config.NumberColumn("Array_Line不良率", format="%.2f%%"),
            "oled_mura_rate": st.column_config.NumberColumn("OLED_Mura不良率", format="%.2f%%"),
        },
        column_order=["sheet_id", "lot_id", "warehousing_time", "array_input_time", "array_pixel_rate", "array_line_rate", "oled_mura_rate"],
        hide_index=True, use_container_width=True
    )
    st.divider()
    return final_filtered_df['sheet_id'].unique().tolist()

def render_sheet_code_details_section(all_data: dict, valid_sheet_ids: list):
    """渲染指定 Sheet 的 Code 级详情 (复用原有图表模块)"""
    st.markdown("### ✍️ By Sheet ID查询Code不良率")
    if len(valid_sheet_ids) > 0:
        code_details_dict = all_data.get("code_level_details")
        if code_details_dict:
            render_sheet_id_query(valid_sheet_ids, code_details_dict)
        else:
            st.error("未能加载Code级别明细数据。")
    else:
        st.info("在当前筛选条件下无Sheet可供查询。")
    st.divider()

def render_sheet_top20_section(all_data: dict, valid_sheet_ids: list):
    """渲染某 Code 下 Top 20 严重 Sheet 的统计"""
    st.header("🔬 ByCode查询Sheet集中性")
    code_details_dict = all_data.get("code_level_details")
    if not code_details_dict:
        st.warning("未能加载Code级明细数据，无法执行此分析。")
        return

    all_codes_df = pd.concat(code_details_dict.values(), ignore_index=True)
    df_in_scope = all_codes_df[all_codes_df['sheet_id'].isin(valid_sheet_ids)]

    selected_code_info = create_code_selection_ui(source_data=df_in_scope, key_prefix="sheet_focus")

    if selected_code_info.get("code"):
        group, code = selected_code_info["group"], selected_code_info["code"]
        st.markdown(f"#### 不良Code **'{code}'** 的Top 20问题Sheet")
        
        result_df = df_in_scope[(df_in_scope['defect_group'] == group) & (df_in_scope['defect_desc'] == code)]
        top_20 = result_df.sort_values(by="defect_rate", ascending=False).head(20)
        
        if not top_20.empty:
            df_display = top_20.copy()
            df_display['defect_rate'] *= 100
            st.dataframe(
                df_display.reset_index(drop=True),
                column_config={
                    "sheet_id": st.column_config.TextColumn("Sheet ID"),
                    "lot_id": st.column_config.TextColumn("Lot ID"),
                    "defect_desc": st.column_config.TextColumn("Defect Code描述"),
                    "defect_panel_count": st.column_config.NumberColumn("不良Panel数"),
                    "defect_rate": st.column_config.NumberColumn("Code不良率", format="%.2f%%")
                },
                column_order=("sheet_id", "lot_id", "defect_desc", "defect_panel_count", "defect_rate"),
                hide_index=True, use_container_width=True
            )
        else:
            st.info("在当前筛选条件下，没有找到该Code对应的不良Sheet。")