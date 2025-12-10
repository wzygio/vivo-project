# src/vivo_project/app/pages/入库不良率BySheet明细表.py
import streamlit as st
from vivo_project.config import CONFIG

# --- 辅助函数 ---
def filter_by_multiple_ids(df, id_column, input_text, valid_ids):
    """
    根据输入的多个ID筛选DataFrame
    
    Args:
        df: 要筛选的DataFrame
        id_column: ID列名
        input_text: 用户输入的文本（多行，每行一个ID）
        valid_ids: 有效的ID集合
    
    Returns:
        筛选后的DataFrame和无效的ID列表
    """
    if not input_text:
        return df, []
    
    input_ids = [id_.strip() for id_ in input_text.split('\n') if id_.strip()]
    invalid_ids = [id_ for id_ in input_ids if id_ not in valid_ids]
    valid_input_ids = [id_ for id_ in input_ids if id_ in valid_ids]
    
    if valid_input_ids:
        filtered_df = df[df[id_column].isin(valid_input_ids)]
    else:
        filtered_df = df.iloc[0:0]  # 返回空DataFrame
    
    return filtered_df, invalid_ids

def render_lot_id_filter(df, lot_ids_in_range):
    """
    渲染Lot ID筛选器
    
    Returns:
        筛选后的DataFrame
    """
    selected_lots = st.text_area("Lot IDs (可选, 每行输入一个ID)")
    filtered_df, invalid_lots = filter_by_multiple_ids(
        df, 'lot_id', selected_lots, lot_ids_in_range
    )
    
    if invalid_lots:
        st.warning(f"以下Lot ID不存在于当前日期范围内: {', '.join(invalid_lots)}")
    
    return filtered_df

def render_sheet_id_query(sheet_ids, code_details_dict):
    """
    渲染Sheet ID查询界面
    
    Args:
        sheet_ids: 可用的Sheet ID列表
        code_details_dict: Code级别详细数据字典
    """
    selected_sheets = st.text_area(
        "请在此输入或粘贴您想查询的Sheet IDs (每行一个):",
        value=sheet_ids[0] if len(sheet_ids) > 0 else ""
    )
    
    if not selected_sheets:
        st.info("请输入有效的Sheet ID进行查询。")
        return
    
    input_sheets = [sheet.strip() for sheet in selected_sheets.split('\n') if sheet.strip()]
    invalid_sheets = [sheet for sheet in input_sheets if sheet not in sheet_ids]
    
    if invalid_sheets:
        st.warning(f"以下Sheet ID不存在于当前数据范围内: {', '.join(invalid_sheets)}")
    
    valid_sheets = [sheet for sheet in input_sheets if sheet in sheet_ids]
    
    if not valid_sheets:
        return
    
    target_defect_groups = CONFIG['processing']['target_defect_groups']
    
    for group_name in target_defect_groups:
        st.subheader(group_name)
        detail_df = code_details_dict.get(group_name)
        
        if detail_df is not None and not detail_df.empty:
            filtered_df = detail_df[detail_df['sheet_id'].isin(valid_sheets)]
            
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
                st.info(f"所选Sheet IDs下无该类型不良。")
        else:
            st.warning(f"未能加载 {group_name} 的明细数据。")