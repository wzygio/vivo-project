# src/vivo_project/app/components/view_sections.py
import streamlit as st
import pandas as pd

# 引入现有的绘图函数
from vivo_project.app.charts.mwd_chart import (
    create_group_trend_chart, 
    create_code_trend_chart,
    slice_recent_data
)
from vivo_project.app.charts.sheet_lot_chart import (
    create_lot_defect_chart, 
    create_sheet_defect_chart,
    create_mapping_heatmap,
    parse_panel_id_to_coords
)
from vivo_project.app.components.components import COLOR_MAP
from vivo_project.core.mapping_processor import apply_hotspot_modification_to_matrix

# ==============================================================================
#  1. 宏观分析区 (Group Level)
# ==============================================================================
def render_macro_trend_section(mwd_group_data: dict):
    if not mwd_group_data:
        st.warning("无宏观趋势数据。")
        return

    available_groups = []
    ref_df = mwd_group_data.get('monthly')
    if ref_df is not None and not ref_df.empty:
        available_groups = sorted(ref_df['defect_group'].unique().tolist())
    
    dynamic_category_orders = {"defect_group": available_groups}

    c1, _, _ = st.columns(3)
    with c1:
        grp_opts = ["全部Group"] + available_groups
        sel_grp_macro = st.selectbox("选择Group:", grp_opts, key="macro_group_sel")

    df_m = slice_recent_data(mwd_group_data.get('monthly'), 3)
    df_w = slice_recent_data(mwd_group_data.get('weekly'), 3)
    df_d = slice_recent_data(mwd_group_data.get('daily'), 7)

    if sel_grp_macro != "全部Group":
        filter_func = lambda df: df[df['defect_group'] == sel_grp_macro] if df is not None else None
        df_m, df_w, df_d = map(filter_func, [df_m, df_w, df_d])

    max_rate = 0
    for df in [df_m, df_w, df_d]:
        if df is not None and not df.empty:
            curr_max = df.groupby('time_period')['defect_rate'].sum().max()
            if pd.notna(curr_max): max_rate = max(max_rate, curr_max)
    y_limit = [0, max_rate * 1.2] if max_rate > 0 else [0, 0.1]

    gc1, gc2, gc3 = st.columns(3)
    chart_configs = [
        (df_m, "月度趋势", False, True, gc1),
        (df_w, "周度趋势", False, False, gc2),
        (df_d, "日度趋势", True, False, gc3)
    ]
    
    for df, title, show_slider, show_count, col in chart_configs:
        with col:
            if df is not None and not df.empty:
                st.plotly_chart(
                    create_group_trend_chart(
                        df, title, show_slider, show_count, y_limit, COLOR_MAP, 
                        dynamic_category_orders, show_input_count=True
                    ),
                    use_container_width=True
                )
            else:
                st.info(f"{title}数据暂无")

# ==============================================================================
#  2. 微观分析区 (Row A: Code 级时间趋势)
# ==============================================================================
def render_micro_trend_section(mwd_code_data: dict, curr_code: str, curr_warning: float):
    with st.container(border=True):
        st.markdown("**A. 月周天趋势图**")
        
        cd_m = slice_recent_data(mwd_code_data.get('monthly'), 3)
        cd_w = slice_recent_data(mwd_code_data.get('weekly'), 3)
        cd_d = slice_recent_data(mwd_code_data.get('daily'), 7)
        
        filter_c = lambda df: df[df['defect_desc'] == curr_code] if df is not None else None
        cd_m, cd_w, cd_d = map(filter_c, [cd_m, cd_w, cd_d])

        c_max = 0
        for df in [cd_m, cd_w, cd_d]:
            if df is not None and not df.empty:
                c_max = max(c_max, df['defect_rate'].max())
        c_ylim = [0, c_max * 1.25] if c_max > 0 else [0, 0.01]

        rc1, rc2, rc3 = st.columns(3)
        chart_configs = [(cd_m, "月度", rc1), (cd_w, "周度", rc2), (cd_d, "日度", rc3)]

        for df, title, col in chart_configs:
            with col:
                if df is not None and not df.empty:
                    fig = create_code_trend_chart(df, title, c_ylim, curr_warning)
                    if fig is not None:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"暂无{title}数据")
                else:
                    st.info(f"暂无{title}数据")

# ==============================================================================
#  3. Lot 集中性 (Row B: 批次维度)
# ==============================================================================
def render_lot_distribution_section(lot_data: dict, curr_code: str, curr_warning: float) -> str:
    """渲染 Lot 集中性图表，并返回用户点击选中的 target_lot"""
    lot_details = lot_data.get("code_level_details", {})
    if not lot_details:
        with st.container(border=True): st.warning("暂无 Lot 数据。")
        return ""

    df_lot_all = pd.concat(lot_details.values(), ignore_index=True)
    df_lot_all['warehousing_time'] = pd.to_datetime(df_lot_all['warehousing_time'], format='%Y%m%d', errors='coerce')
    df_lot_curr = df_lot_all[df_lot_all['defect_desc'] == curr_code].copy()
    df_lot_curr = df_lot_curr[df_lot_curr['defect_rate'] > 0]

    if df_lot_curr.empty:
        with st.container(border=True): st.warning(f"当前 Code ({curr_code}) 在 Lot 级无不良记录。")
        return ""

    iso_s = df_lot_curr['warehousing_time'].dt.isocalendar()
    df_lot_curr['week_label'] = iso_s.year.astype(str) + '-W' + iso_s.week.map('{:02d}'.format)
    df_lot_curr['month_str'] = df_lot_curr['warehousing_time'].dt.strftime('%Y-%m')

    with st.container(border=True):
        st.markdown("**B. Lot集中性 (点击蓝色柱体可查看 Sheet 分布)**")
        
        lc1, lc2, lc3 = st.columns(3)
        with lc1: l_sort = st.selectbox("排序:", ["按入库时间(默认)", "按阵列投入时间", "按不良率(降序)"], key="u_lot_sort")
        with lc2:
            l_months = sorted(df_lot_curr['month_str'].dropna().unique(), reverse=True)
            l_sel_m = st.selectbox("月别:", ["全部"] + l_months, key="u_lot_m")
        with lc3:
            l_weeks = sorted(df_lot_curr['week_label'].dropna().unique(), reverse=True)
            l_sel_w = st.selectbox("周别:", ["全部"] + l_weeks, key="u_lot_w")

        if l_sel_m != "全部": df_lot_curr = df_lot_curr[df_lot_curr['month_str'] == l_sel_m]
        if l_sel_w != "全部": df_lot_curr = df_lot_curr[df_lot_curr['week_label'] == l_sel_w]

        x_lbl = "Lot ID"
        if l_sort == "按不良率(降序)": df_lot_curr = df_lot_curr.sort_values('defect_rate', ascending=False)
        elif l_sort == "按入库时间(默认)": df_lot_curr = df_lot_curr.sort_values('warehousing_time')
        elif l_sort == "按阵列投入时间": df_lot_curr = df_lot_curr.sort_values('array_input_time')

        if df_lot_curr.empty:
            st.warning("当前筛选条件下无 Lot 数据。")
        else:
            fig_lot = create_lot_defect_chart(df_lot_curr, x_lbl, df_lot_curr['lot_id'].tolist(), curr_warning)
            event = st.plotly_chart(fig_lot, use_container_width=True, on_select="rerun", selection_mode="points")
            
            if event and event.selection and event.selection["points"]: # type: ignore
                clicked_lot = event.selection["points"][0]["x"] # type: ignore
                if st.session_state.get("unified_sheet_lot_input") != clicked_lot:
                    st.session_state["unified_sheet_lot_input"] = clicked_lot
                    st.toast(f"已锁定 Lot: {clicked_lot}", icon="🔒")
                    st.rerun()

    return st.session_state.get("unified_sheet_lot_input", "")

# ==============================================================================
#  4. Sheet 分布 (Row C: 单片维度)
# ==============================================================================
def render_sheet_distribution_section(sheet_data: dict, target_lot: str, curr_group: str, curr_code: str):
    """处理复杂的数据清洗与 Left Join，然后渲染图表"""
    with st.container(border=True):
        st.markdown("**C. 单片分布 (By Sheet)**")
        sc1, sc2, _ = st.columns([3, 3, 4])
        
        with sc1:
            # 允许用户手动输入或接收上面点击传过来的值
            input_lot = st.text_input("当前分析 Lot ID:", value=target_lot, key="sheet_lot_input_box", help="点击上方柱图自动填充")
            if input_lot != st.session_state.get("unified_sheet_lot_input"):
                st.session_state["unified_sheet_lot_input"] = input_lot

        target_lot = input_lot

        if not target_lot:
            st.info("等待输入 Lot ID 或点击上方图表...")
            return

        group_summary = sheet_data.get("group_level_summary_for_table", pd.DataFrame())
        if group_summary.empty or 'lot_id' not in group_summary.columns:
            st.warning("暂无 Sheet 级明细数据。")
            return

        df_base_sheets = group_summary[group_summary['lot_id'] == target_lot][
            ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time']
        ].copy()
        
        if df_base_sheets.empty:
            st.warning(f"分析报告：未找到 Lot '{target_lot}' 的任何 Sheet 基础数据。")
            return

        sheet_details_dict = sheet_data.get("code_level_details", {})
        df_sheet_all = sheet_details_dict.get(curr_group, pd.DataFrame())
        
        if not df_sheet_all.empty:
            df_defect_only = df_sheet_all[
                (df_sheet_all['lot_id'] == target_lot) & 
                (df_sheet_all['defect_desc'] == curr_code)
            ][['sheet_id', 'defect_rate', 'defect_panel_count']]
        else:
            df_defect_only = pd.DataFrame(columns=['sheet_id', 'defect_rate', 'defect_panel_count'])
        
        # 数据组装与清洗
        df_sheet = pd.merge(df_base_sheets, df_defect_only, on='sheet_id', how='left')
        df_sheet['defect_rate'] = df_sheet['defect_rate'].fillna(0.0)
        df_sheet['defect_panel_count'] = df_sheet['defect_panel_count'].fillna(0).astype(int)
        df_sheet['warehousing_time'] = pd.to_datetime(df_sheet['warehousing_time'], format='%Y%m%d', errors='coerce')
        
        with sc2:
            s_sort = st.selectbox("Sheet 排序规则:", ["默认(投入时间)", "按不良率(降序)"], key="u_sheet_sort")
        
        if s_sort == "按不良率(降序)":
            df_sheet = df_sheet.sort_values(['defect_rate', 'array_input_time'], ascending=[False, True])
        else:
            df_sheet = df_sheet.sort_values('array_input_time')
        
        fig_sheet = create_sheet_defect_chart(
            df=df_sheet, xaxis_label="Sheet ID", sorted_sheet_ids=df_sheet['sheet_id'].tolist()
        )
        st.plotly_chart(fig_sheet, use_container_width=True)
 
# ==============================================================================
#  5. Mapping (Row D: 空间维度)
# ==============================================================================
def render_mapping_section(mapping_data: pd.DataFrame, curr_group: str, curr_code: str, hotspot_scripts: list):
    """处理矩阵变换与热区应用，渲染 Mapping"""
    with st.container(border=True):
        st.markdown("**D. Mapping集中性**")
        
        if mapping_data is None or mapping_data.empty:
            st.warning("Mapping 数据源为空。")
            return

        df_map = mapping_data[
            (mapping_data['defect_group'] == curr_group) & 
            (mapping_data['defect_desc'] == curr_code)
        ]
        
        if df_map.empty:
            st.warning("该 Code 在 Mapping 数据源中无记录 (可能未达 Top 10 门槛)。")
            return

        batches = sorted(df_map['batch_no'].unique())
        tab_labels = []
        for b in batches:
            b_data = df_map[df_map['batch_no'] == b]
            total_in = b_data['batch_total_input'].iloc[0] if 'batch_total_input' in b_data.columns else 0
            tab_labels.append(f"{b} (入库: {int(total_in):,})" if total_in else f"{b}")
            
        tabs = st.tabs(tab_labels)
        matrices_cache = {}
        g_max = 0
        
        for i, b in enumerate(batches):
            d = df_map[df_map['batch_no'] == b]
            coords = d['panel_id'].apply(parse_panel_id_to_coords)
            d_c = d.assign(r=coords.str[0], c=coords.str[1]).dropna(subset=['r','c'])
            d_c[['r','c']] = d_c[['r','c']].astype(int)
            
            mat = pd.pivot_table(d_c, values='panel_id', index='r', columns='c', aggfunc='count', fill_value=0)
            mat = mat.reindex(index=range(10), columns=range(19), fill_value=0)
            
            b_idx = 'oldest' if i == 0 else ('latest' if i == len(batches) - 1 else 'middle')
            
            mat = apply_hotspot_modification_to_matrix(
                heatmap_matrix=mat, batch_no=b, code_desc=curr_code, 
                batch_index=b_idx, script_config_list=hotspot_scripts
            )

            matrices_cache[b] = mat
            g_max = max(g_max, mat.max().max())
        
        for i, b in enumerate(batches):
            with tabs[i]:
                fig_map = create_mapping_heatmap(matrices_cache[b], f"批次 {b} 热力图", g_max)
                st.plotly_chart(fig_map, use_container_width=True)