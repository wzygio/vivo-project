import streamlit as st
import pandas as pd
import numpy as np
import sys

# --- 1. 配置与初始化 ---
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui, render_page_header

# 引入图表组件
from vivo_project.app.charts.mwd_chart import (
    create_group_trend_chart, 
    create_code_trend_chart,
    slice_recent_data
)
from vivo_project.app.charts.sheet_lot_chart import (
    create_lot_defect_chart, 
    create_sheet_stack_chart,
    create_mapping_heatmap,
    parse_panel_id_to_coords
)

@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()
init_global_resources()

# --- 2. 辅助逻辑：构建全能筛选源 (并集策略) ---
@st.cache_data(ttl="1h")
def _prepare_union_data_for_filter(
    mwd_data: dict, 
    lot_data: dict, 
    mapping_data: pd.DataFrame
) -> pd.DataFrame:
    """
    [核心策略]：并集筛选 (Union Strategy)
    分别从 Trend, Lot, Mapping 中提取满足各自门槛的 Code，合并为一个主表。
    用于欺骗筛选器组件，使其能同时展示所有维度的关注点。
    """
    candidates = {} # {(group, code): max_rate}

    # 1. 提取 Trend 候选者 (门槛 > 0.01%)
    # mwd_data 是 dict {'monthly': df, ...}
    if mwd_data:
        trend_df = pd.concat([df for df in mwd_data.values() if df is not None], ignore_index=True)
        if not trend_df.empty:
            # 按 Code 分组取最大不良率
            valid_trend = trend_df.groupby(['defect_group', 'defect_desc'])['defect_rate'].max()
            valid_trend = valid_trend[valid_trend > 0.0001] # 0.01%
            for (grp, code), rate in valid_trend.items():
                candidates[(grp, code)] = max(candidates.get((grp, code), 0), rate)

    # 2. 提取 Lot 候选者 (门槛 > 0.02%)
    # lot_data['code_level_details'] 是 dict {group: df}
    if lot_data and lot_data.get('code_level_details'):
        lot_dfs = lot_data['code_level_details'].values()
        if lot_dfs:
            lot_full = pd.concat(lot_dfs, ignore_index=True)
            if not lot_full.empty:
                valid_lot = lot_full.groupby(['defect_group', 'defect_desc'])['defect_rate'].max()
                valid_lot = valid_lot[valid_lot > 0.0002] # 0.02%
                for (grp, code), rate in valid_lot.items():
                    candidates[(grp, code)] = max(candidates.get((grp, code), 0), rate)

    # 3. 提取 Mapping 候选者 (门槛 > 10 count)
    if mapping_data is not None and not mapping_data.empty:
        # Mapping 只有 count，没有 rate
        counts = mapping_data.groupby(['defect_group', 'defect_desc']).size()
        valid_map = counts[counts > 10]
        for (grp, code), _ in valid_map.items(): # type: ignore
            # 如果该 Code 仅在 Mapping 中出现，给一个默认权重以便排序
            # 如果它也在 Trend/Lot 中，保留原有的 rate
            if (grp, code) not in candidates:
                candidates[(grp, code)] = 0.0001 

    # 4. 构建最终 DataFrame
    if not candidates:
        return pd.DataFrame(columns=['defect_group', 'defect_desc', 'defect_rate'])
    
    rows = [{'defect_group': k[0], 'defect_desc': k[1], 'defect_rate': v} for k, v in candidates.items()]
    return pd.DataFrame(rows)

# ==============================================================================
#  页面主逻辑
# ==============================================================================
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")
render_page_header("📊 入库不良率分析看板")

# --- 3. 全局数据加载 ---
with st.spinner("正在加载全维度分析数据..."):
    # 并行加载所有服务数据
    mwd_group_data = YieldAnalysisService.get_mwd_trend_data()
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data()
    lot_data = YieldAnalysisService.get_lot_defect_rates()
    sheet_data = YieldAnalysisService.get_sheet_defect_rates()
    mapping_data = YieldAnalysisService.get_mapping_data()
    warning_lines = YieldAnalysisService.load_static_warning_lines()

# 基础校验
if not all([mwd_code_data, lot_data, sheet_data]):
    st.error("部分核心数据加载失败，请检查后台日志。")
    st.stop()

# --- 常量 ---
COLOR_MAP = {
    'Array_Line': "#1930ff",  # Plotly默认的蓝色
    'OLED_Mura': "#ff2828",   # Plotly默认的红色
    'Array_Pixel': "#6fb9ff",   # Plotly默认的浅蓝色
    'array_Line_rate': "#1930ff",  # Plotly默认的蓝色
    'oled_mura_rate': "#ff2828",   # Plotly默认的红色
    'array_pixel_rate': "#6fb9ff"   # Plotly默认的浅蓝色
}

CATEGORY_ORDERS = {"defect_group": CONFIG['processing']['target_defect_groups']}

# ==============================================================================
#  第一部分: 宏观监控 (Group级趋势) - 独立筛选
# ==============================================================================
st.subheader("1️⃣ 入库不良率分析 (Group Level)")

if mwd_group_data:
    c1, _, _ = st.columns(3)
    with c1:
        grp_opts = ["全部Group"] + CONFIG['processing']['target_defect_groups']
        sel_grp_macro = st.selectbox("选择Group:", grp_opts, key="macro_group_sel")

    # 数据准备与切片
    df_m = slice_recent_data(mwd_group_data.get('monthly'), 3)
    df_w = slice_recent_data(mwd_group_data.get('weekly'), 3)
    df_d = slice_recent_data(mwd_group_data.get('daily'), 7)

    # 过滤
    if sel_grp_macro != "全部Group":
        filter_func = lambda df: df[df['defect_group'] == sel_grp_macro] if df is not None else None
        df_m, df_w, df_d = map(filter_func, [df_m, df_w, df_d])

    # Y轴统一
    max_rate = 0
    for df in [df_m, df_w, df_d]:
        if df is not None and not df.empty:
            curr_max = df.groupby('time_period')['defect_rate'].sum().max()
            if pd.notna(curr_max): max_rate = max(max_rate, curr_max)
    y_limit = [0, max_rate * 1.2] if max_rate > 0 else [0, 0.1]

    # 绘图
    gc1, gc2, gc3 = st.columns(3)
    
    # 统一配置三个图表的数据和参数
    chart_configs = [
        (df_m, "月度趋势", False, True, gc1),
        (df_w, "周度趋势", False, False, gc2),
        (df_d, "日度趋势", True, False, gc3)
    ]
    
    for df, title, show_slider, show_count, col in chart_configs:
        with col:
            if df is not None and not df.empty:
                st.plotly_chart(
                    create_group_trend_chart(df, title, show_slider, show_count, y_limit, COLOR_MAP, CATEGORY_ORDERS, show_input_count=True),
                    use_container_width=True
                )
            else:
                st.info(f"{title}数据暂无")

st.divider()

# ==============================================================================
#  第二部分: 核心筛选器 (统一控制下方所有图表)
# ==============================================================================
st.subheader("2️⃣ 入库不良率分析 (Code Level)")

# 1. 准备“全能候选池”
master_df = _prepare_union_data_for_filter(mwd_code_data, lot_data, mapping_data)

# 2. 渲染筛选器 (阈值设为0，因为已经在 prepare 阶段筛选过了)
selection = create_code_selection_ui(
    source_data=master_df,
    target_defect_groups=CONFIG['processing']['target_defect_groups'],
    key_prefix="unified_focus",
    rate_threshold=0 # <--- 关键：信任 master_df 的筛选结果
)

# 如果没选 Code，下方不显示
if not selection.get("code"):
    st.info("👈 请在上方选择一个 Defect Code 以查看详细分析。")
    st.stop()

# 获取当前上下文
curr_code = selection["code"]
curr_group = selection["group"]
curr_warning = warning_lines.get(curr_code)

st.markdown(f"### 🎯 当前分析: **{curr_code}**")

# ==============================================================================
#  Row A: Code 级趋势 (时间维度)
# ==============================================================================
with st.container(border=True):
    st.markdown("**A. 月周天趋势图**")
    
    # 准备数据
    cd_m = slice_recent_data(mwd_code_data.get('monthly'), 3)
    cd_w = slice_recent_data(mwd_code_data.get('weekly'), 3)
    cd_d = slice_recent_data(mwd_code_data.get('daily'), 7)
    
    # 过滤 Code
    filter_c = lambda df: df[df['defect_desc'] == curr_code] if df is not None else None
    cd_m, cd_w, cd_d = map(filter_c, [cd_m, cd_w, cd_d])

    # 动态 Y 轴
    c_max = 0
    for df in [cd_m, cd_w, cd_d]:
        if df is not None and not df.empty:
            c_max = max(c_max, df['defect_rate'].max())
    c_ylim = [0, c_max * 1.25] if c_max > 0 else [0, 0.01]

    rc1, rc2, rc3 = st.columns(3)
    with rc1: st.plotly_chart(create_code_trend_chart(cd_m, "月度", c_ylim, curr_warning), use_container_width=True) # type: ignore
    with rc2: st.plotly_chart(create_code_trend_chart(cd_w, "周度", c_ylim, curr_warning), use_container_width=True) # type: ignore
    with rc3: st.plotly_chart(create_code_trend_chart(cd_d, "日度", c_ylim, curr_warning), use_container_width=True) # type: ignore

# ==============================================================================
#  Row B: Lot 集中性 (批次维度) - 支持交互
# ==============================================================================
# 准备 Lot 数据
lot_details = lot_data.get("code_level_details", {})
# 展平并处理时间
df_lot_all = pd.concat(lot_details.values(), ignore_index=True)
df_lot_all['warehousing_time'] = pd.to_datetime(df_lot_all['warehousing_time'], format='%Y%m%d', errors='coerce')
df_lot_curr = df_lot_all[df_lot_all['defect_desc'] == curr_code].copy()

# 辅助列
iso_s = df_lot_curr['warehousing_time'].dt.isocalendar()
df_lot_curr['week_label'] = iso_s.year.astype(str) + '-W' + iso_s.week.map('{:02d}'.format)
df_lot_curr['month_str'] = df_lot_curr['warehousing_time'].dt.strftime('%Y-%m')

with st.container(border=True):
    st.markdown("**B. Lot集中性 (点击蓝色柱体可查看 Sheet 分布)**")
    
    # 筛选栏
    lc1, lc2, lc3 = st.columns(3)
    with lc1: 
        l_sort = st.selectbox("排序:", ["按入库时间(默认)", "按阵列投入时间", "按不良率(降序)"], key="u_lot_sort")
    with lc2:
        l_months = sorted(df_lot_curr['month_str'].dropna().unique(), reverse=True)
        l_sel_m = st.selectbox("月别:", ["全部"] + l_months, key="u_lot_m")
    with lc3:
        l_weeks = sorted(df_lot_curr['week_label'].dropna().unique(), reverse=True)
        l_sel_w = st.selectbox("周别:", ["全部"] + l_weeks, key="u_lot_w")

    # 应用筛选
    if l_sel_m != "全部": df_lot_curr = df_lot_curr[df_lot_curr['month_str'] == l_sel_m]
    if l_sel_w != "全部": df_lot_curr = df_lot_curr[df_lot_curr['week_label'] == l_sel_w]

    # 应用排序
    if l_sort == "按不良率(降序)":
        df_lot_curr = df_lot_curr.sort_values('defect_rate', ascending=False)
        x_lbl = "Lot ID"
    elif l_sort == "按入库时间(默认)":
        df_lot_curr = df_lot_curr.sort_values('warehousing_time')
        x_lbl = "Lot ID"
    elif l_sort == "按阵列投入时间":
        df_lot_curr = df_lot_curr.sort_values('array_input_time')
        x_lbl = "Lot ID"

    if df_lot_curr.empty:
        st.warning("当前筛选条件下无 Lot 数据。")
    else:
        # 绘图
        fig_lot = create_lot_defect_chart(
            df_lot_curr, x_lbl, df_lot_curr['lot_id'].tolist(), curr_warning
        )
        # 交互逻辑
        event = st.plotly_chart(fig_lot, use_container_width=True, on_select="rerun", selection_mode="points")
        
        # 捕获点击 -> 存入 Session -> 驱动下方的 Sheet 图
        if event and event.selection and event.selection["points"]: # type: ignore
            clicked_lot = event.selection["points"][0]["x"] # type: ignore
            if st.session_state.get("unified_sheet_lot_input") != clicked_lot:
                st.session_state["unified_sheet_lot_input"] = clicked_lot
                st.toast(f"已锁定 Lot: {clicked_lot}", icon="🔒")
                st.rerun()

# ==============================================================================
#  Row C: Sheet 分布 (单片维度) - 接收 Lot 点击
# ==============================================================================
with st.container(border=True):
    st.markdown("**C. 单片分布 (By Sheet)**")
    
    sc1, sc2, _ = st.columns([3, 3, 4])
    with sc1:
        # 这个输入框既可以手动输，也会被上方图表点击自动填
        target_lot = st.text_input("当前分析 Lot ID:", key="unified_sheet_lot_input", help="点击上方柱图自动填充")
    
    if target_lot:
        # 准备数据
        group_summary = sheet_data.get("group_level_summary_for_table")
        if group_summary is not None and not group_summary.empty:
            # 简单清洗
            group_summary['warehousing_time'] = pd.to_datetime(group_summary['warehousing_time'], format='%Y%m%d', errors='coerce')
            rate_cols = [c for c in group_summary.columns if c.endswith('_rate') and c!='pass_rate']
            group_summary['total_defect_rate'] = group_summary[rate_cols].sum(axis=1)
            
            # 筛选
            df_sheet = group_summary[group_summary['lot_id'] == target_lot]
            
            if df_sheet.empty:
                st.warning(f"未找到 Lot '{target_lot}' 的 Sheet 数据。")
            else:
                with sc2:
                    # [修改] 移除 label_visibility="collapsed"，给一个正常的 label
                    s_sort = st.selectbox(
                        "Sheet 排序规则:",  # 这里加上文字，高度自然就对齐了
                        ["默认(投入时间)", "按不良率(降序)"], 
                        key="u_sheet_sort"
                    )
                
                if s_sort == "按不良率(降序)":
                    df_sheet = df_sheet.sort_values('total_defect_rate', ascending=False)
                else:
                    df_sheet = df_sheet.sort_values('array_input_time')
                
                # 绘图
                fig_sheet = create_sheet_stack_chart(
                    df_sheet, "Sheet ID", df_sheet['sheet_id'].tolist(), COLOR_MAP
                )
                st.plotly_chart(fig_sheet, use_container_width=True)
    else:
        st.info("等待输入 Lot ID 或点击上方图表...")

# ==============================================================================
#  Row D: Mapping (空间维度)
# ==============================================================================
with st.container(border=True):
    st.markdown("**D. Mapping集中性**")
    
    if mapping_data is not None and not mapping_data.empty:
        df_map = mapping_data[
            (mapping_data['defect_group'] == curr_group) & 
            (mapping_data['defect_desc'] == curr_code)
        ]
        
        if df_map.empty:
            st.warning("该 Code 在 Mapping 数据源中无记录 (可能未达 Top 10 门槛)。")
        else:
            batches = sorted(df_map['batch_no'].unique())
            tabs = st.tabs([f"批次: {b}" for b in batches])
            
            # 计算全局最大值，统一色阶
            # 预计算所有矩阵以获取 Max
            matrices_cache = {}
            g_max = 0
            for b in batches:
                d = df_map[df_map['batch_no'] == b]
                coords = d['panel_id'].apply(parse_panel_id_to_coords)
                d_c = d.assign(r=coords.str[0], c=coords.str[1]).dropna(subset=['r','c'])
                d_c[['r','c']] = d_c[['r','c']].astype(int)
                mat = pd.pivot_table(d_c, values='panel_id', index='r', columns='c', aggfunc='count', fill_value=0)
                mat = mat.reindex(index=range(10), columns=range(19), fill_value=0)
                matrices_cache[b] = mat
                g_max = max(g_max, mat.max().max())
            
            # 渲染
            for i, b in enumerate(batches):
                with tabs[i]:
                    fig_map = create_mapping_heatmap(matrices_cache[b], f"批次 {b} 热力图", g_max)
                    st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("Mapping 数据源为空。")