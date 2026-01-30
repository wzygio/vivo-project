import streamlit as st
import pandas as pd
import numpy as np
import sys, logging

# --- [新增] 热重载机制 ---
ENABLE_HOT_RELOAD = True

if ENABLE_HOT_RELOAD:
    # 必须在 import 业务服务之前执行清理
    try:
        from vivo_project.utils.reloader import deep_reload_modules
        deep_reload_modules()
    except ImportError:
        pass

# --- 1. 配置与初始化 ---
from vivo_project.utils.session_manager import SessionManager
from vivo_project.config import ConfigLoader
from vivo_project.utils.app_setup import AppSetup
from vivo_project.utils.reloader import get_project_revision

from vivo_project.application.alert_service import AlertService
from vivo_project.application.yield_service import YieldAnalysisService
from vivo_project.core.mapping_processor import apply_hotspot_modification_to_matrix
from vivo_project.app.components.components import create_code_selection_ui, render_page_header

# 引入图表组件
from vivo_project.app.charts.mwd_chart import (
    create_group_trend_chart, 
    create_code_trend_chart,
    slice_recent_data,
    detect_abnormal_fluctuations,
    prepare_union_data_for_filter
)
from vivo_project.app.charts.sheet_lot_chart import (
    create_lot_defect_chart, 
    create_sheet_stack_chart,
    create_mapping_heatmap,
    parse_panel_id_to_coords
)

# ==============================================================================
#  页面主逻辑
# ==============================================================================
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

AppSetup.initialize_app()

# [Refactor] 2. 获取上下文 (配置 & 路径)
active_config = SessionManager.get_active_config()
project_root = ConfigLoader.get_project_root()
resource_dir = SessionManager.get_resource_dir()

# [Refactor] 3. 渲染页头 (注入 config 用于刷新逻辑)
render_page_header("📊 入库不良率分析看板", active_config)

# --- 2 全局数据加载 ---
with st.spinner("正在加载全维度分析数据..."):
    # [Refactor] 4. 获取核心版本号 (依赖注入 project_root)
    current_revision = get_project_revision(project_root)
    
    # 1. 获取当前产品代号
    current_product = st.session_state.get(SessionManager.KEY_PRODUCT, "Unknown")
    
    # 2. 定义默认参数 (兜底)
    # Group 级默认参数
    group_ema_span = 14
    group_scale = 1.0
    # Code 级默认参数
    code_ema_span = 30
    code_scale = 0.7

    # 3. 针对特定产品进行参数微调 (Hardcode 模式)
    if current_product == "M678":
        USE_TOP_DOWN_STRATEGY = False
        
    elif current_product == "M626":
        group_scale = 0.7
        USE_TOP_DOWN_STRATEGY = True

    # [Refactor] 5. 并行加载所有服务数据 (全部注入 active_config 和 resource_dir)
    mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
        active_config, 
        resource_dir, 
        _core_revision=current_revision, 
        ema_span=group_ema_span, 
        scaling_factor=group_scale, 
        use_top_down=USE_TOP_DOWN_STRATEGY
    )
    mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
        active_config, 
        resource_dir, 
        _core_revision=current_revision, 
        ema_span=code_ema_span, 
        scaling_factor=code_scale, 
        use_top_down=USE_TOP_DOWN_STRATEGY
    )
    lot_data = YieldAnalysisService.get_lot_defect_rates(
        active_config, 
        resource_dir, 
        _core_revision=current_revision, 
        scaling_factor=code_scale,
        use_top_down=USE_TOP_DOWN_STRATEGY
    )
    sheet_data = YieldAnalysisService.get_sheet_defect_rates(
        active_config, 
        resource_dir, 
        _core_revision=current_revision, 
        scaling_factor=code_scale,
        use_top_down=USE_TOP_DOWN_STRATEGY
    )

    mapping_data = YieldAnalysisService.get_mapping_data(
        active_config, _core_revision=current_revision
    )
    warning_lines = YieldAnalysisService.load_static_warning_lines(
        active_config, resource_dir
    )

# 基础校验
if not all([mwd_group_data, mwd_code_data, lot_data, sheet_data]):
    st.error("部分核心数据加载失败 (数据为空或数据库连接异常)，请检查后台日志。")
    st.stop()

# --- 常量 ---
COLOR_MAP = {
    'Array_Line': "#1930ff",  # Plotly默认的蓝色
    'OLED_Mura': "#ff2828",   # Plotly默认的红色
    'Array_Pixel': "#6fb9ff",   # Plotly默认的浅蓝色
    'array_Line_rate': "#1930ff",  
    'oled_mura_rate': "#ff2828",   
    'array_pixel_rate': "#6fb9ff"   
}

# ==============================================================================
#  自动预警展示区
# ==============================================================================
with st.spinner("正在进行智能预警扫描..."):
    # [Refactor] 6. 注入 config 到 AlertService
    alert_messages = AlertService.get_dashboard_alerts(
        mwd_group_data=mwd_group_data,
        mwd_code_data=mwd_code_data,
        config=active_config,
        resource_dir=resource_dir
    )

# 渲染结果
if alert_messages:
    with st.container(border=True):
        st.error(f"🚨 系统检测到 {len(alert_messages)} 项异常 (包含系统趋势 & 真实批次比对)")
        for msg in alert_messages:
            st.markdown(msg)
else:
    st.success("✅ 系统监测正常：未发现良率突变或批次异常。")

st.divider()

# ==============================================================================
#  第一部分: 宏观监控 (Group级趋势) - 独立筛选
# ==============================================================================
st.subheader("1️⃣ 入库不良率分析 (Group Level)")

if mwd_group_data:
    available_groups = []
    
    # 尝试从 monthly 数据中获取 Group 列表
    ref_df = mwd_group_data.get('monthly')
    if ref_df is not None and not ref_df.empty:
        available_groups = sorted(ref_df['defect_group'].unique().tolist())
    
    dynamic_category_orders = {"defect_group": available_groups}

    c1, _, _ = st.columns(3)
    with c1:
        grp_opts = ["全部Group"] + available_groups
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
                        dynamic_category_orders, 
                        show_input_count=True
                    ),
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
master_df = prepare_union_data_for_filter(mwd_code_data, lot_data, mapping_data)

# 2. 渲染筛选器
selection = create_code_selection_ui(
    source_data=master_df,
    key_prefix="unified_focus",
    rate_threshold=0 
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

    chart_configs = [
        (cd_m, "月度", rc1),
        (cd_w, "周度", rc2),
        (cd_d, "日度", rc3)
    ]

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
#  Row B: Lot 集中性 (批次维度) - 支持交互
# ==============================================================================
lot_details = lot_data.get("code_level_details", {})
df_lot_all = pd.concat(lot_details.values(), ignore_index=True)
df_lot_all['warehousing_time'] = pd.to_datetime(df_lot_all['warehousing_time'], format='%Y%m%d', errors='coerce')
df_lot_curr = df_lot_all[df_lot_all['defect_desc'] == curr_code].copy()
df_lot_curr = df_lot_curr[df_lot_curr['defect_rate'] > 0]

# 辅助列
iso_s = df_lot_curr['warehousing_time'].dt.isocalendar()
df_lot_curr['week_label'] = iso_s.year.astype(str) + '-W' + iso_s.week.map('{:02d}'.format)
df_lot_curr['month_str'] = df_lot_curr['warehousing_time'].dt.strftime('%Y-%m')

with st.container(border=True):
    st.markdown("**B. Lot集中性 (点击蓝色柱体可查看 Sheet 分布)**")
    
    lc1, lc2, lc3 = st.columns(3)
    with lc1: 
        l_sort = st.selectbox("排序:", ["按入库时间(默认)", "按阵列投入时间", "按不良率(降序)"], key="u_lot_sort")
    with lc2:
        l_months = sorted(df_lot_curr['month_str'].dropna().unique(), reverse=True)
        l_sel_m = st.selectbox("月别:", ["全部"] + l_months, key="u_lot_m")
    with lc3:
        l_weeks = sorted(df_lot_curr['week_label'].dropna().unique(), reverse=True)
        l_sel_w = st.selectbox("周别:", ["全部"] + l_weeks, key="u_lot_w")

    if l_sel_m != "全部": df_lot_curr = df_lot_curr[df_lot_curr['month_str'] == l_sel_m]
    if l_sel_w != "全部": df_lot_curr = df_lot_curr[df_lot_curr['week_label'] == l_sel_w]

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
        fig_lot = create_lot_defect_chart(
            df_lot_curr, x_lbl, df_lot_curr['lot_id'].tolist(), curr_warning
        )
        event = st.plotly_chart(fig_lot, use_container_width=True, on_select="rerun", selection_mode="points")
        
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
        target_lot = st.text_input("当前分析 Lot ID:", key="unified_sheet_lot_input", help="点击上方柱图自动填充")
    
    if target_lot:
        group_summary = sheet_data.get("group_level_summary_for_table")
        if group_summary is not None and not group_summary.empty:
            group_summary['warehousing_time'] = pd.to_datetime(group_summary['warehousing_time'], format='%Y%m%d', errors='coerce')
            rate_cols = [c for c in group_summary.columns if c.endswith('_rate') and c!='pass_rate']
            group_summary['total_defect_rate'] = group_summary[rate_cols].sum(axis=1)
            
            df_sheet = group_summary[group_summary['lot_id'] == target_lot]
            
            if df_sheet.empty:
                st.warning(f"未找到 Lot '{target_lot}' 的 Sheet 数据。")
            else:
                with sc2:
                    s_sort = st.selectbox(
                        "Sheet 排序规则:",  
                        ["默认(投入时间)", "按不良率(降序)"], 
                        key="u_sheet_sort"
                    )
                
                if s_sort == "按不良率(降序)":
                    df_sheet = df_sheet.sort_values('total_defect_rate', ascending=False)
                else:
                    df_sheet = df_sheet.sort_values('array_input_time')
                
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
            
            # [核心修改] 构造带入库数量的 Tab 标题
            tab_labels = []
            for b in batches:
                # 从数据中提取该批次的 total_input (任意取一行即可，因为该列是冗余的)
                b_data = df_map[df_map['batch_no'] == b]
                if 'batch_total_input' in b_data.columns:
                    total_in = b_data['batch_total_input'].iloc[0]
                    # 格式化数字，如 1,113,263
                    label = f"{b} (入库: {int(total_in):,})" 
                else:
                    label = f"{b}"
                tab_labels.append(label)
                
            tabs = st.tabs(tab_labels) # 使用新标签
            
           # [新增] 获取热区修改脚本配置
            hotspot_scripts = active_config.processing.get('mapping_hotspot_script', [])

            matrices_cache = {}
            g_max = 0
            
            # [修改] 使用 enumerate 获取索引，以便判断 oldest/latest
            for i, b in enumerate(batches):
                d = df_map[df_map['batch_no'] == b]
                coords = d['panel_id'].apply(parse_panel_id_to_coords)
                d_c = d.assign(r=coords.str[0], c=coords.str[1]).dropna(subset=['r','c'])
                d_c[['r','c']] = d_c[['r','c']].astype(int)
                
                # 1. 生成原始矩阵
                mat = pd.pivot_table(d_c, values='panel_id', index='r', columns='c', aggfunc='count', fill_value=0)
                mat = mat.reindex(index=range(10), columns=range(19), fill_value=0)
                
                # 2. [插入] 应用热区修改逻辑
                # 确定当前批次的身份
                if i == 0:
                    b_idx = 'oldest'
                elif i == len(batches) - 1:
                    b_idx = 'latest'
                else:
                    b_idx = 'middle'
                
                # 调用处理器函数 (它会自动搜索匹配的脚本并应用，如果没有匹配则原样返回)
                mat = apply_hotspot_modification_to_matrix(
                    heatmap_matrix=mat,
                    batch_no=b,
                    code_desc=curr_code,     # 当前选中的 Code
                    batch_index=b_idx,       # 'oldest' / 'latest' / 'middle'
                    script_config_list=hotspot_scripts  # 传入配置列表
                )

                # 3. 存入缓存 (后续绘图使用修改后的 mat)
                matrices_cache[b] = mat
                g_max = max(g_max, mat.max().max())
            
            for i, b in enumerate(batches):
                with tabs[i]:
                    # 绘图逻辑保持不变，使用的是已经修改过的 matrices_cache
                    fig_map = create_mapping_heatmap(matrices_cache[b], f"批次 {b} 热力图", g_max)
                    st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("Mapping 数据源为空。")