import streamlit as st
import pandas as pd
import numpy as np
from streamlit_echarts import st_echarts
from pydantic import BaseModel, Field
from app.charts.spc_chart import get_spc_summary_echarts_option
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode

# --------------------------------------------------------------------------
# 状态模型定义 (Type-Safe Session State)
# --------------------------------------------------------------------------
class SpcFilterState(BaseModel):
    """用于管理 SPC 看板过滤器的强类型状态模型""" # 抛弃硬编码字典，引入 Pydantic 规范
    selected_products: list[str] = Field(default_factory=list) # 选中的产品型号
    selected_factories: list[str] = Field(default_factory=list) # 选中的厂别

# --------------------------------------------------------------------------
# UI 渲染区块
# --------------------------------------------------------------------------
def render_spc_control_panel(available_products: list[str], available_factories: list[str]) -> SpcFilterState:
    """渲染顶部筛选项，并返回强类型状态对象""" # 顶层控制面板
    col1, col2, col3 = st.columns(3) # 三列等宽布局
    
    with col1:
        # 基准日期 (当前由 Service 接管，此处仅做展示或附加过滤)
        st.date_input("基准日期", value="today", disabled=True, help="统一采用系统级前三个月窗口") # 锁定日期控件
    with col2:
        prods = st.multiselect("产品型号", options=available_products, default=available_products) # 多选产品
    with col3:
        facs = st.multiselect("厂别", options=available_factories, default=available_factories) # 多选厂别
        
    return SpcFilterState(selected_products=prods, selected_factories=facs) # 返回强类型实例

def render_spc_summary_section(summary_df: pd.DataFrame):
    """渲染大盘汇总图表与汇总表"""
    if summary_df.empty:
        st.warning("暂无全局汇总数据")
        return

    st.markdown("#### 📊 SPC报警率汇总表")
    
    # 1. 渲染 Echarts 双 Y 轴图
    echarts_option = get_spc_summary_echarts_option(summary_df)
    st_echarts(options=echarts_option, height="450px")
    
    # 2. 渲染汇总数据表
    view_df = summary_df.copy().set_index('time_group').T
    
    def safe_format(val, is_rate=False):
        if pd.isna(val): return "/"
        if is_rate: return f"{val * 100:.2f}%"
        return str(int(val))

    rate_rows = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']
    for row_idx in view_df.index:
        is_rate = row_idx in rate_rows
        view_df.loc[row_idx] = view_df.loc[row_idx].apply(lambda x: safe_format(x, is_rate))

    # [UI 优化] 定义列着色器 (按时间粒度隔离背景色)
    def highlight_time_columns(series):
        col_name = str(series.name)
        if 'M' in col_name:
            return ['background-color: rgba(230, 240, 255, 0.6)'] * len(series) # 月度：淡蓝
        elif 'W' in col_name:
            return ['background-color: rgba(255, 245, 230, 0.6)'] * len(series) # 周度：淡橙
        else:
            return ['background-color: transparent'] * len(series)               # 日度：透明

    # 应用 Pandas Styler
    styled_summary = view_df.style.apply(highlight_time_columns, axis=0)
    st.dataframe(styled_summary, use_container_width=True)

def render_spc_detail_section(detail_df: pd.DataFrame, filter_state: SpcFilterState):
    """渲染多维下钻透视表 (引入 Ag-Grid 企业级表格引擎)"""
    st.markdown("#### 🗂️ SPC报警率明细表")
    
    if detail_df.empty:
        st.info("所选范围内无明细数据。")
        return
        
    filtered_df = detail_df[
        (detail_df['prod_code'].isin(filter_state.selected_products)) & 
        (detail_df['factory'].isin(filter_state.selected_factories))
    ]
    
    view_df = filtered_df.copy()
    rate_cols = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']
    
    for col in view_df.columns:
        if col in rate_cols:
            view_df[col] = view_df[col].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "/")
        elif '片' in col or '量' in col:
            view_df[col] = view_df[col].apply(lambda x: str(int(x)) if pd.notna(x) else "/")

    # 1. 强制时间轴绝对排序
    ordered_time_groups = detail_df['time_group'].unique().tolist()
    view_df['time_group'] = pd.Categorical(view_df['time_group'], categories=ordered_time_groups, ordered=True)
    ordered_metrics = ['过货量', 'OOS片数', 'SOOS片数', 'OOC片数', 'OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']

    # 2. 基础透视与矩阵旋转
    pivot_df = view_df.pivot_table(
        index=['prod_code', 'factory'],
        columns=['time_group'],
        values=ordered_metrics,
        aggfunc=lambda x: x.iloc[0],
        observed=False
    )
    
    stacked_df = pivot_df.stack(level=0, dropna=False)
    stacked_df.index.names = ['品名', '工厂', '报警类型']
    stacked_df = stacked_df.reindex(ordered_metrics, level='报警类型')

    # =========================================================================
    # [核心重构] Ag-Grid 引擎适配层
    # =========================================================================
    # A. 压平 DataFrame 以适配 JSON 序列化
    flat_df = stacked_df.reset_index()
    
    # [新增修复] 精确填充矩阵旋转后产生的 NaN 空值
    time_cols = [col for col in flat_df.columns if col not in ['品名', '工厂', '报警类型']]
    rate_cols = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC']
    
    # 识别哪些行是百分比，哪些行是计数
    is_rate_row = flat_df['报警类型'].isin(rate_cols)
    
    for col in time_cols:
        # 为“率”指标填充 0.00%
        flat_df.loc[is_rate_row, col] = flat_df.loc[is_rate_row, col].fillna("0.00%")
        # 为“片数/过货量”指标填充 0
        flat_df.loc[~is_rate_row, col] = flat_df.loc[~is_rate_row, col].fillna("0")

    flat_df.columns = flat_df.columns.astype(str) # 确保所有列名都是纯字符串

    # B. 构建 Ag-Grid 配置字典
    gb = GridOptionsBuilder.from_dataframe(flat_df)

    # C. 配置多级行折叠 (Row Grouping)
    gb.configure_column("品名", rowGroup=True, hide=True)
    gb.configure_column("工厂", rowGroup=True, hide=True)
    
    # D. 固定指标列在左侧，并加粗显示
    gb.configure_column(
        "报警类型", 
        pinned="left", 
        width=220, # [修复 2] 显式增加该列的像素宽度，防止过窄
        cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'}
    )

    # E. 为时间列配置动态着色器
    for col in flat_df.columns:
        if col in ['品名', '工厂', '报警类型']:
            continue
            
        bg_color = 'transparent'
        if 'M' in col:
            bg_color = 'rgba(230, 240, 255, 0.6)'  # 月度浅蓝
        elif 'W' in col:
            bg_color = 'rgba(255, 245, 230, 0.6)'  # 周度浅橙
            
        gb.configure_column(col, cellStyle={'backgroundColor': bg_color})

    grid_options = gb.build()

    # =========================================================================
    # [新增 UX 优化] 核心配置参数覆盖
    # =========================================================================
    # [修复 1] 设置组默认展开级别。-1 表示展开所有层级 (品名 -> 工厂 全部铺开)
    grid_options['groupDefaultExpanded'] = -1 

    # F. 优化折叠栏 (Group Column) 的 UI
    grid_options['autoGroupColumnDef'] = {
        'headerName': '🏭 产品/工厂',
        'width': 200,    # [修复 2] 显式收缩分组列的宽度，留出更多空间给右侧数据
        'pinned': 'left',
        'cellRendererParams': {
            'suppressCount': True # 隐藏默认的 (9) 折叠计数
        }
    }

    # 3. 渲染企业级表格
    AgGrid(
        flat_df,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        # [修复 2] 注释掉强制内容自适应模式，尊重我们上面设置的 explicit width 像素值
        # columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS, 
        theme='streamlit',
        height=650
    )