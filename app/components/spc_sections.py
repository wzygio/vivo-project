import streamlit as st
import pandas as pd
import numpy as np
from streamlit_echarts import st_echarts
from pydantic import BaseModel, Field
from app.charts.spc_chart import get_spc_summary_echarts_option

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
    st.markdown("### 🎛️ 监控控制台") # UI 标题
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
    """渲染大盘汇总图表与汇总表""" # 汇总视图呈现
    if summary_df.empty:
        st.warning("暂无全局汇总数据") # 兜底校验
        return

    st.markdown("### 📊 工厂SPC特性报警情况 (全局汇总)") # 标题
    
    # 1. 渲染 Echarts 双 Y 轴图
    echarts_option = get_spc_summary_echarts_option(summary_df) # 获取配置字典
    st_echarts(options=echarts_option, height="450px") # 渲染 Echarts
    
    # 2. 渲染汇总数据表 (需转置并安全格式化)
    st.markdown("#### 报警率明细总表") # 表格标题
    
    # [核心安全策略]：生成副本进行视图格式化，绝不污染源数据
    view_df = summary_df.copy().set_index('time_group').T # 转置为行:指标, 列:时间
    
    # 格式化函数：浮点数转百分比，NaN 转 '/'
    def safe_format(val, is_rate=False):
        if pd.isna(val): return "/" # 空值处理
        if is_rate: return f"{val * 100:.2f}%" # 率指标格式化
        return str(int(val)) # 绝对值格式化

    rate_rows = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC'] # 需要转百分比的行
    for row_idx in view_df.index:
        is_rate = row_idx in rate_rows # 判断是否为率指标
        view_df.loc[row_idx] = view_df.loc[row_idx].apply(lambda x: safe_format(x, is_rate)) # 逐行执行格式化
        
    st.dataframe(view_df, use_container_width=True) # 渲染转置后的 DataFrame

def render_spc_detail_section(detail_df: pd.DataFrame, filter_state: SpcFilterState):
    """渲染多维下钻透视表""" # 明细透视视图
    st.markdown("### 🗂️ By产品-By工厂 报警明细") # 标题
    
    if detail_df.empty:
        st.info("所选范围内无明细数据。") # 兜底提示
        return
        
    # 应用前端过滤器 (仅做显示过滤，不改动计算)
    filtered_df = detail_df[
        (detail_df['prod_code'].isin(filter_state.selected_products)) & 
        (detail_df['factory'].isin(filter_state.selected_factories))
    ] # DataFrame 掩码过滤
    
    # 格式化视图 DataFrame
    view_df = filtered_df.copy() # 拷贝副本
    rate_cols = ['OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC'] # 率列名单
    
    for col in view_df.columns:
        if col in rate_cols:
            view_df[col] = view_df[col].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "/") # 率列格式化
        elif '片' in col or '量' in col:
            view_df[col] = view_df[col].apply(lambda x: str(int(x)) if pd.notna(x) else "/") # 片数列格式化

    # 构建 MultiIndex 透视表
    pivot_df = view_df.pivot_table(
        index=['prod_code', 'factory'], # 多级行索引
        columns=['time_group'], # 时间节点列
        values=['过货量', 'OOS片数', 'SOOS片数', 'OOC片数', 'OOS', 'SOOS', 'OOC', 'OOS+SOOS', 'OOS+SOOS+OOC'], # 聚合字段
        aggfunc=lambda x: x.iloc[0] # 因为数据已经是唯一的粒度，直接取第一条即可
    ) # 生成透视表
    
    st.dataframe(pivot_df, use_container_width=True, height=500) # 渲染最终透视表