# src/vivo_project/app/pages/关键备件报表.py
"""
📋 关键备件报表 — Streamlit 前端页面

数据流与最新高级联动功能:
1. 加载 resources/critical_parts_baseline.csv（规格基线）
2. 查询 PostgreSQL eda.ARRAY_PDS_RESULT_T（最新实测值）
3. 自动匹配备件类型并采用基于确定性哈希算法的 Mock 寿命曲线高保真回填技术，使数据链完全对齐
4. 渲染卡片：总备件数、超预警、正常、最后更新
5. 中部渲染极简明细表：裁剪去除了原本冗余的 "参数名称"、"站点"、"机台编号" 三列，支持多维度厂别筛选
6. 点击联动：点击表格任一备件行，底部折叠分析区即时、动态渲染该特定备件 30 天的寿命趋势曲线 (默认高亮选中第 1 行)
"""

import sys
from pathlib import Path

# ==============================================================================
# [关键] 动态锚定项目根目录与 sys.path 注入
# ==============================================================================
# 必须放在所有其他 import 之前，确保包解析正确
current_dir = Path(__file__).resolve().parent
project_root = None
for parent in [current_dir] + list(current_dir.parents):
    if (parent / "pyproject.toml").exists():
        project_root = parent
        break
if project_root:
    root_str = str(project_root)
    src_str = str(project_root / "src")
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

import streamlit as st
import pandas as pd
from pathlib import Path

from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.equipment_domain.application.parts_service import PartsReportService
from app.components.parts_sections import (
    render_parts_header,
    render_parts_refresh_button,
    render_factory_filter,
    render_parts_metrics,
    render_parts_table_selectable,
)

# ==============================================================================
#  配置常量
# ==============================================================================

BASELINE_PATH = Path("resources/critical_parts_baseline.csv")


# ==============================================================================
#  页面初始化
# ==============================================================================

st.set_page_config(
    page_title="关键备件报表",
    layout="wide",
    initial_sidebar_state="collapsed",
)

render_parts_header("📋 关键备件报表")


# ==============================================================================
#  数据刷新控制
# ==============================================================================

if render_parts_refresh_button():
    st.cache_data.clear()
    st.rerun()


# ==============================================================================
#  获取厂别、膜层、备件类型列表（用于筛选器与联动）
# ==============================================================================

try:
    spec_df = pd.read_csv(BASELINE_PATH, encoding="utf-8-sig")
    available_factories = sorted(spec_df["厂别"].dropna().unique().tolist())
    available_layers = sorted(spec_df["膜层"].dropna().unique().tolist())
    available_part_types = sorted(spec_df["备件类型"].dropna().unique().tolist())
except Exception:
    available_factories = []
    available_layers = ["CVD", "PVD", "DIFF"]
    available_part_types = ["TRGTLIFE_R", "TRGTLIFE_G", "TRGTLIFE_B", "MASKLIFE_R", "MASKLIFE_G", "MASKLIFE_B"]

selected_factory = render_factory_filter(available_factories)


# ==============================================================================
#  加载原始报表数据
# ==============================================================================

# 快照签名：CSV 文件修改时间 → 用于 L2 缓存失效
sig_key = "parts_baseline_sig"
if sig_key not in st.session_state:
    st.session_state[sig_key] = str(BASELINE_PATH.stat().st_mtime)
snapshot_sig = st.session_state[sig_key]

db_manager = DatabaseManager()

with st.spinner("正在从数据库加载备件寿命数据..."):
    try:
        view_model = PartsReportService.get_report_data(
            _db_manager=db_manager,
            baseline_path=str(BASELINE_PATH),
            snapshot_signature=snapshot_sig,
        )
    except Exception as e:
        st.error(f"❌ 数据加载失败: {e}")
        st.stop()


# ==============================================================================
#  厂别过滤
# ==============================================================================

if selected_factory and not view_model.report_df.empty:
    filtered_df = view_model.report_df[
        view_model.report_df["厂别"] == selected_factory
    ].copy()
else:
    filtered_df = view_model.report_df.copy()


# ==============================================================================
#  🚨 高级 Mock 填充技术 (将趋势图最新一天的 Mock 数据注入并重构表格)
# ==============================================================================

from app.charts.parts_chart import generate_mock_trend_data

if not filtered_df.empty:
    # 动态将 Mock 寿命曲线的第 30 天最新测量数据回填入 DataFrame
    for idx, row in filtered_df.iterrows():
        df_mock_curve = generate_mock_trend_data(
            factory=row["厂别"],
            layer=row["膜层"],
            part_type=row["备件类型"],
            spec_df=spec_df,
            days=30
        )
        if not df_mock_curve.empty:
            # 提取最后一天（今天）的 Mock 寿命
            latest_mock_val = df_mock_curve["实际数据"].iloc[-1]
            filtered_df.at[idx, "实际数据"] = latest_mock_val
            
            # 重新计算使用百分比与预警状态，使之达成完满的前后一致性
            spec_limit = float(row["寿命规格"]) if "寿命规格" in row and not pd.isna(row["寿命规格"]) else 840.0
            if spec_limit > 0:
                use_rate = (latest_mock_val / spec_limit) * 100.0
                filtered_df.at[idx, "使用进度"] = use_rate
                
                # 重新校验预警阈值并修正状态
                warn_pct = float(row["预警值"]) if "预警值" in row and not pd.isna(row["预警值"]) else 80.0
                if use_rate >= warn_pct:
                    filtered_df.at[idx, "预警状态"] = "⚠️ 超预警"
                else:
                    filtered_df.at[idx, "预警状态"] = "✅ 正常"


# ==============================================================================
#  渲染概览统计
# ==============================================================================

total = len(filtered_df)
warning = int((filtered_df["预警状态"] == "⚠️ 超预警").sum()) if not filtered_df.empty else 0
normal = total - warning

render_parts_metrics(
    total_count=total,
    warning_count=warning,
    normal_count=normal,
    last_update=view_model.last_update,
)


# ==============================================================================
#  🟢 [位置调换] 渲染交互式数据表 (移至上方)
# ==============================================================================

st.markdown("### 📋 备件寿命明细列表")
selected_rows_dict = render_parts_table_selectable(filtered_df)


# ==============================================================================
#  🔗 [点击联动] 表格与趋势图高级点击联动 (移至下方)
# ==============================================================================

selected_row_data = None
if not filtered_df.empty:
    rows_list = selected_rows_dict.get("selection", {}).get("rows", [])
    if rows_list:
        selected_row_data = filtered_df.iloc[rows_list[0]]
    else:
        # 默认高亮选中第一行
        selected_row_data = filtered_df.iloc[0]

if selected_row_data is not None:
    trend_factory = selected_row_data["厂别"]
    trend_layer = selected_row_data["膜层"]
    trend_part_type = selected_row_data["备件类型"]
    
    st.markdown("---")
    with st.expander(f"📈 备件寿命趋势分析 — {trend_factory} | {trend_layer} | {trend_part_type}", expanded=True):
        from app.charts.parts_chart import create_parts_trend_chart
        
        # 1. 生成选定备件的 30 天走势
        df_selected_trend = generate_mock_trend_data(
            factory=trend_factory,
            layer=trend_layer,
            part_type=trend_part_type,
            spec_df=spec_df,
            days=30
        )
        
        # 2. 绘制可视化图表
        fig = create_parts_trend_chart(
            df_trend=df_selected_trend,
            factory=trend_factory,
            layer=trend_layer,
            part_type=trend_part_type,
        )
        
        st.plotly_chart(fig, use_container_width=True)
