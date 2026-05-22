# app/charts/parts_chart.py
"""
📈 关键备件趋势图绘制模块

符合项目前端约定：所有图表逻辑（Plotly/Altair）统一收拢于 app/charts 目录。
"""

import hashlib
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import plotly.graph_objects as go


def generate_mock_trend_data(
    factory: str,
    layer: str,
    part_type: str,
    spec_df: pd.DataFrame = None,
    days: int = 30,
) -> pd.DataFrame:
    """
    基于确定性的随机种子（哈希）生成高保真的关键备件寿命 Mock 趋势数据。
    
    Args:
        factory: 厂别
        layer: 膜层
        part_type: 备件类型
        spec_df: 备件规格 DataFrame（用以动态加载寿命规格和预警值）
        days: 模拟天数
        
    Returns:
        pd.DataFrame: 包含日期、实际数据、寿命规格、预警值的 DataFrame
    """
    # 采用确定性的随机种子，通过哈希来生成，避免页面刷新造成走势突变
    seed_str = f"{factory}_{layer}_{part_type}"
    seed = int(hashlib.md5(seed_str.encode("utf-8")).hexdigest(), 16) % (10**8)
    np_random = np.random.RandomState(seed)
    
    # 最近 days 天的日期列表
    end_date = datetime.now()
    dates = [(end_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    dates.reverse()
    
    # 模拟实际数据随时间累积与更换重置的过程
    base_val = np_random.randint(50, 300)
    daily_increment = np_random.uniform(15, 30)
    replace_cycle = np_random.randint(18, 28)
    
    values = []
    current_val = float(base_val)
    for i in range(days):
        current_val += daily_increment + float(np_random.normal(0, 2))
        current_val = max(0.0, current_val)
        
        # 在 replace_cycle 左右模拟更换重置
        if i > 0 and i % replace_cycle == 0:
            current_val = float(np_random.uniform(10, 60))
            
        values.append(round(current_val, 1))
        
    df_mock = pd.DataFrame({
        "日期": dates,
        "实际数据": values,
        "寿命规格": [840.0] * days,
        "预警值": [840.0 * 0.8] * days
    })
    
    # 尝试匹配真实规格
    if spec_df is not None and not spec_df.empty:
        try:
            match = spec_df[
                (spec_df["厂别"] == factory) & 
                (spec_df["膜层"] == layer) & 
                (spec_df["备件类型"] == part_type)
            ]
            if not match.empty:
                spec_limit = float(match.iloc[0]["寿命规格"])
                warn_rate = float(match.iloc[0]["预警值"]) / 100.0 if "预警值" in match.columns else 0.8
                df_mock["寿命规格"] = spec_limit
                df_mock["预警值"] = spec_limit * warn_rate
        except Exception:
            pass
            
    return df_mock


def create_parts_trend_chart(
    df_trend: pd.DataFrame,
    factory: str,
    layer: str,
    part_type: str,
) -> go.Figure:
    """
    绘制关键备件实际寿命数据随时间变化的 Plotly 趋势图。
    
    Args:
        df_trend: 趋势数据 DataFrame
        factory: 厂别
        layer: 膜层
        part_type: 备件类型
        
    Returns:
        go.Figure: Plotly Figure 实例
    """
    fig = go.Figure()
    
    # 1. 实际数据折线
    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["实际数据"],
        mode="lines+markers",
        name="实际数据 (HR)",
        line=dict(color="#1f77b4", width=3),
        marker=dict(size=6),
        hovertemplate="日期: %{x}<br>实际数据: %{y} HR<extra></extra>"
    ))
    
    # 2. 寿命规格线
    spec_val = df_trend["寿命规格"].iloc[0]
    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["寿命规格"],
        mode="lines",
        name=f"寿命规格 ({spec_val:.0f} HR)",
        line=dict(color="#d62728", width=2, dash="dash"),
        hovertemplate="寿命规格: %{y} HR<extra></extra>"
    ))
    
    # 3. 预警线
    warn_val = df_trend["预警值"].iloc[0]
    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["预警值"],
        mode="lines",
        name=f"预警线 ({warn_val:.0f} HR)",
        line=dict(color="#ff7f0e", width=1.5, dash="dot"),
        hovertemplate="预警线: %{y} HR<extra></extra>"
    ))
    
    fig.update_layout(
        title=dict(
            text=f"📊 {factory} - {layer} - {part_type} 实际寿命变化趋势图",
            x=0.5,
            xanchor="center"
        ),
        xaxis_title="日期",
        yaxis_title="实际数值 (HR)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=80, b=40),
        height=400,
    )
    
    return fig
