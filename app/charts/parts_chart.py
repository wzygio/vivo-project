# app/charts/parts_chart.py
"""
关键备件趋势图 - Plotly 可视化组件。

数据源: 从 Parquet 快照 (data/equipment/) 读取真实数据。
"""

import glob
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

SNAPSHOT_GLOB = "data/equipment/part_life_snapshot_*.parquet"


def generate_trend_data(
    factory: str,
    layer: str,
    part_type: str,
    spec_df: pd.DataFrame,
    station: str = "",
    machine: str = "",
    days: int = 90,
) -> pd.DataFrame:
    """
    从 Parquet 快照加载指定备件的真实趋势数据。

    Args:
        factory: 厂别
        layer: 膜层
        part_type: 备件类型 (Target / Mask)
        spec_df: 规格基线 DataFrame
        station: 站点 (step_id)，为空则匹配所有
        machine: 机台号-腔室 (sub_equip_id)，为空则匹配所有
        days: 回溯天数

    Returns:
        pd.DataFrame: 包含 日期, 实际数据, 寿命规格, 预警线
                     无数据时返回空 DataFrame
    """
    if spec_df is None or spec_df.empty:
        return pd.DataFrame()

    # 查找匹配的规格行
    matched_spec = spec_df[
        (spec_df["厂别"] == factory)
        & (spec_df["膜层"] == layer)
        & (spec_df["备件类型"] == part_type)
    ]
    if matched_spec.empty:
        return pd.DataFrame()

    # 提取 (站点, 机台号-腔室, 参数名称) 组合
    pairs = matched_spec[["站点", "机台号-腔室", "参数名称"]].drop_duplicates()

    # 如果指定了站点和机台，只取匹配的行
    if station:
        pairs = pairs[pairs["站点"] == station]
    if machine:
        pairs = pairs[pairs["机台号-腔室"] == machine]

    if pairs.empty:
        return pd.DataFrame()

    # 获取寿命规格（取第一个匹配行的值）
    raw_spec = str(matched_spec.iloc[0].iloc[5])
    try:
        spec_limit = float(re.sub(r"[^\d.]", "", raw_spec))
    except (ValueError, TypeError):
        spec_limit = 840.0
    if pd.isna(spec_limit) or spec_limit <= 0:
        spec_limit = 840.0
    warn_line = spec_limit * 0.9

    # 查找快照文件
    snapshot_files = glob.glob(SNAPSHOT_GLOB)
    if not snapshot_files:
        return pd.DataFrame()

    try:
        snap = pd.read_parquet(snapshot_files[0])
    except Exception as e:
        logger.warning(f"Failed to read snapshot: {e}")
        return pd.DataFrame()

    if snap.empty:
        return pd.DataFrame()

    # 按 (站点, 机台号-腔室) 过滤快照
    stations_list = pairs["站点"].unique().tolist()
    machines_list = pairs["机台号-腔室"].unique().tolist()

    mask = (
        snap["step_id"].isin(stations_list)
        & snap["sub_equip_id"].isin(machines_list)
    )
    filtered = snap[mask]

    if filtered.empty:
        return pd.DataFrame()

    # 按参数名称 LIKE 模式过滤
    patterns = pairs["参数名称"].unique().tolist()
    regex_parts = []
    for pat in patterns:
        pat_str = str(pat).strip()
        if not pat_str:
            continue
        escaped = re.escape(pat_str)
        escaped = escaped.replace("%", ".*")
        escaped = escaped.replace("_", ".")
        regex_parts.append("^" + escaped + "$")

    if regex_parts:
        combined = "|".join(regex_parts)
        param_mask = filtered["param_name"].astype(str).str.match(
            combined, case=False, na=False
        )
        filtered = filtered[param_mask]

    if filtered.empty:
        return pd.DataFrame()

    # 时间窗口过滤
    cutoff = datetime.now() - timedelta(days=days)
    time_mask = filtered["glass_start_time"] >= cutoff
    filtered = filtered[time_mask]

    if filtered.empty:
        return pd.DataFrame()

    # 按日期聚合（每日取均值）
    filtered["date"] = filtered["glass_start_time"].dt.date
    daily = filtered.groupby("date")["value"].mean().reset_index()
    daily.columns = ["日期", "实际数据"]
    daily["日期"] = pd.to_datetime(daily["日期"])

    # 填充缺失日期（前向填充）
    date_range = pd.date_range(
        start=cutoff.date(), end=datetime.now().date(), freq="D"
    )
    daily = daily.set_index("日期").reindex(date_range)
    daily["实际数据"] = daily["实际数据"].ffill()
    daily = daily.reset_index()
    daily.columns = ["日期", "实际数据"]

    # 添加规格线和预警线
    daily["寿命规格"] = spec_limit
    daily["预警线"] = warn_line

    # 格式化日期为字符串
    daily["日期"] = daily["日期"].dt.strftime("%Y-%m-%d")

    return daily


def create_parts_trend_chart(
    df_trend: pd.DataFrame,
    factory: str,
    layer: str,
    part_type: str,
) -> go.Figure:
    """
    创建 Plotly 趋势图。

    Args:
        df_trend: 趋势 DataFrame (含 日期, 实际数据, 寿命规格, 预警线)
        factory: 厂别
        layer: 膜层
        part_type: 备件类型

    Returns:
        go.Figure
    """
    if df_trend.empty:
        fig = go.Figure()
        fig.update_layout(
            title="无数据",
            height=400,
        )
        return fig

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["实际数据"],
        mode="lines+markers",
        name="实际数据 (HR)",
        line=dict(color="#1f77b4", width=3),
        marker=dict(size=6),
        hovertemplate="日期: %{x}<br>实际: %{y} HR<extra></extra>",
    ))

    spec_val = df_trend["寿命规格"].iloc[0]
    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["寿命规格"],
        mode="lines",
        name=f"寿命规格 ({spec_val:.0f} HR)",
        line=dict(color="#d62728", width=2, dash="dash"),
        hovertemplate="寿命规格: %{y} HR<extra></extra>",
    ))

    warn_val = df_trend["预警线"].iloc[0]
    fig.add_trace(go.Scatter(
        x=df_trend["日期"],
        y=df_trend["预警线"],
        mode="lines",
        name=f"预警线 ({warn_val:.0f} HR)",
        line=dict(color="#ff7f0e", width=1.5, dash="dot"),
        hovertemplate="预警线: %{y} HR<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text=f"{factory} - {layer} - {part_type} 备件寿命趋势",
            x=0.5,
            xanchor="center",
        ),
        xaxis_title="日期",
        yaxis_title="累计寿命 (HR)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(l=40, r=40, t=80, b=40),
        height=400,
    )

    return fig