# -*- coding: utf-8 -*-
"""
统一预警中心组件

合并趋势异常预警 + Lot 超规预警为单一 expander。
纯展示组件：接受预计算数据，只负责渲染。
"""

import streamlit as st
import pandas as pd
from typing import List, Dict


def compute_lot_oos_records(lot_data, warning_lines, time_period=30):
    """扫描 Lot 超规记录。纯计算，无渲染。"""
    total_recent_lots = 0
    oos_records = []

    if lot_data and "code_level_details" in lot_data:
        all_dfs = []
        for df in lot_data["code_level_details"].values():
            if not df.empty and "lot_id" in df.columns and "warehousing_time" in df.columns:
                all_dfs.append(df)

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df["warehousing_time"] = pd.to_datetime(
                combined_df["warehousing_time"], format="%Y%m%d", errors="coerce"
            )
            max_date = combined_df["warehousing_time"].max()

            if pd.notna(max_date):
                threshold_date = max_date - pd.Timedelta(days=time_period)
                recent_df = combined_df[combined_df["warehousing_time"] >= threshold_date].copy()
                total_recent_lots = recent_df["lot_id"].nunique()

                for _, row in recent_df.iterrows():
                    code = str(row.get("defect_desc", "")).strip()
                    rate = row.get("defect_rate", 0.0)
                    spec_dict = warning_lines.get(code)
                    if not spec_dict:
                        continue
                    spec_limit = spec_dict.get("upper", 1.0)
                    if rate > spec_limit:
                        w_time = row.get("warehousing_time")
                        w_time_str = w_time.strftime("%Y/%m/%d") if pd.notna(w_time) else "-"
                        a_time = pd.to_datetime(row.get("array_input_time"), errors="coerce")
                        a_time_str = a_time.strftime("%Y/%m/%d") if pd.notna(a_time) else "-"
                        defect_count = int(row.get("defect_panel_count", 0)) if pd.notna(row.get("defect_panel_count")) else 0
                        spec_str = f"{(spec_limit or 0) * 100:.2f}%" if spec_limit < 1.0 else "无限制"
                        rate_str = f"{(rate or 0) * 100:.2f}%"
                        oos_records.append({
                            "超规 Lot ID": row.get("lot_id", "Unknown"),
                            "异常 Code": code,
                            "管控规格线": spec_str,
                            "实际不良率": rate_str,
                            "不良panel数": defect_count,
                            "入库时间": w_time_str,
                            "阵列投入时间": a_time_str,
                        })

    return oos_records, total_recent_lots


def build_trend_context(alert_service_result, mwd_code_data, mwd_group_data):
    """
    从预警数据和原始数据中提取趋势监控上下文。
    由页面层调用，组装后传给 render_alert_center。

    Returns:
        dict: {
            "code_count": int,
            "group_count": int,
            "monthly_latest": str | None,
            "monthly_prev": str | None,
            "weekly_latest": str | None,
            "weekly_prev": str | None,
        }
    """
    ctx = {"code_count": 0, "group_count": 0,
           "monthly_latest": None, "monthly_prev": None,
           "weekly_latest": None, "weekly_prev": None}

    # Count codes and groups from monthly data
    monthly = mwd_code_data.get("monthly") if mwd_code_data else None
    if monthly is not None and not monthly.empty:
        ctx["code_count"] = monthly["defect_desc"].nunique()
    group_monthly = mwd_group_data.get("monthly") if mwd_group_data else None
    if group_monthly is not None and not group_monthly.empty:
        ctx["group_count"] = group_monthly["defect_group"].nunique()

    # Extract latest periods from monthly data
    if monthly is not None and not monthly.empty and "time_period" in monthly.columns:
        periods = sorted(monthly["time_period"].unique())
        if len(periods) >= 2:
            ctx["monthly_latest"] = str(periods[-1])
            ctx["monthly_prev"] = str(periods[-2])
        elif len(periods) == 1:
            ctx["monthly_latest"] = str(periods[0])

    # Extract latest periods from weekly data
    weekly = mwd_code_data.get("weekly") if mwd_code_data else None
    if weekly is not None and not weekly.empty and "time_period" in weekly.columns:
        periods = sorted(weekly["time_period"].unique())
        if len(periods) >= 2:
            ctx["weekly_latest"] = str(periods[-1])
            ctx["weekly_prev"] = str(periods[-2])
        elif len(periods) == 1:
            ctx["weekly_latest"] = str(periods[0])

    return ctx


def render_alert_center(trend_alerts, trend_context, oos_records, total_recent_lots, time_period=30):
    """
    统一预警中心：合并趋势异常 + Lot 超规预警。

    Args:
        trend_alerts: AlertService.get_dashboard_alerts 返回的预警消息列表
        trend_context: build_trend_context 返回的监控上下文
        oos_records: compute_lot_oos_records 返回的超规记录列表
        total_recent_lots: 近 N 天总 Lot 数
        time_period: 时间窗口天数
    """
    oos_df = pd.DataFrame(oos_records)
    oos_count = oos_df["超规 Lot ID"].nunique() if not oos_df.empty else 0
    has_trend = len(trend_alerts) > 0
    has_oos = oos_count > 0
    has_any = has_trend or has_oos

    ctx = trend_context or {}

    # --- 构建监控概览文案 ---
    # Lot 过货信息
    lot_line = f"近 {time_period} 天过货 {total_recent_lots} 个 Lot"
    if has_oos and total_recent_lots > 0:
        oos_rate_str = f"{(oos_count / total_recent_lots * 100):.1f}%"
        lot_line += f"，其中 {oos_count} 个超规 ({oos_rate_str})"
    else:
        lot_line += "，全部在规格线内"

    # Code 趋势监控信息
    parts = []
    if ctx.get("group_count", 0) > 0:
        parts.append(f"{ctx['group_count']} 个 Group")
    if ctx.get("code_count", 0) > 0:
        parts.append(f"{ctx['code_count']} 个 Code")
    trend_scope = " × ".join(parts) if parts else "全维度"

    monthly_info = ""
    if ctx.get("monthly_latest") and ctx.get("monthly_prev"):
        monthly_info = f"月环比 ({ctx['monthly_latest']} vs {ctx['monthly_prev']})"
    elif ctx.get("monthly_latest"):
        monthly_info = f"最新月份 {ctx['monthly_latest']}"

    weekly_info = ""
    if ctx.get("weekly_latest") and ctx.get("weekly_prev"):
        weekly_info = f"周环比 ({ctx['weekly_latest']} vs {ctx['weekly_prev']})"
    elif ctx.get("weekly_latest"):
        weekly_info = f"最新周 {ctx['weekly_latest']}"

    comparison_parts = [p for p in [monthly_info, weekly_info] if p]
    comparison_str = "、".join(comparison_parts) if comparison_parts else "趋势监控"

    if has_trend:
        trend_line = f"已监控 {trend_scope}，{comparison_str}，发现 {len(trend_alerts)} 项异常"
    else:
        trend_line = f"已监控 {trend_scope}，{comparison_str}，无异常升高"

    # --- 渲染 ---
    with st.expander(f"智能预警中心（近{time_period}天）", expanded=has_any):
        if has_any:
            parts_summary = []
            if has_trend:
                parts_summary.append(f"{len(trend_alerts)} 项趋势异常")
            if has_oos:
                oos_rate_summary = f"{(oos_count / total_recent_lots * 100):.1f}%" if total_recent_lots > 0 else "0.0%"
                parts_summary.append(f"{oos_count} 个超规 Lot ({oos_rate_summary})")
            st.error("检测到: " + " + ".join(parts_summary) + "，请关注！")
        else:
            st.success("系统监测正常")

        # 监控概览卡片（始终显示）
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown(f"**Lot 过货**")
                st.caption(lot_line)
        with col2:
            with st.container(border=True):
                st.markdown(f"**Code 趋势**")
                st.caption(trend_line)

        # 趋势异常详情
        if has_trend:
            st.markdown("---")
            st.markdown("#### 趋势异常详情")
            with st.container(border=True):
                for msg in trend_alerts:
                    st.markdown(msg)

        # Lot 超规明细
        if has_oos:
            st.markdown("---")
            st.markdown("#### Lot 超规明细")
            with st.container(border=True):
                st.dataframe(
                    oos_df,
                    column_config={
                        "超规 Lot ID": st.column_config.TextColumn("超规 Lot ID"),
                        "入库时间": st.column_config.TextColumn("入库时间"),
                        "阵列投入时间": st.column_config.TextColumn("阵列投入时间"),
                        "异常 Code": st.column_config.TextColumn("异常 Code"),
                        "不良panel数": st.column_config.NumberColumn("不良panel数", format="%d"),
                        "管控规格线": st.column_config.TextColumn("管控规格线"),
                        "实际不良率": st.column_config.TextColumn("实际不良率"),
                    },
                    hide_index=True,
                    use_container_width=True,
                )
