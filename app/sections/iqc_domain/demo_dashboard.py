"""Two workbook-based, local-only demonstration pages."""

import logging

import streamlit as st

from app.sections.iqc_domain.report_table import build_report_table
from src.iqc_domain.application.demo_reports import filter_demo_report, load_demo_report

FILTER_COLUMNS = {
    "inspection": ("项目名", "工厂", "物料分类", "特性项目"),
    "lifetime": ("项目", "画面", "类别"),
}


def render_demo_dashboard(report: str) -> None:
    st.caption("示例数据展示 · 使用原工作表中的假数据")
    try:
        frame = load_demo_report(report)
    except (OSError, ValueError, KeyError):
        logging.exception("Unable to load IQC demo resource: %s", report)
        st.error("示例数据读取失败，请检查 IQC 展示资源是否完整。")
        return

    date_column = "报检日期" if report == "inspection" else "批次"
    with st.container(border=True):
        selections = {}
        columns = st.columns([1] * len(FILTER_COLUMNS[report]) + [1.7])
        for index, column in enumerate(FILTER_COLUMNS[report]):
            with columns[index]:
                selections[column] = st.multiselect(
                    column, frame[column].dropna().unique().tolist(),
                    placeholder="全部", key=f"{report}_{column}",
                )
        with columns[-1]:
            date_range = st.date_input(
                f"{date_column}范围",
                value=(frame[date_column].min().date(), frame[date_column].max().date()),
                key=f"{report}_dates",
            )
    if len(date_range) != 2:
        st.info("请选择完整的开始和结束日期。")
        return
    filtered = filter_demo_report(frame, selections, date_column, date_range)
    st.caption(f"当前显示 {len(filtered)} / {len(frame)} 条记录 · 保留原表列顺序和空白项，左右滚动查看全部测点")
    if filtered.empty:
        st.info("没有符合筛选条件的记录，请调整筛选条件。")
        return
    st.html(build_report_table(filtered, report))
    if report == "lifetime":
        st.caption("衰减比例按原表显示为百分比；CIE-x、CIE-y 的测量值、AVG 与判定保留空白。")
