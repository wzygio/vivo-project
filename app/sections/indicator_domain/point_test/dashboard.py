"""Native tables for product-scoped point-test details and stage summaries."""

import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from app.components.product_labels import get_product_label_formatter
from src.indicator_domain.application.point_test.service import PointTestReportService
from src.indicator_domain.composition import build_point_test_service
from src.indicator_domain.core.point_test.report import METRICS, STAGES
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600, max_entries=8, show_spinner=False)
def fetch_report_payload(path: str, version: tuple[int, int]) -> dict[str, dict[str, pd.DataFrame]]:
    """Full configured path + (mtime_ns, size) identify each source generation."""
    return build_point_test_service(Path(path)).read_report()


def _render_tables(report: dict[str, dict[str, pd.DataFrame]]) -> None:
    report = {product: report[product] for product in ConfigLoader.get_enabled_products()
              if product in report}
    if st.session_state.get("point_test_product") not in report:
        st.session_state.pop("point_test_product", None)
    if not report:
        st.info("暂无密集点测试数据。")
        return
    product = st.selectbox("产品", list(report), key="point_test_product",
                           format_func=get_product_label_formatter())
    detail, summary = report[product]["detail"], report[product]["summary"]
    if detail.empty and summary.empty:
        st.info("该产品暂无密集点测试数据。")
        return
    st.caption(f"{product} · {len(detail):,} 条明细 · {len(summary):,} 条膜位汇总")
    summary_tab, detail_tab = st.tabs(["膜位汇总", "明细数据"])
    with summary_tab:
        st.caption("按 Sheet ID 和膜位列示各阶段统计值。")
        for stage, tab in zip(STAGES, st.tabs(list(STAGES))):
            with tab:
                selected = summary.loc[summary["阶段"] == stage].reset_index(drop=True)
                if selected.empty:
                    st.info("该阶段暂无膜位汇总数据。")
                else:
                    st.dataframe(
                        selected, hide_index=True, width="stretch", height=min(500, 36 * (len(selected) + 1) + 4),
                        column_config={
                            "SHEET_ID": st.column_config.TextColumn("Sheet ID", width="medium", pinned=True),
                            "膜位": st.column_config.TextColumn(width="small", pinned=True),
                            "产品": st.column_config.TextColumn(width="small"),
                            "阶段": st.column_config.TextColumn(width="small"),
                            **{metric: st.column_config.NumberColumn(format="%.6f", width="small") for metric in METRICS},
                        },
                        key=f"point_test_summary_{product}_{stage}",
                    )
    with detail_tab:
        if detail.empty:
            st.info("该产品暂无明细数据。")
        else:
            st.dataframe(
                detail, hide_index=True, width="stretch", height=min(600, 36 * (len(detail) + 1) + 4),
                column_config={"SHEET_ID": st.column_config.TextColumn("Sheet ID", width="medium", pinned=True)},
                key=f"point_test_detail_{product}",
            )


def render_point_test_dashboard(service: PointTestReportService | None = None) -> None:
    """Read before rendering so a failed reload never exposes a previous result."""
    try:
        with st.spinner("正在读取密集点测试数据…"):
            if service is not None:
                report = service.read_report()
            else:
                path = ConfigLoader.get_domain_resource_path("indicator_domain", "point_test_workbook")
                stat = path.stat()
                report = fetch_report_payload(str(path.resolve()), (stat.st_mtime_ns, stat.st_size))
        _render_tables(report)
    except Exception:
        logger.exception("Point-test report could not be loaded")
        st.error("暂时无法读取密集点测试数据，请稍后刷新重试。")
        return
