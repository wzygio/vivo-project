"""Query-gated AOI_RS density tables and product-sheet Excel download."""

from __future__ import annotations

from io import BytesIO
import logging
import re

import pandas as pd
import streamlit as st

from app.components.product_labels import get_product_label_formatter
from src.inline_domain.application.monitor.aoi_rs_monitor_service import AoiRsMonitorQuery

logger = logging.getLogger(__name__)
PREFIX = "aoi_rs_monitor"


def build_aoi_rs_monitor_export(tables: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter", engine_kwargs={"options": {
        "strings_to_formulas": False, "strings_to_urls": False,
    }}) as writer:
        for product, frame in tables.items():
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,31}", product):
                raise ValueError("Invalid product sheet name")
            frame.to_excel(writer, sheet_name=product, index=False)
            sheet = writer.sheets[product]
            sheet.freeze_panes(1, 5)
            sheet.autofilter(0, 0, len(frame), max(len(frame.columns) - 1, 0))
            sheet.set_column(0, 3, 14)
            sheet.set_column(4, 4, 24)
            sheet.set_column(5, len(frame.columns) - 1, 13, writer.book.add_format({"num_format": "0.0000"}))
    return buffer.getvalue()


def _multiselect(label: str, options: list, suffix: str, **kwargs) -> list:
    key = f"{PREFIX}_{suffix}"
    previous = st.session_state.get(key)
    known_key = f"{key}_options"
    old_options = st.session_state.get(known_key, [])
    # Retain explicit subsets; all-selected stays all as available scope changes.
    if previous is not None:
        st.session_state[key] = list(options) if set(previous) == set(old_options) else [item for item in previous if item in options]
    st.session_state[known_key] = list(options)
    defaults = {} if previous is not None else {"default": options}
    return st.multiselect(label, options, key=key, **defaults, **kwargs)


def render_aoi_rs_monitor_section(service, available_products) -> None:
    products_available = list(available_products)
    today = pd.Timestamp.today().normalize()
    if not products_available:
        st.info("暂无可用产品。")
        return
    with st.container():
        columns = st.columns([1, 3], vertical_alignment="bottom")
        with columns[0]:
            factory = st.selectbox("厂别", ["ARRAY", "OLED", "TP"], key=f"{PREFIX}_factory")
        with columns[1]:
            products = _multiselect("产品型号", products_available, "products", format_func=get_product_label_formatter())
        products = products or products_available
        try:
            catalog = service.read_catalog(products, factory)
        except Exception:
            logger.exception("AOI_RS monitor catalog read failed")
            st.error("AOI_RS 历史结果读取失败，请稍后重试。")
            return
        columns = st.columns(2)
        step_options = sorted(catalog["step_id"].unique().tolist())
        code_options = sorted(catalog["rs_code"].unique().tolist())
        with columns[0]:
            steps = _multiselect("站点", step_options, "steps", placeholder="全部站点（首次查询后可筛选）")
        with columns[1]:
            codes = _multiselect("RSCode", code_options, "codes", placeholder="全部 RSCode（首次查询后可筛选）")
        columns = st.columns([1.3, 1.7, 3, 1])
        with columns[0]:
            halves = _multiselect("半年", list(range(1, (today.month - 1) // 6 + 2)), "halfyears", format_func=lambda value: f"{today.year}-H{value}")
        with columns[1]:
            quarters = _multiselect("季度", list(range(1, (today.month - 1) // 3 + 2)), "quarters", format_func=lambda value: f"Q{value}")
        with columns[2]:
            months = _multiselect("月份", list(range(1, today.month + 1)), "months", format_func=lambda value: f"M{value}")
        with columns[3]:
            weeks = st.number_input("周数", min_value=1, max_value=53, value=5, step=1, key=f"{PREFIX}_weeks")
        clicked = st.button("查询", type="primary", key=f"{PREFIX}_query_submit")
        force = False
        if st.query_params.get("admin") == "true":
            force = st.button("刷新数据", key=f"{PREFIX}_refresh_data")
        query = AoiRsMonitorQuery(
            products=tuple(products), factory=factory, halfyears=tuple(halves), quarters=tuple(quarters),
            months=tuple(months), week_count=int(weeks),
            steps=tuple(steps) if set(steps) != set(step_options) else (),
            codes=tuple(codes) if set(codes) != set(code_options) else (),
        )
        signature = (query, today.date().isoformat())
        if clicked or force:
            try:
                with st.spinner("正在生成 AOI_RS 周期密度表…"):
                    payload = service.build_dashboard(query, refresh=force)
                    payload["excel"] = build_aoi_rs_monitor_export(payload["tables"])
                st.session_state[f"{PREFIX}_payload"] = payload
                st.session_state[f"{PREFIX}_query_signature"] = (*signature, service.presentation_signature(products))
                # Rebuild station/Code options after the first successful publication.
                if not step_options and not code_options and any(not frame.empty for frame in payload["tables"].values()):
                    st.rerun()
            except Exception:
                logger.exception("AOI_RS period dashboard query failed")
                st.session_state.pop(f"{PREFIX}_query_signature", None)
                st.error("AOI_RS 数据读取或计算失败，请稍后重试。")
                return
        previous = st.session_state.get(f"{PREFIX}_query_signature")
        if previous is None:
            return
        try:
            signature = (*signature, service.presentation_signature(products))
        except Exception:
            logger.exception("AOI_RS monitor result signature failed")
            st.session_state.pop(f"{PREFIX}_query_signature", None)
            st.error("AOI_RS 查询结果暂不可用，请稍后重新查询。")
            return
        if previous != signature:
            return
        payload = st.session_state[f"{PREFIX}_payload"]
        for product, table in payload["tables"].items():
            st.markdown(f"#### {product} RS density")
            if table.empty:
                st.info(f"{product} 暂无所选范围的 AOI_RS 数据。")
            else:
                st.dataframe(table.style.format(precision=4, na_rep=""),
                             hide_index=True, width="stretch")
        st.download_button("下载 AOI_RS 数据表", payload["excel"],
                           file_name=f"AOI_RS_{factory}_{today:%Y%m%d}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"{PREFIX}_download", on_click="ignore")
