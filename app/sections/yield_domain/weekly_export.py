"""Compose the Yield PowerPoint export for Streamlit and the local command."""
from dataclasses import dataclass
import logging
from pathlib import Path

import streamlit as st

from app.charts.yield_domain.weekly_ppt import build_weekly_yield_ppt
from app.components.product_labels import get_product_label_formatter
from app.sections.yield_domain.group_trend_data import build_yield_source_signature, load_product_group_trends
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.ppt_template import read_ppt_template
from src.yield_domain.application.weekly_report import ReportWeek, load_weekly_conclusions
from src.yield_domain.composition import build_yield_alert_results

_LOGGER = logging.getLogger(__name__)
_REPORT_KEY = "yield_weekly_report"
_PRODUCTS_KEY = "yield_weekly_products"
_PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
_TEMPLATE_KEY = "weekly_ppt_template"


@dataclass(frozen=True)
class WeeklyYieldExport:
    week: ReportWeek
    products: tuple[str, ...]
    data: bytes
    revision: tuple

    @property
    def filename(self) -> str:
        return f"良率周报_{self.week.label}.pptx"


@st.cache_data(show_spinner=False, max_entries=2, ttl=ConfigLoader.get_cache_ttl_seconds())
def _load_template(path: str, modified_ns: int, size: int, temp_root: Path) -> bytes:
    return read_ppt_template(Path(path), temp_root=temp_root)


def _input_revision(products: tuple[str, ...]) -> tuple:
    file_versions = []
    for key in (_TEMPLATE_KEY, "trend_alert_results", "lot_oos_results", "yield_modifier_table"):
        path = ConfigLoader.get_domain_resource_path("yield_domain", key)
        stat = path.stat()
        file_versions.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(file_versions) + tuple(build_yield_source_signature(product) for product in products)


def _selected_products(enabled: tuple[str, ...], selected: tuple[str, ...]) -> tuple[str, ...]:
    if set(selected).difference(enabled):
        raise ValueError("Selected Yield product is no longer enabled")
    return tuple(product for product in enabled if not selected or product in selected)


def generate_weekly_yield_export(*, selected_products: tuple[str, ...] = (), db_manager=None) -> WeeklyYieldExport:
    week = ReportWeek.previous()
    enabled = tuple(ConfigLoader.get_enabled_products(strict=True))
    products = _selected_products(enabled, selected_products)
    if not products:
        raise ValueError("No enabled products for the Yield weekly report")
    revision = _input_revision(products)
    temp_root = ConfigLoader.get_project_root() / "output" / "tmp"
    template_bytes = _load_template(*revision[0], temp_root)
    conclusions = load_weekly_conclusions(build_yield_alert_results(), products, week)
    charts = {}
    for product in products:
        payload = load_product_group_trends(product, db_manager=db_manager)
        if payload["data_health"]["status"] not in {"fresh", "unknown"}:
            raise ValueError(f"Group trend source unavailable for {product}")
        charts[product] = payload["trends"]
    data = build_weekly_yield_ppt(
        conclusions, charts, week_label=week.label, template_bytes=template_bytes, temp_root=temp_root,
    )
    if (tuple(ConfigLoader.get_enabled_products(strict=True)) != enabled
            or _input_revision(products) != revision or ReportWeek.previous() != week):
        raise ValueError("Yield report inputs changed during generation")
    return WeeklyYieldExport(week, products, data, revision)


@st.fragment
def render_yield_weekly_export(*, db_manager=None) -> None:
    week = ReportWeek.previous()
    with st.expander("良率周报导出（PPT）", expanded=False):
        try:
            enabled = tuple(ConfigLoader.get_enabled_products(strict=True))
            format_label = get_product_label_formatter()
        except ValueError:
            _LOGGER.exception("Yield weekly PPT product scope is invalid")
            st.session_state.pop(_REPORT_KEY, None)
            st.error("良率周报产品范围暂不可用。")
            return
        if not enabled:
            st.session_state.pop(_REPORT_KEY, None)
            st.info("暂无可导出的产品。")
            return
        prior = st.session_state.get(_PRODUCTS_KEY, [])
        if any(product not in enabled for product in prior):
            st.session_state[_PRODUCTS_KEY] = [product for product in prior if product in enabled]
        selected = st.multiselect(
            "周报产品型号", enabled, key=_PRODUCTS_KEY, format_func=format_label,
            placeholder="全部启用产品", help="可选择多个型号，留空时导出全部启用产品。",
        )
        products = _selected_products(enabled, tuple(selected))
        st.caption(f"上一周 {week.label}（{week.start}～{week.end}），本次导出 {len(products)} 款产品。")
        if st.button("生成上周报告（PPT）", key="yield_weekly_generate", icon=":material/description:"):
            st.session_state.pop(_REPORT_KEY, None)
            try:
                with st.spinner("正在生成良率周报，请稍候。"):
                    st.session_state[_REPORT_KEY] = generate_weekly_yield_export(
                        selected_products=products, db_manager=db_manager,
                    )
            except Exception:
                _LOGGER.exception("Yield weekly PPT generation failed for %s", week.label)
                st.error("良率周报生成失败，请稍后重试。")
        report = st.session_state.get(_REPORT_KEY)
        if report is None:
            return
        if report.products != products:
            del st.session_state[_REPORT_KEY]
            st.info("周报产品范围已变更，请重新生成。")
            return
        try:
            report_is_current = report.week == week and (
                report.revision == _input_revision(products)
            )
        except (OSError, ValueError):
            _LOGGER.exception("Yield weekly PPT input versions could not be checked")
            report_is_current = False
        if not report_is_current:
            del st.session_state[_REPORT_KEY]
            st.info("周报数据已更新，请重新生成。")
            return
        st.download_button(
            "下载上周报告（PPT）", data=report.data, file_name=report.filename, mime=_PPTX_MIME,
            key="yield_weekly_download", on_click="ignore", icon=":material/download:",
        )
