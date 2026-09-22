"""Administrator-only multi-product RS PDF export, independent of alert sections."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pymupdf
import streamlit as st

from app.charts.inline_domain.aoi_rs_charts import iter_aoi_rs_chart_groups
from app.charts.inline_domain.aoi_rs_pdf import AoiRsPdfReport
from app.components.indicator_cache import (
    build_indicator_product_cache_signature,
    get_indicator_product_revision,
)
from app.sections.inline_domain.shared.constants import INLINE_FACTORY_OPTIONS
from app.utils.step_labels import format_step_label
from src.inline_domain.application.aoi_rs.aoi_rs_service import (
    AoiRsReportService,
    AoiRsReportViewModel,
)
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.application.shared.decision_signature import (
    get_scope_decision_signature,
)
from src.inline_domain.composition import build_aoi_rs_repository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)
EXPORT_BATCH_SIZE = 24
MAX_REPORT_BYTES = 200 * 1024 * 1024
PRODUCT_SELECTION_KEY = "aoi_rs_export_pdf_products"
FACTORY_SELECTION_KEY = "aoi_rs_export_pdf_factories"
EXPORT_STATUS_KEY = "aoi_rs_export_status"


class AoiRsExportError(ValueError):
    """An export limitation with a safe, user-facing explanation."""


@dataclass(frozen=True)
class _PreparedPdf:
    data: bytes
    filename: str
    ready: Event


@dataclass
class _ExportStatus:
    """Per-session status shared by the download thread and the UI fragment."""

    _value: tuple[str, str] = ("idle", "")
    _lock: Lock = field(default_factory=Lock, repr=False)
    _pdf: _PreparedPdf | None = field(default=None, repr=False)
    _download_ready: Event = field(default_factory=Event, repr=False)

    def set(self, kind: str, message: str) -> None:
        with self._lock:
            self._value = (kind, message)

    def get(self) -> tuple[str, str]:
        with self._lock:
            return self._value

    def start(self) -> bool:
        with self._lock:
            if self._value[0] == "busy":
                return False
            self._value = ("busy", "正在生成 RS 报告，请等待下载完成。")
            # Keep the last completed PDF downloadable while a new job runs.
            self._download_ready = Event()
            return True

    def publish(self, data: bytes, filename: str) -> None:
        with self._lock:
            self._pdf = _PreparedPdf(data, filename, self._download_ready)

    def get_pdf(self) -> _PreparedPdf | None:
        with self._lock:
            return self._pdf

    def mark_download_ready(self, pdf: _PreparedPdf) -> None:
        # A late fragment for an older PDF must not signal a newer download job.
        pdf.ready.set()

    def wait_download_ready(self) -> bool:
        with self._lock:
            ready = self._download_ready
        return ready.wait(timeout=30)


@st.fragment(run_every=2)
def _render_export_status(status: _ExportStatus) -> None:
    pdf = status.get_pdf()
    if pdf is not None:
        # A native bytes download keeps a session reference to the generated PDF.
        # Deferred-only media can otherwise be collected during fragment reruns.
        st.download_button(
            "再次下载报告（PDF）",
            data=pdf.data,
            file_name=pdf.filename,
            mime="application/pdf",
            key="aoi_rs_pdf_ready_download",
            on_click="ignore",
        )
        status.mark_download_ready(pdf)
    kind, message = status.get()
    if kind == "error":
        st.error(message)
    elif kind in {"busy", "success"}:
        st.caption(message)


def _filter_factories(frame: pd.DataFrame, factories: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty or "factory" not in frame.columns:
        return frame
    return frame.loc[frame["factory"].astype(str).isin(factories)].copy()


def build_aoi_rs_pdf(
    products: tuple[str, ...],
    *,
    factories: tuple[str, ...] = tuple(INLINE_FACTORY_OPTIONS),
    load_report: Callable[[str], AoiRsReportViewModel],
    start_date: date,
    end_date: date,
    step_desc_map: dict[str, str] | None = None,
) -> bytes:
    """Embed main-report charts into one PDF and clean all intermediate resources.

    Load each product once using the established application contract, then filter
    every chart input to the requested factories. Temporary PNGs never escape the
    unique export directory, which is removed on both success and failure.
    """
    if any(factory not in INLINE_FACTORY_OPTIONS for factory in factories):
        raise AoiRsExportError("所选厂别无效，请重新选择。")
    factories = factories or tuple(INLINE_FACTORY_OPTIONS)
    temp_root = ConfigLoader.get_project_root() / "output" / "tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    with (
        TemporaryDirectory(prefix="aoi_rs_export_", dir=temp_root) as temp_dir,
        pymupdf.open() as document,
    ):
        writer = AoiRsPdfReport(document, start_date, end_date)
        figures: list[go.Figure] = []
        rows: list[tuple[str, str, str]] = []
        empty_products: list[str] = []
        image_bytes = 0
        indicator_count = 0

        def flush() -> None:
            nonlocal image_bytes
            if not figures:
                return
            paths = [Path(temp_dir) / f"{index}.png" for index in range(len(figures))]
            pio.write_images(
                fig=figures,
                file=paths,
                format="png",
                width=600,
                height=460,
                scale=2,
            )
            image_bytes += sum(path.stat().st_size for path in paths)
            if image_bytes > MAX_REPORT_BYTES:
                raise AoiRsExportError("报告过大，请减少导出产品或厂别后重试。")
            for index, (product, factory, title) in enumerate(rows):
                writer.append_indicator(
                    product=product,
                    factory=factory,
                    title=title,
                    images=paths[index * 3 : index * 3 + 3],
                )
            # Images are embedded before removing the owned temporary files.
            for path in paths:
                path.unlink()
            figures.clear()
            rows.clear()

        for product in dict.fromkeys(products):
            report = load_report(product)
            frames = {
                name: _filter_factories(getattr(report, name), factories)
                for name in (
                    "rs_details_df",
                    "pass_through_df",
                    "spec_df",
                    "indicators_df",
                    "lot_points_df",
                    "sheet_points_df",
                )
            }
            count = 0
            for group in iter_aoi_rs_chart_groups(**frames, end_date=end_date):
                for slot, source in group.figures:
                    figure = go.Figure(source)
                    figure.update_layout(
                        template="plotly_white",
                        title={"font": {"size": 17}},
                        font={
                            "family": "Microsoft YaHei, Noto Sans CJK SC, Arial",
                            "size": 15,
                        },
                        paper_bgcolor="white",
                        plot_bgcolor="white",
                        margin={"l": 65, "r": 60, "t": 65, "b": 125},
                    )
                    if slot == "trend":
                        figure.update_layout(legend={"y": -0.42})
                        figure.update_xaxes(title_standoff=8)
                    figures.append(figure)
                rows.append(
                    (
                        product,
                        group.factory,
                        f"{group.code_name} | 站点 {format_step_label(group.step_id, step_desc_map)}",
                    )
                )
                count += 1
                if len(figures) >= EXPORT_BATCH_SIZE:
                    flush()
            indicator_count += count
            if not count:
                empty_products.append(product)
        flush()
        if not indicator_count:
            raise AoiRsExportError("所选产品和厂别暂无可导出的 RS 数据。")
        writer.append_empty_products(empty_products, factories)
        result = writer.to_bytes()
        if len(result) > MAX_REPORT_BYTES:
            raise AoiRsExportError("报告过大，请减少导出产品或厂别后重试。")
    return result


def render_aoi_rs_export(
    *,
    current_product: str,
    start_date: date,
    end_date: date,
    db_manager: DatabaseManager,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Render only for the repository's explicit administrator display mode."""
    if st.query_params.get("admin") != "true":
        return
    enabled = ConfigLoader.get_enabled_products()
    if not enabled:
        return
    if PRODUCT_SELECTION_KEY in st.session_state:
        previous = st.session_state[PRODUCT_SELECTION_KEY]
        retained = [product for product in previous if product in enabled]
        if retained != previous:
            st.session_state[PRODUCT_SELECTION_KEY] = retained
    else:
        st.session_state[PRODUCT_SELECTION_KEY] = list(enabled)
    if FACTORY_SELECTION_KEY not in st.session_state:
        st.session_state[FACTORY_SELECTION_KEY] = list(INLINE_FACTORY_OPTIONS)
    else:
        previous_factories = st.session_state[FACTORY_SELECTION_KEY]
        retained_factories = [
            item for item in previous_factories if item in INLINE_FACTORY_OPTIONS
        ]
        if retained_factories != previous_factories:
            st.session_state[FACTORY_SELECTION_KEY] = retained_factories

    with st.expander("RS 报告导出", expanded=False):
        selected = st.multiselect(
            "导出产品",
            options=enabled,
            key=PRODUCT_SELECTION_KEY,
            help="留空时导出全部已启用产品。",
        )
        products = tuple(
            product for product in enabled if not selected or product in selected
        )
        selected_factories = st.multiselect(
            "导出厂别",
            options=INLINE_FACTORY_OPTIONS,
            key=FACTORY_SELECTION_KEY,
            help="留空时导出全部厂别。",
        )
        factories = tuple(
            factory
            for factory in INLINE_FACTORY_OPTIONS
            if not selected_factories or factory in selected_factories
        )
        st.caption(
            f"待导出 {len(products)} 款产品，厂别：{'、'.join(factories)}。"
            "导出所选范围的主报表，不含预警区，不受下方查询筛选影响。"
            "PDF 内每个指标一行三图（月周天、By Lot、By Sheet），可独立打开；"
            "生成后自动清理临时图片，仅下载报告。"
        )

        # Reloads can retain the earlier ZIP status class in an existing session.
        if EXPORT_STATUS_KEY not in st.session_state or not hasattr(
            st.session_state[EXPORT_STATUS_KEY], "publish"
        ):
            st.session_state[EXPORT_STATUS_KEY] = _ExportStatus()
        status = st.session_state[EXPORT_STATUS_KEY]
        filename = f"AOI_RS_{start_date}_{end_date}.pdf"

        def download() -> bytes:
            def load_report(product: str) -> AoiRsReportViewModel:
                if product not in ConfigLoader.get_enabled_products():
                    raise AoiRsExportError("导出产品范围已变化，请刷新页面后重试。")
                query = AoiRsQueryConfig(
                    prod_code=product,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                )
                return AoiRsReportService.get_aoi_rs_report_data(
                    _data_port=build_aoi_rs_repository(db_manager, product),
                    query_config_json=query.model_dump_json(),
                    snapshot_signature=build_indicator_product_cache_signature(
                        "aoi_rs_report_v1",
                        product,
                        ("aoi_rs_sheet_oos",),
                    ),
                    product_revision=get_indicator_product_revision(
                        "aoi_rs_sheet_oos", product
                    ),
                    decision_signature=get_scope_decision_signature("aoi_rs", product),
                )

            if not status.start():
                raise RuntimeError("已有 RS 报告正在生成，请等待完成。")
            try:
                result = build_aoi_rs_pdf(
                    products,
                    factories=factories,
                    load_report=load_report,
                    start_date=start_date,
                    end_date=end_date,
                    step_desc_map=step_desc_map,
                )
                status.publish(result, filename)
                if not status.wait_download_ready():
                    raise AoiRsExportError(
                        "报告已生成，请点击“再次下载报告（PDF）”保存。"
                    )
            except Exception as exc:
                logger.exception("AOI RS image export failed for %s", products)
                message = (
                    str(exc)
                    if isinstance(exc, AoiRsExportError)
                    else "RS 报告导出失败，请稍后重试。"
                )
                status.set("error", message)
                raise RuntimeError(message) from None
            status.set("success", "RS 报告已生成，临时图片已清理。")
            return result

        st.download_button(
            "下载 RS 报告（PDF）",
            data=download,
            file_name=filename,
            mime="application/pdf",
            key="aoi_rs_pdf_download",
            on_click="ignore",
            icon=":material/download:",
        )
        _render_export_status(status)
