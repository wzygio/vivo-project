"""Administrator-only SPC PDF generation using the main report's chart inputs."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pymupdf
import streamlit as st

from app.charts.inline_domain.indicator_pdf import IndicatorPdfReport
from app.charts.inline_domain.spc_charts import iter_spc_chart_groups
from app.components.indicator_cache import build_indicator_product_cache_signature
from app.sections.inline_domain.shared.constants import INLINE_FACTORY_OPTIONS
from src.inline_domain.application.shared.decision_signature import (
    get_scope_decision_signature,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.spc.spc_service import (
    SpcReportService,
    SpcReportViewModel,
)
from src.inline_domain.composition import build_spc_repository
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)
PRODUCT_SELECTION_KEY = "spc_export_pdf_products"
FACTORY_SELECTION_KEY = "spc_export_pdf_factories"
EXPORT_STATUS_KEY = "spc_pdf_generation_status"
EXPORT_BATCH_SIZE = 24
MAX_REPORT_BYTES = 200 * 1024 * 1024


class SpcExportError(ValueError):
    """An export limitation with a safe user-facing explanation."""


def _filter_factories(frame: pd.DataFrame, factories: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.loc[frame["factory"].astype(str).isin(factories)].copy()


def build_spc_pdf(
    products: tuple[str, ...],
    *,
    factories: tuple[str, ...] = tuple(INLINE_FACTORY_OPTIONS),
    load_report: Callable[[str], SpcReportViewModel],
    start_date: date,
    end_date: date,
    period_box_source: str = "point_value",
    line_param_name_contains: Sequence[str] = (),
    step_desc_map: dict[str, str] | None = None,
) -> bytes:
    """Embed three charts per indicator, cleaning temporary images on every exit."""
    if any(factory not in INLINE_FACTORY_OPTIONS for factory in factories):
        raise SpcExportError("所选厂别无效，请重新选择。")
    factories = factories or tuple(INLINE_FACTORY_OPTIONS)
    temp_root = ConfigLoader.get_project_root() / "output" / "tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    detail_start, _ = previous_iso_week_range(end_date)
    chart_titles = (
        "月周天分布",
        f"{detail_start:%m-%d} 起 | By 设备/腔室",
        f"{detail_start:%m-%d} 起 | By 过货时间",
    )
    with (
        TemporaryDirectory(prefix="spc_export_", dir=temp_root) as temp_dir,
        pymupdf.open() as document,
    ):
        writer = IndicatorPdfReport(document, start_date, end_date, report_name="SPC")
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
            pio.write_images(fig=figures, file=paths, format="png", width=600, height=460, scale=2)
            image_bytes += sum(path.stat().st_size for path in paths)
            if image_bytes > MAX_REPORT_BYTES:
                raise SpcExportError("报告过大，请减少导出产品或厂别后重试。")
            for index, (product, factory, title) in enumerate(rows):
                writer.append_indicator(
                    product=product, factory=factory, title=title,
                    images=paths[index * 3:index * 3 + 3],
                )
            for path in paths:
                path.unlink()
            figures.clear()
            rows.clear()

        for product in dict.fromkeys(products):
            report = load_report(product)
            frames = {
                name: _filter_factories(getattr(report, name), factories)
                for name in (
                    "period_capability_df", "sheet_features_df",
                    "raw_measurements_df", "indicators_df",
                )
            }
            count = 0
            for group in iter_spc_chart_groups(
                period_capability_df=frames["period_capability_df"],
                sheet_features_df=frames["sheet_features_df"],
                raw_measurements_df=frames["raw_measurements_df"],
                period_box_source=period_box_source,
                reference_date=end_date,
                line_param_name_contains=line_param_name_contains,
                step_desc_map=step_desc_map,
            ):
                if len(group.figures) != 3:
                    raise ValueError("Each SPC indicator must contain three charts")
                for title, source in zip(chart_titles, group.figures):
                    figure = go.Figure(source)
                    figure.update_layout(
                        template="plotly_white",
                        title={"text": title, "font": {"size": 17}},
                        font={"family": "Microsoft YaHei, Noto Sans CJK SC, Arial", "size": 13},
                        paper_bgcolor="white", plot_bgcolor="white",
                        margin={"l": 65, "r": 30, "t": 65, "b": 120},
                        legend={"orientation": "h", "yanchor": "top", "y": -0.4,
                                "xanchor": "left", "x": 0, "font": {"size": 10}},
                    )
                    figure.update_xaxes(title_standoff=8)
                    figures.append(figure)
                rows.append((product, group.factory, group.title))
                count += 1
                if len(figures) >= EXPORT_BATCH_SIZE:
                    flush()
            indicator_count += count
            if not count:
                empty_products.append(product)
        flush()
        if not indicator_count:
            raise SpcExportError("所选产品和厂别暂无可导出的 SPC 数据。")
        writer.append_empty_products(empty_products, factories)
        result = writer.to_bytes()
        if len(result) > MAX_REPORT_BYTES:
            raise SpcExportError("报告过大，请减少导出产品或厂别后重试。")
    return result


def render_spc_export(
    *,
    start_date: date,
    end_date: date,
    db_manager: DatabaseManager,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Generate on request; offer a native download only after a PDF is ready."""
    if st.query_params.get("admin") != "true":
        return
    enabled = ConfigLoader.get_enabled_products()
    if not enabled:
        return
    for key, options, default in (
        (PRODUCT_SELECTION_KEY, enabled, list(enabled)),
        (FACTORY_SELECTION_KEY, INLINE_FACTORY_OPTIONS, ["ARRAY"]),
    ):
        if key not in st.session_state:
            st.session_state[key] = default
        else:
            previous = st.session_state[key]
            retained = [item for item in previous if item in options]
            if retained != previous:
                st.session_state[key] = retained
    if EXPORT_STATUS_KEY not in st.session_state:
        st.session_state[EXPORT_STATUS_KEY] = {"kind": "idle", "message": "", "data": None}
    status = st.session_state[EXPORT_STATUS_KEY]
    if status["data"] is not None and (
        not status.get("products") or not set(status["products"]).issubset(enabled)
    ):
        status.update(kind="error", message="产品范围已变化，请重新生成报告。", data=None)

    with st.expander("SPC 报告导出", expanded=False):
        selected = st.multiselect(
            "导出产品", options=enabled, key=PRODUCT_SELECTION_KEY,
            help="留空时导出全部已启用产品。",
        )
        selected_factories = st.multiselect(
            "导出厂别", options=INLINE_FACTORY_OPTIONS, key=FACTORY_SELECTION_KEY,
            help="留空时导出全部厂别。",
        )
        products = tuple(item for item in enabled if not selected or item in selected)
        factories = tuple(
            item for item in INLINE_FACTORY_OPTIONS
            if not selected_factories or item in selected_factories
        )
        st.caption(
            f"待导出 {len(products)} 款产品，厂别：{'、'.join(factories)}。"
            "导出所选范围的主报表图像，不含预警区，不受下方查询筛选影响。"
            "PDF 内每个指标一行三图（月周天分布、腔室分布、时间分布）。"
            "生成后点击下载按钮保存，临时图片会自动清理。"
        )

        def load_report(product: str) -> SpcReportViewModel:
            if product not in ConfigLoader.get_enabled_products():
                raise SpcExportError("导出产品范围已变化，请刷新页面后重试。")
            query = SpcQueryConfig(
                prod_code=product, start_date=start_date.isoformat(),
                end_date=end_date.isoformat(), data_type_filter="SPC",
            )
            signature = build_indicator_product_cache_signature(
                "spc_database_target_v4", product, ("spc_sheet_oos", "spc_cpk_trend"),
            )
            return SpcReportService.get_spc_report_data(
                _data_port=build_spc_repository(db_manager, product),
                query_config_json=query.model_dump_json(),
                snapshot_signature=signature, product_revision=signature,
                decision_signature=get_scope_decision_signature("spc", product),
                period_sigma_source=ConfigLoader.get_spc_period_sigma_source(),
            )

        def generate() -> None:
            if status["kind"] == "busy":
                return
            status.update(kind="busy", message="正在生成 SPC 报告，请稍候。")
            try:
                with st.spinner("正在生成 SPC 报告，请稍候。"):
                    data = build_spc_pdf(
                        products, factories=factories, load_report=load_report,
                        start_date=start_date, end_date=end_date,
                        period_box_source=ConfigLoader.get_spc_period_box_source(),
                        line_param_name_contains=ConfigLoader.get_spc_line_chart_param_name_contains(),
                        step_desc_map=step_desc_map,
                    )
                status.update(
                    kind="success", message="SPC 报告已生成，临时图片已清理。请点击下载按钮保存。",
                    data=data, products=products, filename=f"SPC_{start_date}_{end_date}.pdf",
                    scope=f"已生成报告：{'、'.join(products)}；厂别：{'、'.join(factories)}；"
                          f"期间：{start_date} ~ {end_date}。",
                )
            except Exception as exc:
                logger.exception("SPC report generation failed for %s", products)
                message = str(exc) if isinstance(exc, SpcExportError) else "SPC 报告生成失败，请稍后重试。"
                status.update(kind="error", message=message)
            finally:
                # Preserve cancellation while restoring the ability to retry.
                if status["kind"] == "busy":
                    status.update(kind="error", message="报告生成已中断，请重新生成。")

        st.button(
            "生成报告", key="spc_pdf_generate", on_click=generate,
            disabled=status["kind"] == "busy", icon=":material/description:",
        )
        if status["data"] is not None:
            st.caption(status["scope"])
            st.download_button(
                "下载报告（PDF）", data=status["data"], file_name=status["filename"],
                mime="application/pdf", key="spc_pdf_ready_download",
                on_click="ignore", icon=":material/download:",
            )
        if status["kind"] == "error":
            st.error(status["message"])
        elif status["message"]:
            st.caption(status["message"])
