"""Generate and retain last week's multi-product AOI TT PowerPoint report."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

import streamlit as st

from app.charts.inline_domain.aoi_tt_weekly_ppt import build_weekly_tt_ppt
from app.components.indicator_cache import (
    build_indicator_product_cache_signature,
    get_indicator_product_revision,
)
from app.components.product_labels import get_product_label_formatter
from app.sections.inline_domain.shared.constants import INLINE_FACTORY_OPTIONS
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService, AoiTtReportViewModel
from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.aoi_tt.weekly_report import ARRAY_TT_LAYERS, build_weekly_tt_tables
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import PARTICLE_SIZE_OPTIONS
from src.inline_domain.application.shared.decision_signature import get_scope_decision_signature
from src.inline_domain.composition import build_aoi_tt_repository
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
from app.sections.inline_domain.shared.weekly_ppt_template import load_weekly_ppt_template as _load_template
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)
PRODUCT_SELECTION_KEY = "aoi_tt_weekly_ppt_products"
FACTORY_SELECTION_KEY = "aoi_tt_weekly_ppt_factories"
STEP_SELECTION_KEY = "aoi_tt_weekly_ppt_steps"
PARTICLE_SELECTION_KEY = "aoi_tt_weekly_ppt_particle_sizes"
STATUS_KEY = "aoi_tt_weekly_ppt_status"
_ARRAY_STEPS = ARRAY_TT_LAYERS
_MAX_BYTES = 30 * 1024 * 1024


@dataclass
class _WeeklyExportStatus:
    busy: bool = False
    message: str = ""
    failed: bool = False
    data: bytes | None = None
    filename: str = ""
    scope: tuple = ()


def load_weekly_ppt_template() -> bytes:
    return _load_template("aoi_tt_weekly_ppt_template")


def build_aoi_tt_weekly_ppt(
    products: tuple[str, ...], *, factories: tuple[str, ...], steps: tuple[str, ...],
    load_report: Callable[[str], AoiTtReportViewModel], reference_date: date, template_bytes: bytes,
    particle_sizes: tuple[str, ...] = ("Total",),
) -> bytes:
    tables = build_weekly_tt_tables(products, load_report=load_report, reference_date=reference_date,
                                    factories=factories, steps=steps, particle_sizes=particle_sizes)
    result = build_weekly_tt_ppt(tables, template_bytes=template_bytes)
    if len(result) > _MAX_BYTES:
        raise ValueError("Weekly TT report exceeds size limit")
    return result


def _selection(key: str, options: list[str], default: list[str]) -> None:
    old = st.session_state.get(key, default)
    st.session_state[key] = [value for value in old if value in options]


def render_aoi_tt_weekly_export(
    *, current_product: str, start_date: date, end_date: date, db_manager,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    enabled = ConfigLoader.get_enabled_products()
    if not enabled:
        return
    _selection(PRODUCT_SELECTION_KEY, enabled, enabled)
    _selection(FACTORY_SELECTION_KEY, list(INLINE_FACTORY_OPTIONS), ["ARRAY"])
    _selection(STEP_SELECTION_KEY, list(_ARRAY_STEPS), [])
    _selection(PARTICLE_SELECTION_KEY, list(PARTICLE_SIZE_OPTIONS), ["Total"])
    week_start, week_end = previous_iso_week_range(end_date)
    iso = week_start.isocalendar()
    with st.expander("TT 周报导出（PPT）", expanded=False):
        selected = st.multiselect("周报产品", enabled, key=PRODUCT_SELECTION_KEY,
                                  format_func=get_product_label_formatter(), help="留空时导出全部已启用产品。")
        factories_selected = st.multiselect("周报厂别", list(INLINE_FACTORY_OPTIONS),
                                            key=FACTORY_SELECTION_KEY, help="留空时导出全部厂别。")
        products = tuple(p for p in enabled if not selected or p in selected)
        factories = tuple(f for f in INLINE_FACTORY_OPTIONS if not factories_selected or f in factories_selected)
        steps = ()
        if "ARRAY" in factories:
            selected_steps = st.multiselect(
                "Array 站点（层别）", list(_ARRAY_STEPS), key=STEP_SELECTION_KEY,
                format_func=lambda step: f"{step} · {_ARRAY_STEPS[step]}",
                help="留空时导出全部层别；此项只筛选 Array。",
            )
            steps = tuple(s for s in _ARRAY_STEPS if s in selected_steps)
        particle_sizes = ("Total",)
        if any(factory != "OLED" for factory in factories):
            selected_sizes = st.multiselect(
                "周报 Particle Size", list(PARTICLE_SIZE_OPTIONS), key=PARTICLE_SELECTION_KEY,
                help="默认 Total；留空导出全部粒径。OLED 始终只导出 Total。",
            )
            particle_sizes = tuple(size for size in PARTICLE_SIZE_OPTIONS if not selected_sizes or size in selected_sizes)
        st.caption(
            f"上一周 {iso.year}-W{iso.week:02d}（{week_start:%Y-%m-%d}～{(week_end - timedelta(days=1)):%Y-%m-%d}）。"
            "各产品按 TT 项目和粒径填入周度平均每片 TT 个数，与当前折线图一致；无过货填写“无过货”。"
            "每页汇总最多 8 款产品；Array 按层别分类，项目较多时自动分页。"
        )
        status = st.session_state.setdefault(STATUS_KEY, _WeeklyExportStatus())
        scope = (products, factories, steps, week_start.date(), particle_sizes)

        def generate() -> None:
            if status.busy:
                return
            status.busy, status.failed, status.message = True, False, "正在生成 TT 周报。"
            try:
                # Changes to global product availability also invalidate old results.
                if any(p not in ConfigLoader.get_enabled_products() for p in products):
                    raise ValueError("Enabled product scope changed")

                def load_report(product: str) -> AoiTtReportViewModel:
                    query = AoiTtQueryConfig(prod_code=product, start_date=start_date.isoformat(),
                                             end_date=end_date.isoformat())
                    return AoiTtReportService.get_aoi_tt_report_data(
                        _data_port=build_aoi_tt_repository(db_manager, product),
                        query_config_json=query.model_dump_json(),
                        snapshot_signature=build_indicator_product_cache_signature(
                            "aoi_tt_report_v4_integer_particle_quantities", product, ("aoi_tt_sheet_oos",)),
                        product_revision=get_indicator_product_revision("aoi_tt_sheet_oos", product),
                        decision_signature=get_scope_decision_signature("aoi_tt", product),
                    )

                with st.spinner("正在生成 TT 周报，请稍候。"):
                    result = build_aoi_tt_weekly_ppt(
                        products, factories=factories, steps=steps, load_report=load_report,
                        reference_date=end_date, template_bytes=load_weekly_ppt_template(),
                        particle_sizes=particle_sizes,
                    )
                status.data, status.scope = result, scope
                status.filename = f"AOI_TT_周报_{iso.year}-W{iso.week:02d}.pptx"
                status.message = "TT 周报已生成，请点击下载按钮保存。"
            except Exception:
                logger.exception("AOI TT weekly PPT generation failed for %s", products)
                status.failed = True
                status.message = "TT 周报生成失败，请稍后重试。"
            finally:
                if status.message == "正在生成 TT 周报。":
                    status.failed = True
                    status.message = "TT 周报生成已中断，请重新生成。"
                status.busy = False

        st.button("生成上周报告（PPT）", key="aoi_tt_weekly_ppt_generate", on_click=generate,
                  disabled=status.busy, icon=":material/description:")
        if status.data is not None:
            # Restrict retrievable content too, not just product selector options.
            if all(p in enabled for p in status.scope[0]):
                if scope != status.scope:
                    st.caption("当前筛选已变化，下方仍为上次生成的周报。请重新生成以更新范围。")
                st.download_button("下载上周报告（PPT）", data=status.data, file_name=status.filename,
                                   mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                                   key="aoi_tt_weekly_ppt_download", on_click="ignore", icon=":material/download:")
            else:
                status.data = None
        if status.message:
            if status.failed:
                st.error(status.message)
            else:
                st.caption(status.message)
