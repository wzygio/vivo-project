"""AOI_TT 报表应用服务：缓存 payload → ViewModel（缓存边界遵循 ADR-0001）。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.application.aoi_tt.decoration_service import prepare_aoi_tt_decoration
from src.inline_domain.application.shared.ooc_decoration_service import persist_ooc_facts
from src.inline_domain.application.shared.throughput_persistence import (
    persist_throughput_facts,
)
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    AOI_TT_OOC_KEY_COLUMNS,
    build_aoi_tt_ooc_detail,
)
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import (
    build_generated_particle_size_details,
    build_particle_size_details,
)
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.inline_domain.application.aoi_tt.ports import AoiTtDataPort

logger = logging.getLogger(__name__)


def _covers_full_product(query_config: AoiTtQueryConfig) -> bool:
    """History replacement is safe only for an unfiltered product/date query."""
    return all(
        value is None
        for value in (query_config.factory, query_config.step_id, query_config.tt_name)
    )


class AoiTtReportBuildError(RuntimeError):
    """Raised when an AOI_TT report cannot be built safely."""


def _load_particle_size_counts(
    data_port: "AoiTtDataPort",
    query_config: AoiTtQueryConfig,
) -> pd.DataFrame | None:
    loader = getattr(data_port, "get_particle_size_counts", None)
    if loader is None:
        return None
    try:
        return loader(query_config)
    except Exception:
        logger.exception(
            "[AOI_TT] Particle Size data unavailable for %s; using Total only",
            query_config.prod_code,
        )
        return None


def _load_particle_size_ratios(data_port: "AoiTtDataPort") -> pd.DataFrame | None:
    loader = getattr(data_port, "get_particle_size_ratios", None)
    if loader is None:
        return None
    try:
        return loader()
    except Exception:
        logger.exception("[AOI_TT] Particle Size ratio specification unavailable; using Total only")
        return None


def _particle_size_runtime_config() -> tuple[bool, float, str]:
    generated = ConfigLoader.get_aoi_tt_particle_size_generation_enabled()
    jitter = ConfigLoader.get_aoi_tt_particle_size_ratio_jitter()
    path = ConfigLoader.get_aoi_tt_particle_size_ratio_spec_path()
    try:
        stat = path.stat()
        signature = f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        signature = f"{path.name}:missing"
    return generated, jitter, signature


@dataclass
class AoiTtReportViewModel:
    """AOI_TT 报表视图模型：明细、规格与指标元数据。"""

    tt_details_df: pd.DataFrame
    spec_df: pd.DataFrame
    indicators_df: pd.DataFrame


def _build_indicators(tt_details_df: pd.DataFrame) -> pd.DataFrame:
    """指标粒度 = 厂别 + 站点 + TT 参数名。"""
    if tt_details_df.empty:
        return pd.DataFrame(columns=["prod_code", "factory", "step_id", "tt_name"])

    indicators = (
        tt_details_df[["prod_code", "factory", "step_id", "tt_name"]]
        .drop_duplicates()
        .sort_values(["factory", "step_id", "tt_name"])
        .reset_index(drop=True)
    )
    return indicators


class AoiTtReportService:
    """AOI_TT 报表应用服务。"""

    @staticmethod
    def _empty_payload() -> dict[str, object]:
        return {
            "tt_details_df": pd.DataFrame(),
            "spec_df": pd.DataFrame(),
            "indicators_df": pd.DataFrame(),
        }

    @staticmethod
    def _view_model_from_payload(payload: dict[str, object]) -> AoiTtReportViewModel:
        def _df(key: str) -> pd.DataFrame:
            value = payload.get(key)
            return value if isinstance(value, pd.DataFrame) else pd.DataFrame()

        return AoiTtReportViewModel(
            tt_details_df=_df("tt_details_df"),
            spec_df=_df("spec_df"),
            indicators_df=_df("indicators_df"),
        )

    @staticmethod
    @st.cache_data(
        show_spinner=False,
        max_entries=16,
        ttl=ConfigLoader.get_cache_ttl_seconds(),
    )
    def fetch_aoi_tt_report_payload(
        _data_port: "AoiTtDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        product_revision: str = "",
        decision_signature: str = "",
        generate_particle_sizes: bool = True,
        particle_ratio_jitter: float = 0.1,
        particle_ratio_signature: str = "",
    ) -> dict[str, object]:
        """缓存仅含 DataFrame 的原生 payload；构建失败向上抛出。

        max_entries=16 覆盖已启用产品及短期 revision；TTL 由 config/global.yaml 的
        application.cache_ttl_hours 统一配置。product_revision /
        decision_signature 进入缓存 key 并透传到 core 刷新门控：页头刷新
        或用户编辑决策台账会换 key 立即重建，不受周期 TTL 遮挡。
        """
        try:
            query_config = AoiTtQueryConfig.model_validate_json(query_config_json)
        except Exception as exc:
            logger.error("[AOI_TT] query config parse failed: %s", exc, exc_info=True)
            raise AoiTtReportBuildError("AOI_TT query config is invalid.") from exc

        try:
            tt_details_df = _data_port.get_tt_details(query_config)
            persist_shared_history = bool(
                getattr(_data_port, "supports_shared_history_persistence", False)
            )
            if tt_details_df.empty:
                coverage_start, coverage_end = OosHistoryService.inclusive_date_window(
                    query_config.start_date, query_config.end_date
                )
                if persist_shared_history and _covers_full_product(query_config):
                    product_dir = resolve_product_resource_dir(
                        query_config.prod_code, scope="aoi_tt"
                    )
                    persist_ooc_facts(
                        scope="aoi_tt",
                        prod_code=query_config.prod_code,
                        detail_df=build_aoi_tt_ooc_detail(pd.DataFrame(), pd.DataFrame()),
                        key_columns=AOI_TT_OOC_KEY_COLUMNS,
                        product_dir=product_dir,
                        coverage_start=coverage_start,
                        coverage_end=coverage_end,
                        product_revision=product_revision,
                        decision_signature=decision_signature,
                    )
                    persist_throughput_facts(
                        scope="aoi_tt",
                        prod_code=query_config.prod_code,
                        details=pd.DataFrame(),
                        event_time_column="start_time",
                        item_id_column="sheet_id",
                        product_dir=product_dir,
                        coverage_start=coverage_start,
                        coverage_end=coverage_end,
                    )
                return AoiTtReportService._empty_payload()
            spec_df = _data_port.get_tt_spec_limits(query_config.prod_code)
            # 超规片修饰：工作簿三态 flag（Delete 删除 / False 释放 / True 默认截断），
            # 无工作簿时与引入前的自动截断行为一致；scope 门控决定是否真正落盘
            decoration_result = prepare_aoi_tt_decoration(
                tt_details_df,
                spec_df,
                product_dir=resolve_product_resource_dir(query_config.prod_code, scope="aoi_tt"),
                prod_code=query_config.prod_code,
                exempt_param_name_contains=(
                    ConfigLoader.get_auto_decoration_param_exemptions()
                ),
                scope="aoi_tt",
                product_revision=product_revision,
                decision_signature=decision_signature,
            )
            coverage_start, coverage_end = OosHistoryService.inclusive_date_window(
                query_config.start_date, query_config.end_date
            )
            covers_full_product = _covers_full_product(query_config)
            product_dir = resolve_product_resource_dir(
                query_config.prod_code, scope="aoi_tt"
            )
            if (
                persist_shared_history
                and not spec_df.empty
                and covers_full_product
            ):
                persist_ooc_facts(
                    scope="aoi_tt",
                    prod_code=query_config.prod_code,
                    detail_df=build_aoi_tt_ooc_detail(tt_details_df, spec_df),
                    key_columns=AOI_TT_OOC_KEY_COLUMNS,
                    product_dir=product_dir,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                    product_revision=product_revision,
                    decision_signature=decision_signature,
                )
            elif spec_df.empty:
                logger.warning(
                    "[AOI_TT] Skip OOC ledger update for %s: specifications are empty",
                    query_config.prod_code,
                )
            if persist_shared_history and covers_full_product:
                persist_throughput_facts(
                    scope="aoi_tt",
                    prod_code=query_config.prod_code,
                    details=tt_details_df,
                    event_time_column="start_time",
                    item_id_column="sheet_id",
                    product_dir=product_dir,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                )
            tt_details_df = decoration_result.tt_details_df
            if generate_particle_sizes:
                ratio_spec_df = _load_particle_size_ratios(_data_port)
                if ratio_spec_df is None:
                    tt_details_df = tt_details_df.assign(particle_size="Total")
                else:
                    tt_details_df = build_generated_particle_size_details(
                        tt_details_df,
                        ratio_spec_df,
                        ratio_jitter=particle_ratio_jitter,
                    )
            else:
                particle_counts_df = _load_particle_size_counts(_data_port, query_config)
                if particle_counts_df is None:
                    tt_details_df = tt_details_df.assign(particle_size="Total")
                else:
                    tt_details_df = build_particle_size_details(tt_details_df, particle_counts_df)
            indicators_df = _build_indicators(tt_details_df)
            return {
                "tt_details_df": tt_details_df,
                "spec_df": spec_df,
                "indicators_df": indicators_df,
            }
        except Exception as exc:
            logger.error("[AOI_TT] report generation failed: %s", exc, exc_info=True)
            raise AoiTtReportBuildError("AOI_TT report generation failed.") from exc

    @staticmethod
    def get_aoi_tt_report_data(
        _data_port: "AoiTtDataPort",
        query_config_json: str,
        snapshot_signature: str = "",
        product_revision: str = "",
        decision_signature: str = "",
    ) -> AoiTtReportViewModel:
        """在 Streamlit pickle 缓存边界外构造 ViewModel。"""
        generated, jitter, ratio_signature = _particle_size_runtime_config()
        payload = AoiTtReportService.fetch_aoi_tt_report_payload(
            _data_port=_data_port,
            query_config_json=query_config_json,
            snapshot_signature=snapshot_signature,
            product_revision=product_revision,
            decision_signature=decision_signature,
            generate_particle_sizes=generated,
            particle_ratio_jitter=jitter,
            particle_ratio_signature=ratio_signature,
        )
        return AoiTtReportService._view_model_from_payload(payload)
