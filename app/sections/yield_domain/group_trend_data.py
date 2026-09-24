"""Read Group trends using the warning matrix's existing application cache keys."""

from app.components.indicator_cache import build_indicator_product_cache_signature
from src.shared_kernel.config import ConfigLoader
from yield_domain.application.yield_service import YieldAnalysisService


YIELD_SNAPSHOT_SIGNATURE_BASE = "yield_dashboard_manual_refresh_v1"


def load_product_group_trends(product_code: str, *, db_manager=None) -> dict:
    """Reuse read-only MWD entries; do not introduce a second board-level cache.

    The warning matrix uses file revisions and read_only=True. The single-product
    report uses a manual resource revision and writable mode, so its L2 entries
    are intentionally distinct even though the indicator revision is shared.
    """
    config = ConfigLoader.load_config(product_code)
    product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / product_code
    snapshot_signature = build_indicator_product_cache_signature(
        YIELD_SNAPSHOT_SIGNATURE_BASE, product_code, ("yield_trend_fluctuation",),
    )
    context = YieldAnalysisService.build_cache_context(config, product_dir)
    health = YieldAnalysisService.get_data_health(
        config,
        _db_manager=db_manager,
        snapshot_signature=snapshot_signature,
        analysis_start_date=context["analysis_start_date"],
        analysis_end_date=context["analysis_end_date"],
    )
    if health.get("status") == "unavailable":
        return {"data_health": health, "trends": None}
    trends = YieldAnalysisService.get_mwd_trend_data(
        config,
        product_dir,
        _db_manager=db_manager,
        snapshot_signature=snapshot_signature,
        read_only=True,
        analysis_start_date=context["analysis_start_date"],
        analysis_end_date=context["analysis_end_date"],
        modifier_signature=context["modifier_signature"],
    )
    return {"data_health": health, "trends": trends}
