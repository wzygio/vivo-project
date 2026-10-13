"""Bind the IQC use case to the shared database lifecycle."""

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.infrastructure.eva_materials.evaporation_repository import EvaporationRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.infrastructure.lifetime.lifetime_repository import LifetimeRepository
from src.iqc_domain.infrastructure.lifetime.m3_database import get_m3_engine


def build_evaporation_service(
    *, enabled_products: tuple[str, ...] | None = None,
    work_order_types: tuple[str, ...] | None = None,
) -> EvaporationReportService:
    return EvaporationReportService(EvaporationRepository(
        DatabaseManager(), ConfigLoader.get_data_forward_policy(),
        ConfigLoader.get_report_cutoff_policy(),
        tuple(ConfigLoader.get_work_order_types()) if work_order_types is None else work_order_types,
    ), enabled_products=(
        tuple(ConfigLoader.get_enabled_products(strict=True)) if enabled_products is None else enabled_products
    ))


def build_lifetime_service() -> LifetimeReportService:
    return LifetimeReportService(LifetimeRepository(get_m3_engine()))
