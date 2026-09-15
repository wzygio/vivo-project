"""Bind the IQC use case to the shared database lifecycle."""

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.infrastructure.eva_materials.evaporation_repository import EvaporationRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.infrastructure.lifetime.lifetime_repository import LifetimeRepository
from src.iqc_domain.infrastructure.lifetime.m3_database import get_m3_engine


def build_evaporation_service() -> EvaporationReportService:
    return EvaporationReportService(EvaporationRepository(
        DatabaseManager(), ConfigLoader.get_data_forward_policy(),
        ConfigLoader.get_report_cutoff_policy(),
    ))


def build_lifetime_service() -> LifetimeReportService:
    return LifetimeReportService(LifetimeRepository(get_m3_engine()))
