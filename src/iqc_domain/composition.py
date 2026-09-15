"""Bind the IQC use case to the shared database lifecycle."""

from src.iqc_domain.application.evaporation import EvaporationReportService
from src.iqc_domain.infrastructure.evaporation_repository import EvaporationRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def build_evaporation_service() -> EvaporationReportService:
    return EvaporationReportService(EvaporationRepository(
        DatabaseManager(), ConfigLoader.get_data_forward_policy(),
        ConfigLoader.get_report_cutoff_policy(),
    ))
