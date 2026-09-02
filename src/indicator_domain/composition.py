"""Composition root for indicator-domain outbound adapters."""

from src.indicator_domain.application.ijp.service import IjpReportService
from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.infrastructure.ijp.repository import IjpRepository
from src.indicator_domain.infrastructure.qtime.decoration_repository import (
    QTimeDecorationRepository,
)
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def build_qtime_repository(db_manager: DatabaseManager) -> QTimeRepository:
    return QTimeRepository(db_manager)


def build_qtime_service(db_manager: DatabaseManager) -> QTimeReportService:
    decoration_path = ConfigLoader.get_domain_resource_path(
        "indicator_domain",
        "qtime_oos_decoration",
        "qtime_oos_decoration.xlsx",
    )
    return QTimeReportService(
        build_qtime_repository(db_manager),
        QTimeDecorationRepository(decoration_path),
    )


def build_ijp_repository(db_manager: DatabaseManager) -> IjpRepository:
    return IjpRepository(db_manager)


def build_ijp_service(db_manager: DatabaseManager) -> IjpReportService:
    return IjpReportService(build_ijp_repository(db_manager))
