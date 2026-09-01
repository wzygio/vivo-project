"""Composition root for Q-Time outbound adapters."""

from src.qtime_domain.infrastructure.ijp_repository import IjpRepository
from src.qtime_domain.infrastructure.qtime_repository import QTimeRepository
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def build_qtime_repository(db_manager: DatabaseManager) -> QTimeRepository:
    return QTimeRepository(db_manager)


def build_ijp_repository(db_manager: DatabaseManager) -> IjpRepository:
    return IjpRepository(db_manager)
