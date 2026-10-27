"""Composition root for indicator-domain outbound adapters."""

from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults

from src.indicator_domain.application.ijp.service import IjpReportService
from src.indicator_domain.application.ijp.settings import IjpSettings
from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
from src.indicator_domain.infrastructure.ijp_hole.repository import IjpHoleRepository
from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.chamber_settings import ChamberDecorationSettings
from src.indicator_domain.infrastructure.qtime.chamber_database import DatabaseChamberQTimeRepository
from src.indicator_domain.infrastructure.ijp.repository import IjpRepository
from src.indicator_domain.infrastructure.ijp.alert_workbook import IjpAlertWorkbook
from src.indicator_domain.infrastructure.qtime.decoration_repository import (
    QTimeDecorationRepository,
)
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def build_qtime_repository(db_manager: DatabaseManager) -> QTimeRepository:
    return QTimeRepository(
        db_manager,
        snapshot_dir=(
            ConfigLoader.get_project_root() / "data" / "indicator_domain" / "qtime"
        ),
        snapshot_ttl_hours=ConfigLoader.get_snapshot_ttl_hours(),
    )


def build_qtime_service(db_manager: DatabaseManager) -> QTimeReportService:
    settings = ConfigLoader.load_domain_config("indicator_domain").get("qtime", {})
    decoration_path = ConfigLoader.get_domain_resource_path(
        "indicator_domain",
        "qtime_oos_decoration",
        "qtime_oos_decoration.xlsx",
    )
    return QTimeReportService(
        build_qtime_repository(db_manager),
        QTimeDecorationRepository(decoration_path),
        spec_overrides=settings.get("spec_overrides", {}),
        constrain_display=settings.get("constrain_display", False),
        alert_results=build_qtime_alert_results(),
    )


def build_qtime_alert_results() -> "QTimeAlertResults":
    """Saved-result consumer and producer; constructing it never touches a database."""
    import hashlib
    from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults
    from src.indicator_domain.infrastructure.qtime.alert_workbook import QTimeAlertWorkbook

    return QTimeAlertResults(
        QTimeAlertWorkbook(ConfigLoader.get_domain_resource_path("indicator_domain", "qtime_alerts")),
        ttl_seconds=ConfigLoader.get_cache_ttl_seconds(),
        policy_signature=lambda: hashlib.sha256(repr((
            "qtime-published-v1", ConfigLoader.get_indicator_data_exclusion(),
            ConfigLoader.get_data_forward_policy().signature,
            ConfigLoader.get_report_cutoff_policy().signature,
            ConfigLoader.load_domain_config("indicator_domain").get("qtime", {}),
            _qtime_decision_signature(),
        )).encode()).hexdigest(),
    )


def _qtime_decision_signature() -> tuple[int, int] | None:
    from src.indicator_domain.application.qtime.cached_monitoring import get_qtime_decision_file_stat
    return get_qtime_decision_file_stat(ConfigLoader.get_domain_resource_path(
        "indicator_domain", "qtime_oos_decoration", "qtime_oos_decoration.xlsx",
    ))


def read_qtime_alert_result(product: str, reference_date: date, **selection) -> dict:
    return build_qtime_alert_results().read(product, reference_date, **selection)


def qtime_alert_result_signature(product: str, reference_date: date, **selection) -> str:
    return build_qtime_alert_results().source_signature(product, reference_date, **selection)


def build_chamber_qtime_service(db_manager: DatabaseManager) -> ChamberQTimeService:
    settings = ConfigLoader.load_domain_config('indicator_domain')['qtime']
    return ChamberQTimeService(
        DatabaseChamberQTimeRepository(db_manager, target_seconds=settings['chamber_target_seconds']),
        enabled_products=tuple(ConfigLoader.get_enabled_products()),
        target_seconds=settings['chamber_target_seconds'],
        cache_namespace='qtime-chamber-production',
        decoration_settings=ChamberDecorationSettings.model_validate(
            settings.get('chamber_point_decoration', {}),
        ),
    )


def build_ijp_repository(db_manager: DatabaseManager) -> IjpRepository:
    return IjpRepository(db_manager)


def build_ijp_service(db_manager: DatabaseManager) -> IjpReportService:
    return IjpReportService(
        build_ijp_repository(db_manager),
        enabled_product_codes=tuple(ConfigLoader.get_enabled_products()),
        work_order_types=tuple(ConfigLoader.get_work_order_types()),
        settings=IjpSettings.model_validate(
            ConfigLoader.load_domain_config("indicator_domain").get("ijp", {})
        ),
        alert_store=build_ijp_alert_store(),
    )


def build_ijp_hole_service(db_manager: DatabaseManager) -> IjpHoleReportService:
    return IjpHoleReportService(
        IjpHoleRepository(db_manager),
        enabled_products=tuple(ConfigLoader.get_enabled_products()),
        work_order_types=tuple(ConfigLoader.get_work_order_types()),
        alert_store=build_ijp_alert_store(),
    )


def build_ijp_alert_store() -> IjpAlertWorkbook:
    return IjpAlertWorkbook(ConfigLoader.get_domain_resource_path(
        'indicator_domain', 'ijp_alerts', 'ijp/ijp_alerts.xlsx',
    ))
