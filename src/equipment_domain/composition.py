"""Default equipment outbound dependency assembly."""
from src.equipment_domain.application.ports import PartsDataPort
from src.equipment_domain.infrastructure.report_data_adapter import PartsReportDataAdapter
from src.equipment_domain.infrastructure.parts_alert_workbook import PartsAlertWorkbook
from src.shared_kernel.config import ConfigLoader


def create_parts_data_port(db_manager) -> PartsDataPort:
    return PartsReportDataAdapter(db_manager)


def create_parts_alert_store() -> PartsAlertWorkbook:
    return PartsAlertWorkbook(ConfigLoader.get_domain_resource_path(
        'equipment_domain', 'parts_alerts', 'parts/parts_alerts.xlsx',
    ))
