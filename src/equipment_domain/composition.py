"""Default equipment outbound dependency assembly."""
from src.equipment_domain.application.ports import PartsDataPort
from src.equipment_domain.infrastructure.report_data_adapter import PartsReportDataAdapter


def create_parts_data_port(db_manager) -> PartsDataPort:
    return PartsReportDataAdapter(db_manager)
