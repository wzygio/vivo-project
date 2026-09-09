"""Default Yield outbound dependency assembly."""
from pathlib import Path

from src.yield_domain.application.ports import YieldDataPort
from src.yield_domain.infrastructure.report_data_adapter import YieldReportDataAdapter


def create_yield_data_port(db_manager=None, data_dir: Path = Path("data")) -> YieldDataPort:
    return YieldReportDataAdapter(db_manager, data_dir)
