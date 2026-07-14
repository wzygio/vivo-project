from app.components.page_header import extract_cached_funcs
from src.equipment_domain.application.parts_service import PartsReportService
from src.spc_domain.application.cpm_service import CpmReportService


def test_report_services_expose_reload_safe_payload_caches_to_page_refresh() -> None:
    cached_functions = extract_cached_funcs(CpmReportService, PartsReportService)
    cached_function_names = {cached_function.__name__ for cached_function in cached_functions}

    assert "fetch_cpm_report_payload" in cached_function_names
    assert "fetch_report_payload" in cached_function_names
    assert "get_cpm_report_data" not in cached_function_names
    assert "get_report_data" not in cached_function_names
