from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
YIELD_DASHBOARD_PATH = (
    PROJECT_ROOT / "app" / "pages" / "入库不良率分析看板.py"
)


def test_yield_dashboard_keys_cache_by_window_and_manual_resource_revision() -> None:
    source = YIELD_DASHBOARD_PATH.read_text(encoding="utf-8")
    details = (PROJECT_ROOT / 'app/sections/yield_domain/lazy_details.py').read_text(encoding='utf-8')

    assert "build_indicator_product_cache_signature(" in source
    assert "lot_signature=lot_cache_signature" in source
    assert "trend_signature=product_cache_signature" in source
    assert "source_signature=source_signature" in source
    assert "snapshot_signature': source_signature" in details
    assert "analysis_start_date=" in details
    assert "analysis_end_date=" in details
    assert "'modifier_signature':" in details
    assert "warning_signature=" in details
    assert "rate_override_signature=" in details
    assert "resource_revision=source_signature" in source
    assert "get_dashboard_resource_config(active_config, source_signature)" in source
    assert 'YIELD_SOURCE_INDICATOR' in source
    assert 'get_sheet_defect_rates(' not in source
    assert 'get_mapping_data(' not in source
    assert "ExcelService.inject_mapping_config_to_config(active_config)" not in source


def test_yield_dashboard_passes_same_configured_threshold_to_both_sections():
    source = YIELD_DASHBOARD_PATH.read_text(encoding="utf-8")
    assert "code_rate_threshold = get_code_monthly_rate_threshold()" in source
    assert source.count("rate_threshold=code_rate_threshold") == 2
