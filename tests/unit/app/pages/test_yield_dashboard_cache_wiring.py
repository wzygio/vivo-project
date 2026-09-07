from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
YIELD_DASHBOARD_PATH = (
    PROJECT_ROOT / "app" / "pages" / "入库不良率分析看板.py"
)


def test_yield_dashboard_keys_cache_by_window_and_external_resources() -> None:
    source = YIELD_DASHBOARD_PATH.read_text(encoding="utf-8")

    assert "build_product_cache_signature(" in source
    assert "analysis_start_date=" in source
    assert "analysis_end_date=" in source
    assert "modifier_signature=" in source
    assert "warning_signature=" in source
    assert "rate_override_signature=" in source
