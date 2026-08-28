from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
YIELD_DASHBOARD_PATH = (
    PROJECT_ROOT / "app" / "pages" / "入库不良率分析看板.py"
)


def test_yield_dashboard_refreshes_modifier_data_only_through_product_revision() -> None:
    """修饰表文件时间不得绕过页头按钮直接使页面缓存失效。"""
    source = YIELD_DASHBOARD_PATH.read_text(encoding="utf-8")

    assert "build_product_cache_signature(" in source
    assert "compute_snapshot_signature(" not in source
    assert "modifier_signature=" not in source
