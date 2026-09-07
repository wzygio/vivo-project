from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
PARTS_PAGE = PROJECT_ROOT / "app" / "pages" / "关键备件报表.py"


def test_parts_report_cache_is_global_and_tracks_its_real_inputs() -> None:
    source = PARTS_PAGE.read_text(encoding="utf-8")

    assert "build_product_cache_signature" not in source
    assert "as_of_date=" in source
    assert "baseline_signature=" in source
    assert "runtime_config_signature=" in source

