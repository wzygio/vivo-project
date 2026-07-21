from pathlib import Path

from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService


def test_spc_product_discovery_excludes_non_product_data_dirs(tmp_path: Path) -> None:
    """SPC ALL mode should not scan sibling data-domain folders as products."""
    for dirname in ["equipment", "raw", "processed", "M626", "M678", "Z571"]:
        (tmp_path / dirname).mkdir()

    discovered = MonitorAnalysisService.discover_monitor_products(tmp_path)

    assert discovered == ["M626", "M678", "Z571"]
