"""Default Yield outbound dependency assembly."""
from pathlib import Path

from src.yield_domain.application.ports import YieldDataPort
from src.yield_domain.infrastructure.report_data_adapter import YieldReportDataAdapter


def create_yield_data_port(db_manager=None, data_dir: Path = Path("data")) -> YieldDataPort:
    return YieldReportDataAdapter(db_manager, data_dir)


def yield_alert_policy_signature(product: str) -> str:
    """Policy identity only; do not load Panel facts to validate saved alerts."""
    import hashlib
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.alert_results import RESULT_SCHEMA_VERSION

    root = ConfigLoader.get_project_root()
    parts = [str(RESULT_SCHEMA_VERSION)]
    for path in (root / "config/global.yaml", root / "config/products" / f"{product}.yaml",
                 ConfigLoader.get_domain_config_path("yield_domain")):
        try:
            stat = path.stat()
            parts.append(f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}")
        except FileNotFoundError:
            parts.append(f"{path.resolve()}:missing")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def build_yield_alert_results():
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.alert_results import YieldAlertResults
    from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

    paths = {
        "lot": ConfigLoader.get_domain_resource_path(
            "yield_domain", "lot_oos_results", "sheet_lot/yield_lot_oos_results.xlsx"),
        "trend": ConfigLoader.get_domain_resource_path(
            "yield_domain", "trend_alert_results", "mwd_trend/yield_trend_alert_results.xlsx"),
    }
    return YieldAlertResults(YieldAlertWorkbookStore(paths), policy_signature=yield_alert_policy_signature,
                             ttl_seconds=ConfigLoader.get_cache_ttl_seconds())
