"""Default Yield outbound dependency assembly."""
from pathlib import Path

from src.yield_domain.application.ports import YieldDataPort
from src.yield_domain.infrastructure.report_data_adapter import YieldReportDataAdapter


def create_yield_data_port(db_manager=None, data_dir: Path = Path("data")) -> YieldDataPort:
    return YieldReportDataAdapter(db_manager, data_dir)


def build_period_trend_service(db_manager=None):
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.period_trend import PeriodTrendService
    from src.yield_domain.infrastructure.period_trend_source import PeriodTrendDatabaseSource
    from src.yield_domain.infrastructure.period_trend_workbook import PeriodTrendWorkbook

    modifier = ConfigLoader.get_domain_resource_path('yield_domain', 'yield_modifier_table')
    history = ConfigLoader.get_domain_resource_path('yield_domain', 'period_trend_history')
    policy = ConfigLoader.load_domain_config('yield_domain').get('period_trend', {})
    return PeriodTrendService(PeriodTrendDatabaseSource(modifier, db_manager=db_manager),
        PeriodTrendWorkbook(history), maintained_start=policy['maintained_rate_start'],
        ttl_seconds=ConfigLoader.get_cache_ttl_seconds())


def yield_alert_policy_signature(product: str) -> str:
    """Hash Yield policy content; unrelated config edits never expire results."""
    import hashlib
    import json
    import yaml
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.alert_results import RESULT_SCHEMA_VERSION

    root = ConfigLoader.get_project_root()
    def read_mapping(path: Path) -> dict:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
        if not isinstance(value, dict):
            raise ValueError("Yield alert policy requires mapping configuration")
        return value

    global_config = read_mapping(root / "config/global.yaml")
    product_config = read_mapping(root / "config/products" / f"{product}.yaml")
    policy = {
        "schema": RESULT_SCHEMA_VERSION,
        "global": {key: global_config.get(key) for key in ("data_source", "data_forward", "report_cutoff")},
        "resources": global_config.get("resources", {}).get("yield_domain"),
        "product": {key: product_config.get(key) for key in ("data_source", "paths", "processing")},
        "domain": read_mapping(ConfigLoader.get_domain_config_path("yield_domain")),
    }
    encoded = json.dumps(policy, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False)
    return "yield-alert-policy-v2:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
