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
    policy = ConfigLoader.load_domain_config('yield_domain', 'mwd_trend').get('period_trend', {})
    return PeriodTrendService(PeriodTrendDatabaseSource(modifier, db_manager=db_manager),
        PeriodTrendWorkbook(history), maintained_start=policy['maintained_rate_start'],
        ttl_seconds=ConfigLoader.get_cache_ttl_seconds())


def yield_alert_policy_signature(product: str) -> str:
    """Hash Yield policy content; unrelated config edits never expire results."""
    import hashlib
    import json
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.alert_results import RESULT_SCHEMA_VERSION

    global_config = ConfigLoader._load_resource_global_config()
    domain = ConfigLoader.load_domain_config("yield_domain")
    if not domain:
        raise ValueError("Missing Yield alert policy")
    overrides = domain.pop("product_overrides", {})
    enabled = global_config.get("product_registry", {}).get("enabled_products", [])
    if product not in overrides and product not in enabled:
        raise ValueError(f"Unknown Yield product: {product}")
    product_config = overrides.get(product, {})
    resources = global_config.get("resources", {}).get("yield_domain", {})
    alert_resources = {**resources, "files": {
        key: value for key, value in resources.get("files", {}).items()
        if key != "weekly_ppt_template"
    }}
    policy = {
        "schema": RESULT_SCHEMA_VERSION,
        "global": {key: global_config.get(key) for key in ("data_source", "data_forward", "report_cutoff")},
        "resources": alert_resources if resources else resources,
        "product": {"data_source": {**product_config.get("data_source", {}), "product_code": product},
                    "paths": product_config.get("paths"), "processing": product_config.get("processing")},
        "domain": domain,
    }
    encoded = json.dumps(policy, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False)
    return "yield-alert-policy-v2:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_yield_alert_results(*, workbook_reader=None):
    from src.shared_kernel.config import ConfigLoader
    from src.yield_domain.application.alert_results import YieldAlertResults
    from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

    paths = {
        "lot": ConfigLoader.get_domain_resource_path(
            "yield_domain", "lot_oos_results", "sheet_lot/yield_lot_oos_results.xlsx"),
        "trend": ConfigLoader.get_domain_resource_path(
            "yield_domain", "trend_alert_results", "mwd_trend/yield_trend_alert_results.xlsx"),
    }
    kwargs = {"workbook_reader": workbook_reader} if workbook_reader is not None else {}
    return YieldAlertResults(YieldAlertWorkbookStore(paths, **kwargs), policy_signature=yield_alert_policy_signature,
                             ttl_seconds=ConfigLoader.get_cache_ttl_seconds(),
                             lot_factory_mapping=ConfigLoader.get_yield_lot_factory_mapping)
