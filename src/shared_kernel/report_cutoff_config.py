"""Load report cutoff settings independently of ConfigLoader class versions."""

from typing import Any

from src.shared_kernel.report_cutoff import ReportCutoffPolicy


def load_report_cutoff_policy(config_loader: Any = None) -> ReportCutoffPolicy:
    """Use established config APIs, including on classes retained by hot reload."""
    if config_loader is None:
        from src.shared_kernel.config import ConfigLoader

        config_loader = ConfigLoader
    path = config_loader.get_project_root() / "config" / "global.yaml"
    settings = config_loader._load_yaml(path).get("report_cutoff", {})
    if not isinstance(settings, dict):
        raise ValueError("global.yaml: 'report_cutoff' must be a mapping")
    return ReportCutoffPolicy(latest_day_time=settings.get("latest_day_time", "12:00:00"))
