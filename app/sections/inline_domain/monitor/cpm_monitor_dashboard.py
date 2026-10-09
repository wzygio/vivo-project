"""CPM dashboard uses the same layout with independent widget/query keys."""
from collections.abc import Sequence

from app.sections.inline_domain.monitor.cpk_monitor_dashboard import (
    CpkDashboardService, render_capability_monitor_results, render_capability_monitor_section,
)
from app.sections.inline_domain.monitor.refresh_controls import clear_cpk_source_cache
from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel


def render_cpm_monitor_results(view: CpkMonitorViewModel) -> None:
    render_capability_monitor_results(view, metric="cpm")


def render_cpm_monitor_section(
    service: CpkDashboardService, available_products: Sequence[str],
    available_factories: Sequence[str],
) -> None:
    render_capability_monitor_section(
        service, available_products, available_factories, metric="cpm",
        clear_cache=clear_cpk_source_cache, render_results=render_cpm_monitor_results,
    )
