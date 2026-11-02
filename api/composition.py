"""Read-only cross-domain composition; no producer or DB object is constructed."""
from __future__ import annotations

from datetime import date
from typing import Any

from api.schemas import AnomalyQuery
from api.service import PublishedAnomalyService
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.core.alert_visibility import parse_matrix_config
from src.shared_kernel.infrastructure.api_alert_metadata import ApiAlertMetadata
from src.shared_kernel.infrastructure.api_workbook_cache import BoundedWorkbookCache

DISPLAY_NAMES = {
    'SPC': 'SPC 单片异常', 'CTQ': 'CTQ 单片异常',
    'AOI_TT': 'AOI_TT 单片异常', 'AOI_RS': 'AOI_RS 单片异常',
    'Yield': 'Yield 单片异常（Lot 超规）', 'Q-Time': 'Q-Time 单片异常',
}


def build_service() -> PublishedAnomalyService:
    reader = BoundedWorkbookCache()

    def hidden(domain: str, product: str) -> bool:
        path = ConfigLoader.get_compliance_config_path()
        stat = path.stat()
        frames = reader(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        switches = parse_matrix_config(frames['全指标预警看板'])
        key = (DISPLAY_NAMES[domain], product)
        # A missing public policy is not permission to disclose a source.
        if key not in switches:
            raise ValueError('Public policy unavailable')
        return switches[key]

    def source(query: AnomalyQuery, day: date, factories: tuple[str, ...]) -> dict:
        if query.sub_domain in ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS'):
            from src.inline_domain.composition import build_sheet_alert_query
            scope = query.sub_domain.lower()
            return build_sheet_alert_query(scope, query.event_type, workbook_reader=reader, strict_empty=True).read(scope, query.product, day)
        if query.sub_domain == 'Q-Time':
            from src.indicator_domain.composition import build_qtime_alert_results
            shops = tuple(shop for shop in factories if shop in ('ARRAY', 'OLED', 'TP'))
            if not shops:
                import pandas as pd
                return {'state': 'ok', 'records': pd.DataFrame(columns=['flag']), 'release_evidence': True}
            return build_qtime_alert_results(workbook_reader=reader).read(query.product, day, shops=shops)
        if query.sub_domain == 'Yield':
            from src.yield_domain.composition import build_yield_alert_results
            result = build_yield_alert_results(workbook_reader=reader).read(query.product, 'lot', day)
            return {'state': 'ok' if result.status in ('ready', 'no_data') else 'error',
                    'records': result.records, 'release_records': result.release_records,
                    'release_evidence': result.release_evidence}
        raise ValueError('Unknown result source')

    # Resource changes are resolved on each call; a long-lived process must not
    # keep reading an old configured path after an operator relocation.
    class Metadata:
        def enrich(self, *args: Any) -> dict[str, str | None]:
            path = ConfigLoader.get_domain_resource_path('shared_kernel', 'api_alert_metadata')
            return ApiAlertMetadata(path, workbook_reader=reader).enrich(*args)

    return PublishedAnomalyService(source_reader=source, hidden=hidden, metadata=Metadata())
