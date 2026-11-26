"""Read-only cross-domain composition; no producer or DB object is constructed."""
from __future__ import annotations

from datetime import date
import logging
from typing import Any

import pandas as pd

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
CAPABILITY_DISPLAY_NAMES = {
    'CPK': 'SPC 趋势波动（CPK）', 'CPM': 'SPC 趋势波动（CPM）',
}


def build_service() -> PublishedAnomalyService:
    reader = BoundedWorkbookCache()

    def empty_source() -> dict:
        return {'state': 'ok', 'records': pd.DataFrame(columns=['flag']),
                'release_evidence': True}

    def hidden_row(display_name: str, product: str) -> bool:
        path = ConfigLoader.get_compliance_config_path()
        stat = path.stat()
        frames = reader(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        switches = parse_matrix_config(frames['全指标预警看板'])
        key = (display_name, product)
        # A missing public policy is not permission to disclose a source.
        if key not in switches:
            raise ValueError('Public policy unavailable')
        return switches[key] is not False

    def hidden(domain: str, product: str) -> bool:
        return hidden_row(DISPLAY_NAMES[domain], product)

    def capability_hidden(event_type: str, product: str) -> bool:
        return hidden_row(CAPABILITY_DISPLAY_NAMES[event_type], product)

    def source(query: AnomalyQuery, day: date, factories: tuple[str, ...]) -> dict:
        if query.product is None or query.sub_domain is None:
            raise ValueError('Source query must be resolved')
        dates = {'start_date': query.start_date.isoformat() if query.start_date else None,
                 'end_date': query.end_date.isoformat() if query.end_date else None}
        if query.sub_domain in ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS'):
            from src.inline_domain.composition import read_published_inline_alerts

            records = read_published_inline_alerts(query.product, query.sub_domain.lower(), query.event_type,
                                                  day, workbook_reader=reader, **dates)
            return {'state': 'ok', 'records': records}
        if query.sub_domain == 'Q-Time':
            from src.indicator_domain.composition import build_qtime_alert_results
            shops = tuple(shop for shop in factories if shop in ('ARRAY', 'OLED', 'TP'))
            if not shops:
                return {'state': 'ok', 'records': pd.DataFrame(columns=['flag']), 'release_evidence': True}
            return build_qtime_alert_results(workbook_reader=reader).read(query.product, day, shops=shops, check_freshness=False, **dates)
        if query.sub_domain == 'Yield':
            from src.yield_domain.composition import build_yield_alert_results
            publication = build_yield_alert_results(workbook_reader=reader)
            # Yield, like Inline, creates product publications only as needed.
            # The store distinguishes true absence from unreadable workbooks
            # and incomplete existing data/metadata/decision-sheet sets.
            if publication.store.is_product_absent(query.product, 'lot'):
                return empty_source()
            result = publication.read(query.product, 'lot', day, check_freshness=False, **dates)
            if result.status not in ('ready', 'no_data'):
                logging.getLogger(__name__).warning(
                    'API_PUBLICATION_UNAVAILABLE product=%s sub_domain=Yield status=%s',
                    query.product, result.status,
                )
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

    def sheet_membership(query: AnomalyQuery, row: dict) -> list[str]:
        from src.inline_domain.composition import build_spc_sheet_membership_query

        return build_spc_sheet_membership_query(query.product).sheet_ids(
            product=query.product, factory=str(row['factory']).upper(),
            step_id=str(row['step_id']), parameter_name=str(row['param_name']),
            start_date=row['calendar_start'], end_date=row['calendar_end'],
        )

    return PublishedAnomalyService(source_reader=source, hidden=hidden, metadata=Metadata(),
                                   capability_hidden=capability_hidden,
                                   sheet_membership=sheet_membership)
