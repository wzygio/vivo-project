"""Macro-independent detail loading and explicitly opened Code charts."""
from collections import OrderedDict
from collections.abc import Callable
import logging
from pathlib import Path
import time

import pandas as pd
import streamlit as st

from src.shared_kernel.config import ConfigLoader
from src.yield_domain.application.report_cache import report_operation, YieldSourceGenerationChanged
from yield_domain.application.yield_service import YieldAnalysisService
from app.sections.yield_domain.chart_data import prepare_lot_chart_index


@report_operation
def load_report_details(config, product_dir: Path, *, source_signature: str,
                        cache_context: dict, db_manager=None, expected_health: dict | None = None) -> dict:
    try:
        return _load_report_details(config, product_dir, source_signature=source_signature,
                                    cache_context=cache_context, db_manager=db_manager,
                                    expected_health=expected_health)
    except YieldSourceGenerationChanged:
        YieldAnalysisService.invalidate_data_health(
            config, snapshot_signature=source_signature, _db_manager=db_manager,
            analysis_start_date=cache_context['analysis_start_date'],
            analysis_end_date=cache_context['analysis_end_date'],
        )
        raise


def _load_report_details(config, product_dir: Path, *, source_signature: str,
                         cache_context: dict, db_manager=None, expected_health: dict | None = None) -> dict:
    """Only requested details call this; failures stay local to each capability."""
    result = {'sheet_data': None, 'mapping_data': None, 'errors': []}
    common = {'_db_manager': db_manager, 'snapshot_signature': source_signature,
              'analysis_start_date': cache_context['analysis_start_date'],
              'analysis_end_date': cache_context['analysis_end_date'],
              'modifier_signature': cache_context['modifier_signature']}
    if expected_health is not None:
        current = YieldAnalysisService.get_data_health(
            config, _db_manager=db_manager, snapshot_signature=source_signature,
            analysis_start_date=cache_context['analysis_start_date'],
            analysis_end_date=cache_context['analysis_end_date'],
        )
        _, current_end = YieldAnalysisService.get_time_window()
        if current != expected_health or current_end.date().isoformat() != cache_context['analysis_end_date']:
            raise YieldSourceGenerationChanged('YIELD_REPORT_GENERATION_CHANGED')
    try:
        result['sheet_data'] = YieldAnalysisService.get_sheet_defect_rates(
            config, product_dir, warning_signature=cache_context['warning_signature'],
            rate_override_signature=cache_context['rate_override_signature'], **common,
        )
    except YieldSourceGenerationChanged:
        raise
    except Exception:
        logging.exception('YIELD_SHEET_DETAILS_UNAVAILABLE')
        result['errors'].append('Sheet 分布暂时无法加载，请稍后重试。')
    try:
        result['mapping_data'] = YieldAnalysisService.get_mapping_data(
            config, product_dir=product_dir, **common,
        )
    except YieldSourceGenerationChanged:
        raise
    except Exception:
        logging.exception('YIELD_MAPPING_DETAILS_UNAVAILABLE')
        result['errors'].append('Mapping 图像暂时无法加载，请稍后重试。')
    return result


def _memoized_code_payload(key: str, version: str, build: Callable[[], dict]) -> dict:
    # Figures live only in bounded session state, never in data caches.
    memo = st.session_state.setdefault('yield_detail_chart_payloads', OrderedDict())
    now = time.monotonic()
    entry = memo.get(key)
    if entry is None or entry['version'] != version or now - entry['created'] > ConfigLoader.get_cache_ttl_seconds():
        entry = {'version': version, 'created': now, 'payload': build()}
        memo[key] = entry
    memo.move_to_end(key)
    while len(memo) > 16:
        memo.popitem(last=False)
    return entry['payload']


@report_operation
def render_lazy_code_groups(
    *, pairs: list[tuple[str, str]], warning_lines: dict, mwd_code_data: dict,
    lot_data: dict, detail_loader: Callable[[], dict], hotspot_scripts: list,
    product_code: str, mapping_layout: dict | None, data_signature: str,
    key_prefix: str, first_expanded: bool = False,
) -> None:
    from app.sections.yield_domain.yield_dashboard import (
        _build_code_expander_label, _build_compact_render_payload, _state_key_fragment,
        render_compact_payload_contents,
    )

    loaded = None
    indexed_lots = None
    scoped_prefix = f'{key_prefix}:{product_code}'
    for position, (group, code) in enumerate(pairs):
        upper = float((warning_lines.get(code) or {}).get('upper', .002))
        fragment = _state_key_fragment(scoped_prefix, group, code)
        key = f'yield_detail_open_{product_code}_{fragment}'
        with st.expander(_build_code_expander_label(mwd_code_data, code, upper),
                         key=key, expanded=first_expanded and position == 0,
                         on_change='rerun') as expander:
            if not expander.open:
                continue
            if loaded is None:
                try:
                    loaded = detail_loader()
                except YieldSourceGenerationChanged:
                    st.rerun(scope='app')
                indexed_lots = {**(lot_data or {}), '_chart_code_index': prepare_lot_chart_index(lot_data, data_signature)}
                mapping = loaded.get('mapping_data')
                if isinstance(mapping, pd.DataFrame):
                    mapping.attrs['yield_chart_signature'] = data_signature
            for message in loaded.get('errors', []):
                st.warning(message)
            selected_lot = str(st.session_state.get(f'compact_sheet_lot_{fragment}', ''))
            version = repr((data_signature, group, code, upper, selected_lot,
                            hotspot_scripts, mapping_layout))

            def build():
                return _build_compact_render_payload(
                    mwd_code_data, indexed_lots, loaded.get('sheet_data'), loaded.get('mapping_data'),
                    group, code, upper, hotspot_scripts, product_code, mapping_layout,
                    key_prefix=scoped_prefix,
                )

            payload = build() if loaded.get('errors') else _memoized_code_payload(key, version, build)
            render_compact_payload_contents(payload)


@st.fragment
def render_lazy_alert_code_expanders(
    *, pairs: list[tuple[str, str]], warning_lines: dict, mwd_code_data: dict,
    lot_data: dict, detail_loader: Callable[[], dict], hotspot_scripts: list,
    product_code: str, mapping_layout: dict | None, data_signature: str,
    chart_key_prefix: str,
) -> None:
    with st.expander(f'🚨 自动预警缺陷图像（{len(pairs)} 个 Code）',
                     key=f'yield_automatic_details_{product_code}', on_change='rerun') as expander:
        if not expander.open:
            return
        st.caption('展开缺陷名称可查看趋势、Mapping 和 Lot 分布。')
        render_lazy_code_groups(
            pairs=pairs, warning_lines=warning_lines or {}, mwd_code_data=mwd_code_data,
            lot_data=lot_data, detail_loader=detail_loader, hotspot_scripts=hotspot_scripts,
            product_code=product_code, mapping_layout=mapping_layout,
            data_signature=data_signature, key_prefix=chart_key_prefix,
        )
