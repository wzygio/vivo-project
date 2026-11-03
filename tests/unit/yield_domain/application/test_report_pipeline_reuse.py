from collections import Counter
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from yield_domain.application import yield_service as module
from src.shared_kernel.data_health import attach_data_health, make_data_health
from src.yield_domain.application.alert_publication import generate_yield_alert_publications
from src.yield_domain.application.report_cache import report_cache_scope

Service = module.YieldAnalysisService


@pytest.fixture(autouse=True)
def clear_service_caches():
    for name in dir(Service):
        method = getattr(Service, name)
        if hasattr(method, 'clear'):
            method.clear()
    yield


def test_shared_source_reuses_calculations_preserving_indicator_and_read_mode_keys(monkeypatch, mock_config, tmp_path):
    counts = Counter()
    mock_config.processing['defect_multipliers'] = {}
    mock_config.processing['array_input_time'] = {'query_enabled': False}
    health = make_data_health('fresh', refreshed_at=datetime.now().isoformat())
    panel = attach_data_health(pd.DataFrame({'lot_id': ['L1'], 'panel_id': ['P1']}), health)

    def raw(*_args, **_kwargs):
        counts['raw'] += 1
        return panel.copy()

    def overrides(*_args):
        counts['overrides'] += 1
        return None, None

    port = SimpleNamespace(read_panel=raw, read_rate_overrides=overrides,
                           read_array_times=lambda *_: pytest.fail('Array is disabled'))
    monkeypatch.setattr(module, '_resolve_data_port', lambda *_: port)

    def context(*args, **kwargs):
        counts['context'] += 1
        return {'targets': {}, 'group_targets': {}, 'factors': {}, 'signature': 'x'}

    def code(**kwargs):
        counts['code'] += 1
        return {'weekly': pd.DataFrame({'time_period': ['2026-W44'], 'defect_group': ['G'],
                                       'defect_desc': ['C'], 'defect_rate': [.01]})}

    def group(**kwargs):
        counts['group'] += 1
        return code_result

    code_result = code()
    counts.clear()
    monkeypatch.setattr(Service, '_build_modifier_context', staticmethod(context))
    monkeypatch.setattr(module.MWDTrendProcessor, 'create_code_level_mwd_trend_data', staticmethod(code))
    monkeypatch.setattr(module.MWDTrendProcessor, 'create_mwd_trend_data', staticmethod(group))
    monkeypatch.setattr(Service, 'load_static_warning_lines', staticmethod(lambda *_args, **_kwargs: {}))

    def lot(**kwargs):
        counts['lot'] += 1
        return {'code_level_details': {'G': pd.DataFrame({'lot_id': ['L1'], 'defect_desc': ['C'],
            'warehousing_time': ['20261028'], 'defect_rate': [.01]})}}

    def sheet(**kwargs):
        counts['sheet'] += 1
        return {'code_level_details': {}}

    monkeypatch.setattr(module, 'calculate_lot_defect_rates', lot)
    monkeypatch.setattr(module, 'calculate_sheet_defect_rates', sheet)
    publisher = SimpleNamespace(policy_signature=lambda *_: 'p', lot_factory_mapping=lambda: {},
                                publish=lambda *_args, **_kwargs: True,
                                mark_failed=lambda *_: pytest.fail('Unexpected publication failure'))
    window = {'analysis_start_date': '2026-08-01', 'analysis_end_date': '2026-11-03'}
    resources = {'modifier_signature': 'm', 'warning_signature': 'w', 'rate_override_signature': 'r'}

    def produce(source, trend='t'):
        return generate_yield_alert_publications(
            analysis=Service, publisher=publisher, config=mock_config, product_dir=tmp_path,
            cache_context={**window, **resources}, trend_signature=trend, lot_signature='l',
            source_signature=source, db_manager=None,
        )

    produced = produce('source')
    assert not produced['errors']
    generation = produced['source_signature']
    with report_cache_scope():
        Service.get_sheet_defect_rates(mock_config, tmp_path, snapshot_signature=generation, **window, **resources)
    assert counts == Counter(raw=1, context=1, code=1, group=1, lot=1, sheet=1, overrides=1)
    # A display-only indicator change does not invent a new source generation.
    assert not produce('source', trend='t2')['errors']
    assert counts['raw'] == counts['code'] == counts['lot'] == 1
    Service.get_code_level_trend_data(mock_config, tmp_path, snapshot_signature=generation,
                                     modifier_signature='m', read_only=True, **window)
    assert counts['context'] == counts['code'] == 2
    assert counts['raw'] == 1
    assert not produce('new-source')['errors']
    assert counts['raw'] == 2 and counts['lot'] == 2
    monkeypatch.setattr(Service, 'get_raw_panel_details', lambda *_args, **_kwargs: pytest.fail('Health must use its small payload'))
    assert Service.get_data_health(mock_config, snapshot_signature='new-source', **window) == health


def test_array_query_is_disabled_by_default_and_can_be_explicitly_enabled(mock_config):
    seen = []
    port = SimpleNamespace(read_array_times=lambda *args: seen.append(args) or pd.DataFrame())
    mock_config.processing['array_input_time'] = {}
    assert Service._get_array_times(('L1',), mock_config, _data_port=port).empty
    assert not seen
    mock_config.processing['array_input_time']['query_enabled'] = True
    Service._get_array_times(('L1',), mock_config, _data_port=port)
    assert len(seen) == 1


def test_generation_is_validated_at_actual_raw_return_before_derived_cache_commit(monkeypatch, mock_config):
    from src.yield_domain.application.report_cache import bind_source_generation, YieldSourceGenerationChanged

    mock_config.processing['defect_multipliers'] = {}
    window = {'analysis_start_date': '2026-08-01', 'analysis_end_date': '2026-11-03'}
    first = make_data_health('fresh', refreshed_at='2026-11-03T08:00:00')
    second = make_data_health('fresh', refreshed_at='2026-11-03T09:00:00')
    active = {'health': first}
    port = SimpleNamespace(read_panel=lambda *_: attach_data_health(pd.DataFrame({'panel_id': ['P']}), active['health']))
    monkeypatch.setattr(module, '_resolve_data_port', lambda *_: port)
    assert Service.get_data_health(mock_config, snapshot_signature='root', **window) == first
    generation = bind_source_generation('root', first)
    query, policy = Service._build_panel_request(mock_config, **window)
    Service.get_raw_panel_details.clear(query.model_dump_json(), policy.model_dump_json(), snapshot_signature='root')
    active['health'] = second
    with pytest.raises(YieldSourceGenerationChanged):
        Service.get_modified_panel_details(mock_config, snapshot_signature=generation, **window)
    # Refreshing just the small metadata enables a fresh coherent page instead
    # of repeatedly recreating the old captured token.
    Service.invalidate_data_health(mock_config, snapshot_signature=generation, **window)
    assert Service.get_data_health(mock_config, snapshot_signature='root', **window) == second
    new_generation = bind_source_generation('root', second)
    assert Service.get_modified_panel_details(mock_config, snapshot_signature=new_generation, **window).attrs['data_health'] == second
