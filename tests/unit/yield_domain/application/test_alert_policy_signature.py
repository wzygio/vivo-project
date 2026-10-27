import os

import pytest
import yaml

from src.shared_kernel.config import ConfigLoader
from src.yield_domain.composition import yield_alert_policy_signature


@pytest.fixture
def policy_files(tmp_path, monkeypatch):
    paths = {
        'global': tmp_path / 'config/global.yaml',
        'product': tmp_path / 'config/products/P1.yaml',
        'domain': tmp_path / 'config/domain/yield_domain.yaml',
    }
    values = {
        'global': {'application': {'cache_ttl_hours': 12}, 'data_forward': {'enabled': False},
                   'report_cutoff': {'latest_day_time': '12:00:00'}, 'data_source': {'factory': 'ARRAY'},
                   'resources': {'yield_domain': {'files': {'static_warning_lines': 'spec.xlsx'}},
                                 'inline_domain': {'files': {}}, 'indicator_domain': {'files': {}}}},
        'product': {'data_source': {'product_code': 'P1'}, 'processing': {'threshold': .1}},
        'domain': {'dashboard': {'enabled': True}},
    }
    for key, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(values[key]), encoding='utf-8')
    monkeypatch.setattr(ConfigLoader, 'get_project_root', staticmethod(lambda: tmp_path))
    return paths, values


def test_unrelated_resource_registration_and_touch_do_not_expire_yield_results(policy_files):
    paths, values = policy_files
    before = yield_alert_policy_signature('P1')
    stat = paths['global'].stat()
    os.utime(paths['global'], ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000000))
    assert yield_alert_policy_signature('P1') == before
    values['global']['resources']['indicator_domain']['files']['qtime_alerts'] = 'qtime.xlsx'
    values['global']['resources']['inline_domain']['files']['aoi_rs_trend_alerts'] = 'rs.xlsx'
    paths['global'].write_text(yaml.safe_dump(values['global']), encoding='utf-8')
    assert yield_alert_policy_signature('P1') == before


@pytest.mark.parametrize('part', ['product', 'domain', 'time_policy', 'yield_resource'])
def test_real_yield_policy_changes_still_invalidate_results(policy_files, part):
    paths, values = policy_files
    before = yield_alert_policy_signature('P1')
    if part == 'product':
        values['product']['processing']['threshold'] = .2
        key = 'product'
    elif part == 'domain':
        values['domain']['dashboard']['enabled'] = False
        key = 'domain'
    elif part == 'time_policy':
        values['global']['data_forward']['enabled'] = True
        key = 'global'
    else:
        values['global']['resources']['yield_domain']['files']['static_warning_lines'] = 'other.xlsx'
        key = 'global'
    paths[key].write_text(yaml.safe_dump(values[key]), encoding='utf-8')
    assert yield_alert_policy_signature('P1') != before
