from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from app.sections.inline_domain.monitor import published_alert_snapshots as saved
from src.shared_kernel.config import ConfigLoader
from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS
from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS


@pytest.fixture
def snapshots(tmp_path, monkeypatch):
    paths = {key: tmp_path / f'{key}.xlsx' for key in ('ijp_alerts', 'parts_alerts', 'evaporation_alerts')}
    original = ConfigLoader.get_domain_resource_path
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path',
        lambda domain, key, default=None: paths[key] if key in paths else original(domain, key, default))
    saved.read_snapshot_workbook.clear()
    yield paths
    saved.read_snapshot_workbook.clear()


def write_snapshots(paths, *, empty=False, updated=None):
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    start, end = monday - timedelta(days=7), monday - timedelta(days=1)
    now = (updated or datetime.now()).isoformat()
    rows = []
    with pd.ExcelWriter(paths['ijp_alerts']) as writer:
        for region, code in [('AA区', 'C3DM1'), ('孔区', 'C3RA1')]:
            frame = pd.DataFrame([] if empty else [dict(region=region, productcode='M678', line='L1', printer='P1',
                rs_code=code, period_start=str(start), period_end=str(end), ratio=.8, minimum=.9)], columns=ALERT_COLUMNS)
            frame.to_excel(writer, sheet_name=region, index=False)
            rows.append(dict(region=region, reference_date=str(today), period_start=str(start), period_end=str(end),
                             updated_at=now, alert_count=len(frame), status='success'))
        pd.DataFrame(rows).to_excel(writer, sheet_name='__meta__', index=False)
    parts = pd.DataFrame([] if empty else [{'厂别': 'Array', '备件类型': 'Target', '使用进度': 110,
        '原始测量值': 'private'}]).reindex(columns=[*PARTS_PUBLIC_COLUMNS, '原始测量值'])
    with pd.ExcelWriter(paths['parts_alerts']) as writer:
        parts.to_excel(writer, sheet_name='超规备件', index=False)
        pd.DataFrame([dict(as_of_date=str(today), updated_at=now, alert_count=len(parts), status='success')]).to_excel(writer, sheet_name='__meta__', index=False)
    iqc = pd.DataFrame([] if empty else [{'产品型号': 'M678、M626', 'IQC结果': 'NG', 'COA结果': 'NG', 'flag': 'private'}]).reindex(columns=[*REPORT_COLUMNS, 'flag'])
    with pd.ExcelWriter(paths['evaporation_alerts']) as writer:
        iqc.to_excel(writer, sheet_name='超规材料', index=False)
        pd.DataFrame([dict(start_date=str(start), end_date=str(end), products='M678、M626', updated_at=now,
            alert_count=len(iqc), status='success')]).to_excel(writer, sheet_name='__meta__', index=False)


def test_product_matching_global_parts_and_public_projection(snapshots):
    write_snapshots(snapshots)
    for key in ('ijp_aa_oos', 'ijp_hole_oos'):
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'alert'
        assert saved.read_published_alerts(key, 'M626', date.today())['state'] == 'ok'
    for product in ('M678', 'M626'):
        parts = saved.read_published_alerts('parts_oos', product, date.today())
        assert parts['state'] == 'alert' and parts['alert_factories'] == ['ARRAY']
        assert '原始测量值' not in parts['alerts']
        iqc = saved.read_published_alerts('iqc_evaporation_ng', product, date.today())
        assert iqc['state'] == 'alert' and len(iqc['alerts']) == 1
        assert 'flag' not in iqc['alerts']
    assert saved.read_published_alerts('iqc_evaporation_ng', 'M67', date.today())['state'] == 'error'


def test_missing_stale_and_valid_empty_are_distinct(snapshots):
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'error'
    write_snapshots(snapshots, empty=True)
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'ok'
    write_snapshots(snapshots, updated=datetime.now() - timedelta(days=2))
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'error'


def test_file_version_invalidates_parse_cache_and_signature(snapshots, monkeypatch):
    write_snapshots(snapshots)
    calls = []
    original = saved.read_workbook_sheets
    def read(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(saved, 'read_workbook_sheets', read)
    before = saved.published_alert_signature('ijp_aa_oos', 'M678', date.today())
    for key in saved.PUBLISHED_ALERT_ROWS:
        for product in ('M678', 'M626'):
            saved.read_published_alerts(key, product, date.today())
    assert len(calls) == 3
    saved.clear_published_snapshot('ijp_aa_oos')
    saved.read_published_alerts('ijp_hole_oos', 'M678', date.today())
    saved.read_published_alerts('parts_oos', 'M678', date.today())
    saved.read_published_alerts('iqc_evaporation_ng', 'M678', date.today())
    assert len(calls) == 4  # Only the shared IJP workbook was reparsed.
    write_snapshots(snapshots, empty=True)
    after = saved.published_alert_signature('ijp_aa_oos', 'M678', date.today())
    assert before != after
    assert saved.read_published_alerts('ijp_aa_oos', 'M678', date.today())['state'] == 'ok'


def test_new_matrix_rows_and_details_never_run_report_applications(snapshots, monkeypatch):
    from app.sections.inline_domain.monitor import alert_matrix_cache as cache, alert_matrix_service as matrix
    from src.indicator_domain.application.ijp.service import IjpReportService
    from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
    from src.equipment_domain.application.parts_service import PartsReportService
    from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
    write_snapshots(snapshots)
    def forbidden(*args, **kwargs):
        pytest.fail('snapshot consumer ran application or SQL')
    for owner, name in ((IjpReportService, 'get_alert_report'), (IjpHoleReportService, 'get_alert_report'),
                        (PartsReportService, 'get_report_data'), (EvaporationReportService, 'get_report')):
        monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(cache, 'DatabaseManager', forbidden)
    monkeypatch.setattr(pd, 'read_sql', forbidden)
    context = cache.build_default_matrix_context(('M678', 'M626'), reference_date=date.today())
    for key in saved.PUBLISHED_ALERT_ROWS:
        for product in ('M678', 'M626'):
            cell = matrix.MATRIX_ROW_MAP[key].evaluator(product, context)
            detail = saved.load_published_detail(key, product, date.today())
            assert cell['state'] == detail['state']
    before = {path: path.read_bytes() for path in snapshots.values()}
    for key in saved.PUBLISHED_ALERT_ROWS:
        cache.build_cell_source_signature(key, 'M678', date.today())
    assert before == {path: path.read_bytes() for path in snapshots.values()}


def test_matrix_filters_and_real_detail_clicks_are_file_only(snapshots, monkeypatch):
    from streamlit.testing.v1 import AppTest
    from app.sections.inline_domain.monitor import alert_matrix_cache as cache, alert_matrix_service as matrix, alert_matrix_detail as detail
    write_snapshots(snapshots)
    monkeypatch.setattr(matrix, 'MATRIX_ROWS', tuple(matrix.MATRIX_ROW_MAP[key] for key in saved.PUBLISHED_ALERT_ROWS))
    def forbidden(*args, **kwargs):
        pytest.fail('snapshot detail tried to assemble report services')
    monkeypatch.setattr(cache, 'DatabaseManager', forbidden)
    monkeypatch.setattr(detail, 'build_default_detail_loaders', forbidden)
    cache._cached_alert_matrix_cell.clear()
    detail._cached_matrix_detail_bundle.clear()
    app = AppTest.from_string('''
from datetime import date
from app.sections.inline_domain.monitor.alert_matrix_cache import get_cached_alert_matrix
from app.sections.inline_domain.monitor.alert_matrix import render_alert_matrix_section
from app.sections.inline_domain.monitor.alert_matrix_detail import render_alert_matrix_detail
payload = get_cached_alert_matrix(reference_date=date.today(), products=('M678','M626'), _revision_provider=lambda *a: 'test')
render_alert_matrix_section(payload)
render_alert_matrix_detail(payload)
''').run()
    assert not app.exception and not app.dataframe
    assert app.button(key='matrix_cell_ijp_aa_oos_M678').label == '🔴'
    assert app.button(key='matrix_cell_ijp_aa_oos_M626').label == '🟢'
    for key in saved.PUBLISHED_ALERT_ROWS:
        app.button(key=f'matrix_cell_{key}_M678').click().run()
        assert not app.exception and len(app.dataframe) == 1
        assert not {'原始测量值', 'flag', 'batch_no'} & set(app.dataframe[0].value)
    app.selectbox(key='alert_matrix_data_type').select('关键备件').run()
    assert app.button(key='matrix_cell_parts_oos_M626').label == '🔴'
    app.multiselect(key='alert_matrix_factories').set_value(['OLED']).run()
    assert app.button(key='matrix_cell_parts_oos_M626').label == '🟢'
    app.button(key='matrix_cell_parts_oos_M626').click().run()
    assert not app.exception and not app.dataframe
    cache._cached_alert_matrix_cell.clear()
    detail._cached_matrix_detail_bundle.clear()


def test_expiration_invalidates_signatures_without_rewriting_files(snapshots, monkeypatch):
    write_snapshots(snapshots)
    original = snapshots['parts_alerts'].read_bytes()
    before = saved.published_alert_signature('parts_oos', 'M678', date.today())
    monkeypatch.setattr(ConfigLoader, 'get_cache_ttl_seconds', lambda: 0)
    after = saved.published_alert_signature('parts_oos', 'M678', date.today())
    assert before != after
    assert saved.read_published_alerts('parts_oos', 'M678', date.today())['state'] == 'error'
    assert snapshots['parts_alerts'].read_bytes() == original


@pytest.mark.parametrize('broken', ['status', 'period', 'count', 'metadata'])
def test_invalid_publication_is_unavailable(snapshots, broken):
    write_snapshots(snapshots)
    path = snapshots['ijp_alerts']
    sheets = pd.read_excel(path, sheet_name=None)
    if broken == 'status':
        sheets['__meta__']['status'] = 'failed'
    elif broken == 'period':
        sheets['__meta__']['period_start'] = '2000-01-01'
    elif broken == 'count':
        sheets['__meta__']['alert_count'] = 999
    else:
        del sheets['__meta__']
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    result = saved.read_published_alerts('ijp_aa_oos', 'M678', date.today())
    assert result['state'] == 'error' and result['alerts'].empty
    assert result['message'] == saved.UNAVAILABLE
