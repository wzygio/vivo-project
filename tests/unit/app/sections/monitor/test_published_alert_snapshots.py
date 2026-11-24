from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
import pytest

from app.sections.inline_domain.monitor import published_alert_snapshots as saved
from src.shared_kernel.config import ConfigLoader
from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS
from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.iqc_domain.core.lifetime.alerts import LIFETIME_ALERT_COLUMNS


@pytest.fixture
def snapshots(tmp_path, monkeypatch):
    from src.shared_kernel.utils import excel_tools

    def forbid_office(path):
        pytest.fail(f'Fixture workbook unexpectedly requested Office: {path.name}')

    monkeypatch.setattr(excel_tools, '_read_all_sheets_via_com', forbid_office)
    paths = {key: tmp_path / f'{key}.xlsx'
             for key in ('ijp_alerts', 'parts_alerts', 'evaporation_alerts', 'lifetime_alerts')}
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
    lifetime = pd.DataFrame([] if empty else [{
        '产品型号': 'M678', '批次号': '00042', '测试画面': 'W', '样品编号': 1, '指标': '亮度衰减',
        '规格时间(H)': 660, '规格点数值': .92, '取值方式': '预测', '判定阈值': .95,
    }], columns=LIFETIME_ALERT_COLUMNS)
    with pd.ExcelWriter(paths['lifetime_alerts']) as writer:
        lifetime.to_excel(writer, sheet_name='寿命预警', index=False)
        pd.DataFrame([dict(reference_date=str(today), products='M678、M626', updated_at=now,
            alert_count=len(lifetime), status='success')]).to_excel(writer, sheet_name='__meta__', index=False)


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
    assert saved.read_published_alerts('iqc_evaporation_ng', 'M67', date.today())['state'] == 'ok'
    assert saved.read_published_alerts('iqc_evaporation_ng', 'M67', date.today())['alerts'].empty
    lifetime = saved.read_published_alerts('iqc_lifetime_oos', 'M678', date.today())
    assert lifetime['state'] == 'alert' and lifetime['alerts']['规格点数值'].tolist() == [.92]
    assert lifetime['alerts']['批次号'].tolist() == ['00042']
    assert saved.read_published_alerts('iqc_lifetime_oos', 'M626', date.today())['state'] == 'ok'


def test_shared_parts_reader_accepts_factory_wide_scope_without_a_product(snapshots):
    from app.sections.shared.alert_results import read_alert_result
    write_snapshots(snapshots)
    before = {path: path.read_bytes() for path in snapshots.values()}
    result = read_alert_result('parts_oos', '', date.today())
    assert result['state'] == 'alert'
    assert result['alert_factories'] == ['ARRAY']
    assert len(result['alerts']) == 1 and '原始测量值' not in result['alerts']
    assert {path: path.read_bytes() for path in snapshots.values()} == before


def test_shared_ijp_aggregate_selects_enabled_products_and_distinguishes_empty_scope(snapshots, monkeypatch):
    from app.sections.shared.alert_results import read_alert_result
    write_snapshots(snapshots)
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda: ['M626', 'M678'])
    result = read_alert_result('ijp_aa_oos', '', date.today())
    assert result['state'] == 'alert'
    assert result['records']['productcode'].tolist() == ['M678']
    assert result['alerts']['productcode'].tolist() == ['M678']
    write_snapshots(snapshots, empty=True)
    empty = read_alert_result('ijp_aa_oos', '', date.today())
    assert empty['state'] == 'ok' and empty['alerts'].empty
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda: [])
    assert read_alert_result('ijp_aa_oos', '', date.today())['state'] == 'no_data'


def test_missing_stale_and_valid_empty_are_distinct(snapshots):
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'ok'
    write_snapshots(snapshots, empty=True)
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'ok'
    write_snapshots(snapshots, updated=datetime.now() - timedelta(days=2))
    for key in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(key, 'M678', date.today())['state'] == 'error'


def test_missing_snapshot_cache_is_invalidated_by_real_publication(snapshots):
    from app.sections.shared.alert_results import alert_result_signature, read_alert_result

    row = 'parts_oos'
    before = alert_result_signature(row, 'M678', date.today())
    empty = read_alert_result(row, 'M678', date.today())
    assert empty['state'] == 'ok' and empty['metadata']['availability'] == 'missing'
    assert not snapshots['parts_alerts'].exists()
    write_snapshots(snapshots)
    assert alert_result_signature(row, 'M678', date.today()) != before
    assert read_alert_result(row, 'M678', date.today())['state'] == 'alert'


def test_stale_parts_workbook_without_any_history_is_green_until_first_alert(snapshots):
    from src.equipment_domain.infrastructure.parts_alert_workbook import PARTS_DECISIONS as layout
    from app.sections.shared.alert_results import read_alert_result

    path = snapshots['parts_alerts']
    old = datetime.now() - timedelta(days=7)
    parts = pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS)
    ledger = pd.DataFrame(columns=[*layout.keys, 'Flag'])
    meta = pd.DataFrame([dict(as_of_date=str(old.date()), updated_at=old.isoformat(),
                              candidate_count=0, alert_count=0, status='success')])
    def write(ledger, metadata):
        with pd.ExcelWriter(path) as writer:
            for name, frame in ((layout.sheet, parts), (layout.candidates, parts),
                                (layout.decisions, ledger), ('__meta__', metadata)):
                frame.to_excel(writer, sheet_name=name, index=False)

    write(ledger, meta)
    for product in ('M626', 'Z622'):
        assert read_alert_result('parts_oos', product, date.today())['state'] == 'ok'
    for invalid in (meta.assign(status='failed'), meta.assign(candidate_count=1),
                    meta.assign(alert_count=1)):
        write(ledger, invalid)
        assert read_alert_result('parts_oos', 'Z622', date.today())['state'] == 'error'
    write(pd.DataFrame([dict.fromkeys(layout.keys, 'A')]).assign(Flag=True), meta)
    assert read_alert_result('parts_oos', 'Z622', date.today())['state'] == 'error'


def test_lifetime_inspection_warmup_persists_product_cells_and_invalidates_on_republication(
    snapshots, tmp_path, monkeypatch,
):
    from api.inspection.matrix_warmup import main
    from app.sections.inline_domain.monitor import alert_matrix_cache as cache, alert_matrix_service as matrix
    from app.sections.inline_domain.monitor.alert_matrix_snapshot import DailyMatrixSnapshot

    write_snapshots(snapshots)
    row = 'iqc_lifetime_oos'
    monkeypatch.setattr(matrix, 'MATRIX_ROWS', (matrix.MATRIX_ROW_MAP[row],))
    store = DailyMatrixSnapshot(tmp_path / 'daily.json')
    def load(**kwargs):
        return cache.get_cached_alert_matrix(**kwargs, _snapshot_store=store)

    day = date.today()
    products = ('M678', 'M626')
    assert main([], loader=load, store=store, log_dir=tmp_path / 'logs',
                reference_date=day, products=products) == 0
    cells = store.read(day, 3600)['cells']
    assert cells[row, 'M678']['state'] == 'alert' and cells[row, 'M626']['state'] == 'ok'
    path = snapshots['lifetime_alerts']
    sheets = pd.read_excel(path, sheet_name=None, dtype=object)
    current = sheets['寿命预警'].assign(本期状态='超规')
    history = current.assign(产品型号='DISABLED', 本期状态='未判定', Flag=False)
    signature = cells[row, 'M678']['cache_signature']
    for suppressed, state in [(True, 'ok'), (False, 'alert')]:
        ledger = pd.concat([current.assign(Flag=suppressed), history], ignore_index=True)
        with pd.ExcelWriter(path) as writer:
            ledger.to_excel(writer, sheet_name='寿命预警', index=False)
            sheets['__meta__'].assign(ledger_count=2, alert_count=int(not suppressed)).to_excel(
                writer, sheet_name='__meta__', index=False)
        cell = load(reference_date=day, products=products)['cells'][row, 'M678']
        detail = saved.load_published_detail(row, 'M678', day)
        assert cell['state'] == detail['state'] == state
        assert cell['cache_signature'] != signature
        assert detail['alerts']['产品型号'].tolist() == ([] if suppressed else ['M678'])
        assert tuple(detail['alerts']) == LIFETIME_ALERT_COLUMNS
        signature = cell['cache_signature']


@pytest.mark.parametrize('exception', [PermissionError, OSError, ValueError])
def test_unreadable_workbook_is_not_misclassified_as_absent(snapshots, monkeypatch, exception):
    write_snapshots(snapshots)
    before = {path: path.read_bytes() for path in snapshots.values()}

    def failed_read(*args):
        raise exception('read failed')

    with monkeypatch.context() as patch:
        patch.setattr(saved, 'read_snapshot_workbook', failed_read)
        for row in saved.PUBLISHED_ALERT_ROWS:
            result = saved.read_published_alerts(row, 'M678', date.today())
            assert result['state'] == 'error' and result['alerts'].empty
    assert {path: path.read_bytes() for path in snapshots.values()} == before


def test_file_version_invalidates_parse_cache_and_signature(snapshots, monkeypatch):
    write_snapshots(snapshots)
    calls = []
    original = saved.read_workbook_sheets
    def read(path, **options):
        calls.append(path)
        return original(path, **options)
    monkeypatch.setattr(saved, 'read_workbook_sheets', read)
    before = saved.published_alert_signature('ijp_aa_oos', 'M678', date.today())
    for key in saved.PUBLISHED_ALERT_ROWS:
        for product in ('M678', 'M626'):
            saved.read_published_alerts(key, product, date.today())
    assert len(calls) == 4
    saved.clear_published_snapshot('ijp_aa_oos')
    saved.read_published_alerts('ijp_hole_oos', 'M678', date.today())
    saved.read_published_alerts('parts_oos', 'M678', date.today())
    saved.read_published_alerts('iqc_evaporation_ng', 'M678', date.today())
    assert len(calls) == 5  # Only the shared IJP workbook was reparsed.
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
    from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
    write_snapshots(snapshots)
    def forbidden(*args, **kwargs):
        pytest.fail('snapshot consumer ran application or SQL')
    for owner, name in ((IjpReportService, 'get_alert_report'), (IjpHoleReportService, 'get_alert_report'),
                        (PartsReportService, 'get_report_data'), (EvaporationReportService, 'get_report'),
                        (LifetimeReportService, 'get_report')):
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
    # Keep the fixture XLSX bytes readable across UI reruns on managed Windows
    # hosts that asynchronously wrap temporary workbooks with Office protection.
    payloads = {path: path.read_bytes() for path in snapshots.values()}
    def read_fixture(path, *, preserve_text=False):
        return pd.read_excel(BytesIO(payloads[path]), sheet_name=None,
                             dtype=object if preserve_text else None, keep_default_na=not preserve_text)
    monkeypatch.setattr(saved, 'read_workbook_sheets', read_fixture)
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
''', default_timeout=30).run()
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


def write_empty_templates(paths):
    specs = {
        'lifetime_alerts': (
            {'寿命预警': LIFETIME_ALERT_COLUMNS},
            ['reference_date', 'products', 'updated_at', 'alert_count', 'status'],
        ),
        'ijp_alerts': (
            {'AA区': ALERT_COLUMNS, '孔区': ALERT_COLUMNS},
            ['region', 'reference_date', 'period_start', 'period_end', 'updated_at', 'alert_count', 'status'],
        ),
        'parts_alerts': (
            {'超规备件': PARTS_PUBLIC_COLUMNS},
            ['as_of_date', 'last_update', 'updated_at', 'alert_count', 'status'],
        ),
        'evaporation_alerts': (
            {'超规材料': REPORT_COLUMNS},
            ['start_date', 'end_date', 'products', 'updated_at', 'alert_count', 'status'],
        ),
    }
    for key, (sheets, metadata_columns) in specs.items():
        with pd.ExcelWriter(paths[key]) as writer:
            for name, columns in sheets.items():
                pd.DataFrame(columns=columns).to_excel(writer, sheet_name=name, index=False)
            pd.DataFrame(columns=metadata_columns).to_excel(writer, sheet_name='__meta__', index=False)


def test_header_only_templates_are_no_data_without_fabricating_success(snapshots):
    write_empty_templates(snapshots)
    for row in saved.PUBLISHED_ALERT_ROWS:
        for reference in (date.today(), date.today() + timedelta(days=8)):
            result = saved.read_published_alerts(row, 'M678', reference)
            assert result['state'] == 'no_data'
            assert result['alerts'].empty and result['message'] == '当前暂无预警记录。'
    for path in snapshots.values():
        assert all(frame.empty for frame in pd.read_excel(path, sheet_name=None).values())
    # A normal subsequent publication retains its normal freshness contract.
    write_snapshots(snapshots, empty=True)
    for row in saved.PUBLISHED_ALERT_ROWS:
        assert saved.read_published_alerts(row, 'M678', date.today())['state'] == 'ok'
        expected = 'ok' if row == 'parts_oos' else 'error'
        assert saved.read_published_alerts(row, 'M678', date.today() + timedelta(days=1))['state'] == expected


def test_empty_metadata_cannot_hide_nonempty_alert_records(snapshots):
    write_empty_templates(snapshots)
    path = snapshots['ijp_alerts']
    sheets = pd.read_excel(path, sheet_name=None)
    sheets['AA区'] = pd.DataFrame([dict(region='AA区', productcode='M678')], columns=ALERT_COLUMNS)
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    assert saved.read_published_alerts('ijp_hole_oos', 'M678', date.today())['state'] == 'error'


def test_publishing_one_ijp_region_keeps_other_initialized_region_no_data(snapshots):
    from src.indicator_domain.infrastructure.ijp.alert_workbook import IjpAlertWorkbook
    write_empty_templates(snapshots)
    store = IjpAlertWorkbook(snapshots['ijp_alerts'])
    store.save('AA区', pd.DataFrame(columns=ALERT_COLUMNS), date.today())
    assert saved.read_published_alerts('ijp_aa_oos', 'M678', date.today())['state'] == 'ok'
    assert saved.read_published_alerts('ijp_hole_oos', 'M678', date.today())['state'] == 'no_data'


def test_empty_template_cells_are_green_and_clicks_show_no_records(snapshots, monkeypatch):
    from streamlit.testing.v1 import AppTest
    from app.sections.inline_domain.monitor import alert_matrix_cache as cache, alert_matrix_service as matrix
    write_empty_templates(snapshots)
    monkeypatch.setattr(matrix, 'MATRIX_ROWS', tuple(matrix.MATRIX_ROW_MAP[key] for key in saved.PUBLISHED_ALERT_ROWS))
    def forbidden(*args, **kwargs):
        pytest.fail('empty template unexpectedly queried database')
    monkeypatch.setattr(cache, 'DatabaseManager', forbidden)
    cache._cached_alert_matrix_cell.clear()
    try:
        app = AppTest.from_string('''
from datetime import date
from app.sections.inline_domain.monitor.alert_matrix_cache import get_cached_alert_matrix
from app.sections.inline_domain.monitor.alert_matrix import render_alert_matrix_section
from app.sections.inline_domain.monitor.alert_matrix_detail import render_alert_matrix_detail
payload = get_cached_alert_matrix(reference_date=date.today(), products=('M678','M626'),
                                  _revision_provider=lambda *a: 'initialized-empty')
render_alert_matrix_section(payload)
render_alert_matrix_detail(payload)
''').run()
        for row in saved.PUBLISHED_ALERT_ROWS:
            for product in ('M678', 'M626'):
                button = app.button(key=f'matrix_cell_{row}_{product}')
                assert button.label == '🟢' and button.help == '当前暂无预警记录。'
            app.button(key=f'matrix_cell_{row}_M678').click().run()
            assert not app.exception and not app.dataframe and not app.error and not app.warning
            assert any(item.value == '当前暂无预警记录。' for item in app.caption)
    finally:
        cache._cached_alert_matrix_cell.clear()
