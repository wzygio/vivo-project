from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from api.inspection.producers_auxiliary import _produce_lifetime, _verify_saved
from app.sections.shared.alert_results import read_alert_result
from src.iqc_domain import composition
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from src.iqc_domain.core.lifetime.alerts import LIFETIME_ALERT_COLUMNS, LifetimeLedgerInvalid
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError
from src.iqc_domain.infrastructure.lifetime.workbooks import LifetimeAlertWorkbook
from src.shared_kernel.config import ConfigLoader
from tests.unit.test_iqc_lifetime import MemorySource, source_frame


@pytest.fixture
def ledger_app(tmp_path, monkeypatch):
    path = tmp_path / 'custom.xlsx'
    store = LifetimeAlertWorkbook(path)
    products = ('M678',)
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda *args: path)
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda **kwargs: list(products))
    app = AppTest.from_string("""
from app.sections.iqc_domain.lifetime.lifetime_dashboard import render_lifetime_dashboard
class Offline:
    def get_report(self):
        raise RuntimeError()
render_lifetime_dashboard(Offline())
""", default_timeout=30)
    return store, products, app.run()


def workbook_bytes(rows, downloaded=None):
    payload = BytesIO()
    with pd.ExcelWriter(payload) as writer:
        pd.DataFrame({'alert_count': [999]}).to_excel(writer, sheet_name='__meta__', index=False)
        rows.to_excel(writer, sheet_name='寿命预警', index=False)
        if downloaded is not None:
            pd.read_excel(BytesIO(downloaded), sheet_name='决策版本').to_excel(
                writer, sheet_name='决策版本', index=False)
    return payload.getvalue()


def upload(app, payload):
    app.file_uploader[0].set_value(('edit.xlsx', payload, 'application/octet-stream')).run()
    app.button(key='lifetime_ledger_apply').click().run()
    assert not app.exception


def download_rows(store):
    payload, _ = store.download()
    return pd.read_excel(BytesIO(payload), sheet_name='寿命预警')


def public_result():
    result = read_alert_result('iqc_lifetime_oos', 'M678', date.today())
    assert list(result['alerts'].columns) == list(LIFETIME_ALERT_COLUMNS)
    for frame in (result['alerts'], result['records']):
        assert not {'Flag', '本期状态'}.intersection(frame.columns)
    return result


def test_legacy_upload_preserves_omitted_rows_and_rejects_stale_edits(ledger_app):
    store, products, app = ledger_app
    path = store.path
    assert not app.exception and not app.file_uploader and not app.get('download_button')
    app.query_params['admin'] = 'true'
    app.run()
    assert app.get('download_button')[0].label == '下载预警台账'
    template, version = store.download()
    rows = pd.DataFrame(columns=LIFETIME_ALERT_COLUMNS)
    rows.loc[0] = ['M678', 'B01', 'W', 1, '效率衰减', 660, 0.949978, '预测', 0.95]
    with pytest.raises(LifetimeLedgerInvalid):
        store.replace_uploaded(workbook_bytes(rows), products=products, expected_digest=version)
    assert not path.exists()
    store.save(rows, reference_date=date.today(), products=products)
    app.run()
    assert not app.exception and app.dataframe[0].value['规格点数值'].tolist() == [0.949978]
    assert public_result()['state'] == 'alert'
    assert download_rows(store)['Flag'].tolist() == [False]
    _, current = store.download()
    published = path.read_bytes()
    with pytest.raises(SnapshotChangedError):
        store.replace_uploaded(template, products=products, expected_digest=version)
    wrong_sheet = BytesIO()
    rows.to_excel(wrong_sheet, index=False)
    with pytest.raises(LifetimeLedgerInvalid):
        store.replace_uploaded(wrong_sheet.getvalue(), products=products, expected_digest=current)
    assert path.read_bytes() == published
    upload(app, workbook_bytes(rows.iloc[:0]))
    assert any('共 1 条记录' in message.value for message in app.success)
    app.run()
    assert app.dataframe[0].value['规格点数值'].tolist() == [0.949978]
    assert public_result()['state'] == 'alert'
    assert pd.read_excel(path, sheet_name='__meta__').iloc[0].alert_count == 1


def test_uploaded_flag_survives_inspection_and_release_restores_latest_value(ledger_app, monkeypatch):
    store, products, app = ledger_app
    raw = source_frame().iloc[[0]].assign(test_time=660, bu='W', eff_decay=0.94, lumi_decay=1.0)
    monkeypatch.setattr(composition, 'build_lifetime_service', lambda **kwargs:
                        LifetimeReportService(MemorySource(raw), **kwargs))
    monkeypatch.setattr(composition, 'load_lifetime_specs', lambda: {('M678', 'W'): 660})

    assert _produce_lifetime(products, date.today()) == 1
    app.run()
    assert app.dataframe[0].value['规格点数值'].tolist() == [0.94]
    assert not app.file_uploader and not app.get('download_button')
    assert set(app.dataframe[0].value.columns) == set(LIFETIME_ALERT_COLUMNS)
    assert not any('Flag' in caption.value for caption in app.caption)

    app.query_params['admin'] = 'ture'
    app.run()
    rows = download_rows(store)
    assert rows['本期状态'].tolist() == ['超规']
    original, _ = store.download()
    upload(app, workbook_bytes(rows.assign(Flag=True), original))
    assert public_result()['state'] == 'ok'
    assert not app.dataframe
    _, current = store.download()
    with pytest.raises(SnapshotChangedError):
        store.replace_uploaded(original, products=products, expected_digest=current)

    raw['eff_decay'] = 0.932
    refresh_started = datetime.now()
    assert _produce_lifetime(products, date.today()) == 0
    _verify_saved('iqc_lifetime', products, date.today(), 0, refresh_started)
    rows = download_rows(store)
    assert rows['Flag'].tolist() == [True]
    assert rows['规格点数值'].tolist() == [0.932]
    assert rows['本期状态'].tolist() == ['超规']
    assert public_result()['state'] == 'ok'

    app.run()
    upload(app, workbook_bytes(rows.assign(Flag='Delete'), store.download()[0]))
    assert download_rows(store)['Flag'].tolist() == ['Delete']
    assert public_result()['state'] == 'ok'
    upload(app, workbook_bytes(rows.assign(Flag=False, 规格点数值=0.94), store.download()[0]))
    assert public_result()['alerts']['规格点数值'].tolist() == [0.932]
    del app.query_params['admin']
    app.run()
    assert not app.exception and not app.file_uploader and not app.get('download_button')
    assert not any('Flag' in caption.value for caption in app.caption)
    assert list(app.dataframe[0].value.columns) == list(LIFETIME_ALERT_COLUMNS)
    assert app.dataframe[0].value['规格点数值'].tolist() == [0.932]

    with pd.ExcelWriter(store.path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        rows.assign(Flag='Delete').to_excel(writer, sheet_name='寿命预警', index=False)
    assert public_result()['state'] == 'ok'
    assert _produce_lifetime(products, date.today()) == 0
    assert download_rows(store)['Flag'].tolist() == ['Delete']


def test_upload_preserves_inspection_freshness_and_scope(ledger_app):
    store, products, _ = ledger_app
    yesterday = date.today() - timedelta(days=1)
    rows = pd.DataFrame([['M678', 'B1', 'W', 1, '效率衰减', 660, .92, '预测', .95]],
                        columns=LIFETIME_ALERT_COLUMNS)
    store.save(rows, reference_date=yesterday, products=products)
    before = pd.read_excel(store.path, sheet_name='__meta__').iloc[0]
    payload, version = store.download()
    store.replace_uploaded(workbook_bytes(download_rows(store).assign(Flag='Delete'), payload),
                           products=('M626',), expected_digest=version)
    after = pd.read_excel(store.path, sheet_name='__meta__').iloc[0]
    for key in ('reference_date', 'updated_at', 'products', 'status'):
        assert after[key] == before[key]
    assert after.alert_count == 0
    assert read_alert_result('iqc_lifetime_oos', 'M678', date.today())['state'] == 'error'


@pytest.mark.parametrize('legacy', [False, True])
def test_sample_numbers_survive_earlier_new_sample_and_absence(ledger_app, monkeypatch, legacy):
    store, products, _ = ledger_app
    raw = source_frame().iloc[[0]].assign(panel_id='private-B', test_time=660, eff_decay=.93, lumi_decay=1.)
    source = MemorySource(raw)
    service = LifetimeReportService(source, sample_numberer=store.sample_numbers)
    monkeypatch.setattr(composition, 'build_lifetime_service', lambda **kwargs: service)
    monkeypatch.setattr(composition, 'load_lifetime_specs', lambda: {('M678', 'W'): 660})

    def flag(number):
        original, version = store.download()
        rows = download_rows(store)
        edited = rows.loc[rows['样品编号'].eq(number)].assign(Flag='Delete')
        store.replace_uploaded(workbook_bytes(edited, original), products=products, expected_digest=version)

    assert service.get_report()['样品编号'].tolist() == [1] and not store.path.exists()
    if legacy:
        rows = pd.DataFrame([['M678', '2026/3/10', 'W', 1, '效率衰减', 660, .93, '实测', .95]],
                            columns=LIFETIME_ALERT_COLUMNS)
        store.save(rows, reference_date=date.today(), products=products)
        flag(1)
    original_number = 2 if legacy else 1
    assert _produce_lifetime(products, date.today()) == 1
    if legacy:
        retired = download_rows(store).set_index('样品编号').loc[1]
        assert retired['本期状态'] == '未判定' and retired['Flag'] == 'Delete'
    assert public_result()['alerts']['样品编号'].tolist() == [original_number]
    flag(original_number)
    earlier = raw.assign(panel_id='private-A', eff_decay=.90)
    source.frame = pd.concat([earlier, raw], ignore_index=True)
    before = store.path.read_bytes()
    version = store.numbering_version()
    preview = service.get_report().set_index('样品编号')['效率衰减'].to_dict()
    assert preview == {original_number: .93, original_number + 1: .90}
    assert before == store.path.read_bytes()
    assert _produce_lifetime(products, date.today()) == 1
    assert store.numbering_version() != version
    assert public_result()['alerts']['样品编号'].tolist() == [original_number + 1]
    source.frame = earlier
    assert _produce_lifetime(products, date.today()) == 1
    source.frame = pd.concat([raw, earlier], ignore_index=True)
    assert _produce_lifetime(products, date.today()) == 1
    assert public_result()['alerts']['规格点数值'].tolist() == [.90]
    assert download_rows(store).set_index('样品编号').loc[original_number, 'Flag'] == 'Delete'
    assert set(pd.ExcelFile(BytesIO(store.download()[0])).sheet_names) == {'寿命预警', '决策版本'}
    sheets = pd.read_excel(store.path, sheet_name=None)
    assert all('private-' not in frame.to_json() for frame in sheets.values())
