"""HTTP is the approved public seam; all payloads here are synthetic."""
from datetime import datetime
from hashlib import sha256
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient


def test_authorized_caller_gets_only_public_fields():
    from api.main import create_app
    from api.settings import ApiClient

    key = 'synthetic-test-credential-not-a-real-key'
    principal = ApiClient('demo', sha256(key.encode()).hexdigest(), ('P1',), ('ARRAY',), ('SPC',))
    rows = [{
        '分类': 'SPC', '发生站点': '0100', '部门': 'ARRAY', '层别': 'PI2',
        '监控参数': 'THK', '异常事件': 'OOS', '对象类型': 'sheet',
        '对象编号': '000001', '产品型号': 'P1',
        '发生时间': '2026-10-28T09:30:00+08:00', '命中边界': 'USL',
    }]

    class SavedService:
        def read(self, query, factories, reference_date):
            assert query.product == 'P1' and factories == ('ARRAY',)
            assert str(reference_date) == '2026-11-02'
            return rows

    app = create_app(service=SavedService(), clients_provider=lambda: (principal,),
                     enabled_products=lambda: ['P1'],
                     clock=lambda: datetime(2026, 11, 2, 10, tzinfo=ZoneInfo('Asia/Shanghai')))
    with TestClient(app) as client:
        response = client.get('/api/v1/sheet-anomalies?product=P1&sub_domain=SPC&event_type=OOS', headers={'X-API-Key': key})
    assert response.status_code == 200
    assert response.json()['data'] == rows
    assert response.json()['meta']['period_start'] == '2026-10-26T00:00:00+08:00'
    assert response.headers['cache-control'] == 'no-store'


def test_spc_file_to_http_shares_existing_filters_and_never_writes(tmp_path, monkeypatch):
    import pandas as pd
    from api.main import create_app
    from api.service import PublishedAnomalyService
    from api.settings import ApiClient
    from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
    from src.inline_domain.application.monitor.sheet_alert_query import SheetAlertQuery
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure.api_alert_metadata import ApiAlertMetadata

    monkeypatch.setattr(ConfigLoader, 'get_inline_data_exclusion', lambda: None)
    path = tmp_path / 'spc.xlsx'
    frame = pd.DataFrame([
        dict(factory='ARRAY', prod_code='P1', step_id='0100', param_name=parameter,
             sheet_id=sheet, sheet_start_time='2026-10-28', sheet_max=11,
             sheet_min=1, usl=10, lsl=2, oos_type='USL', flag=flag)
        for sheet, parameter, flag in [('000001','THK',False), ('S2','PPA_X',False), ('S3','CD',True)]
    ])
    frame.to_excel(path, sheet_name='P1', index=False)
    metadata = tmp_path / 'metadata.xlsx'
    pd.DataFrame([{'step_id':'0100','description':'PI2_CURE_THK'}]).to_excel(metadata, sheet_name='站点字典', index=False)
    before = path.read_bytes(), metadata.read_bytes()
    read = lambda path, *args: pd.read_excel(path, sheet_name=None, dtype=object)
    query_service = SheetAlertQuery(ExcelAlarmReader(ExcelAlarmStore({'spc':path}, workbook_reader=read)), excluded_param_name_contains=['PPA'])
    service = PublishedAnomalyService(
        source_reader=lambda query, day, factories: query_service.read('spc',query.product,day),
        hidden=lambda domain, product: False, metadata=ApiAlertMetadata(metadata,workbook_reader=read),
    )
    key = 'synthetic-file-test-key'
    principal = ApiClient('demo',sha256(key.encode()).hexdigest(),('P1',),('ARRAY',),('SPC',))
    app = create_app(service=service,clients_provider=lambda:(principal,),enabled_products=lambda:['P1'],
                     clock=lambda:datetime(2026,11,2,10,tzinfo=ZoneInfo('Asia/Shanghai')))
    with TestClient(app) as client:
        response=client.get('/api/v1/sheet-anomalies?product=P1&sub_domain=SPC&event_type=OOS',headers={'X-API-Key':key})
    assert response.status_code == 200
    assert [row['对象编号'] for row in response.json()['data']] == ['000001']
    assert response.json()['data'][0]['层别'] == 'PI2'
    assert before == (path.read_bytes(),metadata.read_bytes())


def test_cursor_pages_one_public_snapshot_and_rejects_changed_results():
    from api.main import create_app
    from api.settings import ApiClient
    key='synthetic-pagination-key'
    principal=ApiClient('pages',sha256(key.encode()).hexdigest(),('P1',),('ARRAY',),('SPC',))
    rows=[dict(分类='SPC',发生站点='100',部门='ARRAY',层别='PI2',监控参数='THK',异常事件='OOS',
               对象类型='sheet',对象编号=sheet,产品型号='P1',发生时间='2026-10-28T09:30:00+08:00')
          for sheet in ('S1','S2','S3')]
    class Source:
        def read(self,*args): return rows
    app=create_app(service=Source(),clients_provider=lambda:(principal,),enabled_products=lambda:['P1'])
    params={'product':'P1','sub_domain':'SPC','event_type':'OOS','limit':1}
    with TestClient(app) as client:
        first=client.get('/api/v1/sheet-anomalies',params=params,headers={'X-API-Key':key})
        assert first.status_code==200 and first.json()['meta']['has_more'] is True
        cursor=first.json()['meta']['next_cursor']
        second=client.get('/api/v1/sheet-anomalies',params={**params,'cursor':cursor},headers={'X-API-Key':key})
        assert second.status_code==200 and second.json()['data'][0]['对象编号']=='S2'
        rows.pop()
        stale=client.get('/api/v1/sheet-anomalies',params={**params,'cursor':cursor},headers={'X-API-Key':key})
        assert stale.status_code==410


def test_unavailable_source_is_safe_json_and_does_not_echo_technical_details():
    from api.main import create_app
    from api.settings import ApiClient
    key='synthetic-error-key'
    principal=ApiClient('errors',sha256(key.encode()).hexdigest(),('P1',),('ARRAY',),('SPC',))
    class Source:
        def read(self,*args): raise ValueError('private-workbook-path and internal flag')
    app=create_app(service=Source(),clients_provider=lambda:(principal,),enabled_products=lambda:['P1'])
    with TestClient(app,raise_server_exceptions=False) as client:
        result=client.get('/api/v1/sheet-anomalies?product=P1&sub_domain=SPC&event_type=OOS',headers={'X-API-Key':key})
    assert result.status_code==503
    assert result.json()['error']['code']=='result_unavailable'
    assert 'private-workbook-path' not in result.text and 'flag' not in result.text


def test_client_limit_and_read_timeout_are_enforced(caplog):
    import time
    from api.main import create_app
    from api.settings import ApiClient, ApiSettings
    key='synthetic-limits-key'
    principal=ApiClient('limits',sha256(key.encode()).hexdigest(),('P1',),('ARRAY',),('SPC',))
    class SlowSource:
        def read(self,*args):
            time.sleep(.08)
            return []
    settings=ApiSettings(requests_per_minute=1,read_timeout_seconds=.01,max_concurrent_reads=1)
    app=create_app(service=SlowSource(),settings=settings,clients_provider=lambda:(principal,),enabled_products=lambda:['P1'])
    with TestClient(app) as client:
        first=client.get('/api/v1/sheet-anomalies?product=P1&sub_domain=SPC&event_type=OOS',headers={'X-API-Key':key})
        assert first.status_code==503
        assert first.json()['error']['code']=='read_timeout'
        assert first.headers['x-request-id'] in caplog.text
        assert 'API_READ_TIMEOUT' in caplog.text
        second=client.get('/api/v1/sheet-anomalies?product=P1&sub_domain=SPC&event_type=OOS',headers={'X-API-Key':key})
        assert second.status_code==429 and int(second.headers['retry-after'])>0


def test_default_all_source_budget_fits_within_portable_client_wait(monkeypatch):
    from api.settings import ApiSettings
    from tools import fetch_sheet_anomalies as caller

    # The previous 30s server / 45s caller budget failed on a cold 84-source read.
    defaults = []
    monkeypatch.setattr(caller, 'launch_gui', lambda args: defaults.append(args) or 0)
    assert caller.main([]) == 0
    assert ApiSettings().read_timeout_seconds == 90
    assert defaults[0].timeout == 120
