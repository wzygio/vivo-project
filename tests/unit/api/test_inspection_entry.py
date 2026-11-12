"""Exercise production composition/CLI contracts without opening source systems."""
from datetime import date
import json
from types import SimpleNamespace

import pytest

from api.inspection.application import required_keys, run_inspection


def test_composition_runs_each_real_boundary_once_for_complete_scope(monkeypatch, tmp_path):
    from api.inspection import composition, producers_inline, producers_auxiliary
    calls=[]
    products=('P1','P2')
    day=date(2026,11,12)
    def inline(product,reference):
        assert reference==day
        calls.append(('inline',product))
        return [{'module':m,'product':product,'status':'success'} for m in ('spc','ctq','aoi_tt','aoi_rs')]
    def one(module,product=None):
        calls.append((module,product))
        return [{'module':module,**({'product':product} if product else {}),'status':'success'}]
    def auxiliary(scope,reference):
        assert scope==products and reference==day
        calls.append(('auxiliary',scope))
        return [{'module':m,'status':'success'} for m in ('ijp_aa','ijp_hole','parts','iqc_evaporation')]
    monkeypatch.setattr(producers_inline,'produce_inline',inline)
    monkeypatch.setattr(producers_auxiliary,'produce_auxiliary',auxiliary)
    monkeypatch.setattr(composition,'produce_yield',lambda product,reference:one('yield',product))
    monkeypatch.setattr(composition,'produce_qtime',lambda scope,reference:one('qtime'))
    report=run_inspection(products,day,composition.build_jobs(products,day),tmp_path)
    assert report['status']=='success'
    assert {(r['module'],r.get('product','')) for r in report['outcomes']}==required_keys(products)
    assert calls==[('inline','P1'),('inline','P2'),('yield','P1'),('yield','P2'),
                   ('qtime',None),('auxiliary',products)]


def test_cli_check_does_not_build_producers_or_write_receipts(monkeypatch,tmp_path,capsys):
    from api.inspection import __main__ as cli, composition
    from src.shared_kernel.config import ConfigLoader
    monkeypatch.setattr(cli,'ROOT',tmp_path)
    monkeypatch.setattr(ConfigLoader,'get_enabled_products',lambda **kwargs:['P1'])
    monkeypatch.setattr(composition,'build_jobs',lambda *args:pytest.fail('check constructed producers'))
    assert cli.main(['--check'])==0
    assert json.loads(capsys.readouterr().out)['status']=='checked'
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('partial',[False,True])
@pytest.mark.parametrize('warmup_code',[0,1,2])
def test_cli_actual_registry_result_controls_exit_code(monkeypatch,tmp_path,capsys,partial,warmup_code):
    from api.inspection import __main__ as cli, composition
    from src.shared_kernel.config import ConfigLoader
    from tests.unit.api.test_inspection_runner import jobs
    monkeypatch.setattr(cli,'ROOT',tmp_path)
    monkeypatch.setattr(ConfigLoader,'get_enabled_products',lambda **kwargs:['P1'])
    monkeypatch.setattr(composition,'build_jobs',lambda products,day:jobs(products,'spc' if partial else None))
    from api.inspection import matrix_warmup
    calls=[]
    def warm(argv, **kwargs):
        receipt=json.loads((tmp_path/'output/logs/alert_inspection/latest.json').read_text())
        assert receipt['finished_at'] and len(receipt['outcomes'])==10
        calls.append(kwargs)
        print('warmup-summary')
        return warmup_code
    monkeypatch.setattr(matrix_warmup,'main',warm)
    assert cli.main([])==int(partial or warmup_code != 0)
    response=json.loads(capsys.readouterr().out)
    assert response['total']==10 and response['completed']==(9 if partial else 10)
    assert response['matrix_warmup_exit_code']==warmup_code
    assert calls==[{'reference_date':date.today(),'products':('P1',)}]
    assert json.loads((tmp_path/'output/logs/alert_inspection/latest.json').read_text())['status']==('partial' if partial else 'success')


def test_cli_rejects_empty_product_scope_without_private_diagnostics(monkeypatch,capsys):
    from api.inspection import __main__ as cli
    from src.shared_kernel.config import ConfigLoader
    monkeypatch.setattr(ConfigLoader,'get_enabled_products',lambda **kwargs:[])
    assert cli.main([])==2
    assert json.loads(capsys.readouterr().out)=={'status':'failed','code':'INSPECTION_FAILED'}


def test_database_guard_protects_current_and_next_transactions_and_is_removed(monkeypatch):
    from sqlalchemy import event
    from api.inspection.database import read_only_sessions
    events=[]
    callbacks=[]
    monkeypatch.setattr(event,'listen',lambda owner,name,callback:callbacks.append(callback))
    monkeypatch.setattr(event,'remove',lambda owner,name,callback:events.append('removed'))
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,statement):events.append(statement)
    class Connection:
        def set_session(self,**options):
            assert options=={'readonly':True}
            events.append('readonly')
        def cursor(self):return Cursor()
        def commit(self):events.append('commit-session-settings')
    Connection.__module__='psycopg2.extensions'
    with pytest.raises(RuntimeError):
        with read_only_sessions():
            callbacks[0](Connection(),None,None)
            raise RuntimeError('fixture interrupt')
    assert events==['readonly','SET statement_timeout = 120000','commit-session-settings','removed']


def test_qtime_requires_all_fresh_shops_and_verified_publication(monkeypatch):
    from api.inspection.composition import produce_qtime
    from src.indicator_domain import composition
    from src.indicator_domain.application.qtime import cached_monitoring
    from src.shared_kernel.infrastructure import db_handler
    monkeypatch.setattr(db_handler,'DatabaseManager',lambda:object())
    refreshes=(SimpleNamespace(refreshed_from_database=True),)*3
    service=SimpleNamespace(refresh_snapshots=lambda **kwargs:refreshes)
    monkeypatch.setattr(composition,'build_qtime_service',lambda manager:service)
    published=[]
    monkeypatch.setattr(cached_monitoring,'publish_refreshed_qtime_alerts',lambda *args,**kwargs:published.append(args) or True)
    reader=SimpleNamespace(read=lambda *args:{'state':'ok','release_evidence':True})
    monkeypatch.setattr(composition,'build_qtime_alert_results',lambda:reader)
    assert produce_qtime(('P1',),date(2026,11,12))==[{'module':'qtime','status':'success'}]
    reader.read=lambda *args:{'state':'ok','release_evidence':False}
    with pytest.raises(ValueError):produce_qtime(('P1',),date(2026,11,12))
    service.refresh_snapshots=lambda **kwargs:(SimpleNamespace(refreshed_from_database=False),)
    count=len(published)
    with pytest.raises(ValueError):produce_qtime(('P1',),date(2026,11,12))
    assert len(published)==count


def test_yield_binding_preserves_both_kinds_and_failure(monkeypatch):
    from api.inspection.composition import produce_yield
    from api.inspection import yield_publication
    calls=[]
    monkeypatch.setattr(yield_publication,'publish_product',lambda product,**kwargs:
                        calls.append((product,kwargs)) or {'status':'partial'})
    assert produce_yield('P1',date.today())==[{'module':'yield','product':'P1','status':'failed'}]
    assert calls==[('P1',{'kinds':('trend','lot')})]
    from datetime import timedelta
    with pytest.raises(ValueError):produce_yield('P1',date.today()-timedelta(days=1))
    assert len(calls)==1
