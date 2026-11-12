from datetime import date
import json

import pytest

from api.inspection.application import InspectionJob, required_keys, run_inspection


DAY = date(2026, 11, 12)


def jobs(products, failure=None):
    values=[]
    for module, product in sorted(required_keys(products)):
        def produce(module=module, product=product):
            if module == failure:
                raise OSError('secret driver connection string')
            return [{'module': module, **({'product': product} if product else {}), 'status': 'success'}]
        values.append(InspectionJob(module+product, ((module,product),), produce))
    return values


def test_all_modules_products_atomic_receipt(tmp_path):
    report=run_inspection(('P1','P2'),DAY,jobs(('P1','P2')),tmp_path)
    assert report['status']=='success' and len(report['outcomes'])==15
    assert json.loads((tmp_path/'latest.json').read_text())==report
    assert (tmp_path/(report['run_id']+'.json')).exists()
    assert report['coverage_start']=='2026-11-02' and report['runner_created']>0


def test_failure_isolated_and_diagnostics_safe(tmp_path):
    report=run_inspection(('P1',),DAY,jobs(('P1',),failure='spc'),tmp_path)
    assert report['status']=='partial' and len(report['outcomes'])==10
    assert 'secret' not in json.dumps(report)
    assert sum(row['status']=='failed' for row in report['outcomes'])==1


def test_successful_call_without_expected_output_fails(tmp_path):
    plan=jobs(('P1',))
    first=plan[0]
    plan[0]=InspectionJob(first.name,first.expected,lambda: [])
    report=run_inspection(('P1',),DAY,plan,tmp_path)
    assert report['status']=='partial'


def test_partial_scope_rejected_before_running(tmp_path):
    with pytest.raises(ValueError):
        run_inspection(('P1',),DAY,jobs(('P1',))[:-1],tmp_path)
    assert not list(tmp_path.iterdir())


def test_interruption_records_failure_and_releases_lock(tmp_path):
    plan=jobs(('P1',))
    first=plan[0]
    def interrupt(): raise KeyboardInterrupt()
    plan[0]=InspectionJob(first.name,first.expected,interrupt)
    with pytest.raises(KeyboardInterrupt):run_inspection(('P1',),DAY,plan,tmp_path)
    assert json.loads((tmp_path/'latest.json').read_text())['status']=='failed'
    assert run_inspection(('P1',),DAY,jobs(('P1',)),tmp_path)['status']=='success'


def test_failure_diagnostics_only_keep_code_location_not_external_messages(tmp_path):
    plan=jobs(('P1',))
    first=plan[0]
    module,product=first.expected[0]
    plan[0]=InspectionJob(first.name,first.expected,lambda:[{
        'module':module,'product':product,'status':'failed','message':'secret-driver-message',
        'diagnostics':[{'type':'WorkbookReadError','file':'reader.py','line':123,'message':'private'},
                       {'type':'Bad','file':'../private.env','line':1}]}])
    report=run_inspection(('P1',),DAY,plan,tmp_path)
    assert report['outcomes'][0]['diagnostics']==[{'type':'WorkbookReadError','file':'reader.py','line':123}]
    assert all(word not in json.dumps(report) for word in ('secret','private','../'))
