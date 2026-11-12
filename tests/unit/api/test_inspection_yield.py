from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest


@pytest.fixture
def yield_runtime(monkeypatch, tmp_path):
    from api.inspection.yield_publication import publish_product
    from src.shared_kernel.config import ConfigLoader
    from yield_domain.application.yield_service import YieldAnalysisService
    from src.shared_kernel.infrastructure import db_handler
    from src.yield_domain import composition
    from src.yield_domain.application import alert_publication
    from app.components import indicator_cache
    from app.sections.yield_domain import group_trend_data, resource_config
    events=[]
    config=SimpleNamespace(data_source=SimpleNamespace(product_code='P1'))
    monkeypatch.setattr(ConfigLoader,'load_config',lambda product:config)
    monkeypatch.setattr(ConfigLoader,'get_domain_resource_dir',lambda domain:tmp_path)
    monkeypatch.setattr(indicator_cache,'build_indicator_product_cache_signature',lambda *args:'fixture')
    monkeypatch.setattr(group_trend_data,'build_yield_source_signature',lambda *args:'fixture-source')
    monkeypatch.setattr(resource_config,'get_dashboard_resource_config',lambda *args:config)
    monkeypatch.setattr(db_handler,'DatabaseManager',lambda:object())
    monkeypatch.setattr(YieldAnalysisService,'build_cache_context',lambda *args,**kw:{
        'analysis_start_date':'2026-10-01','analysis_end_date':'2026-11-12',
        'modifier_signature':'fixture','warning_signature':'fixture'})
    monkeypatch.setattr(YieldAnalysisService,'safe_refresh_snapshots',
                        lambda *args:events.append('refresh') or True)
    monkeypatch.setattr(YieldAnalysisService,'get_data_health',lambda *args,**kw:{
        'status':'fresh','refreshed_at':datetime.now().isoformat()})
    monkeypatch.setattr(YieldAnalysisService,'load_static_warning_lines',lambda *args,**kw:{'D1':{'upper':.01}})
    def generate(**kwargs):
        events.append('publish')
        return {'errors':{},'publication_errors':{}}
    monkeypatch.setattr(alert_publication,'generate_yield_alert_publications',generate)
    monkeypatch.setattr(composition,'build_yield_alert_results',lambda:SimpleNamespace(
        read=lambda *args:SimpleNamespace(status='ready')))
    return SimpleNamespace(publish=publish_product,analysis=YieldAnalysisService,events=events)


def test_yield_inspection_refreshes_before_publication(yield_runtime):
    assert yield_runtime.publish('P1')['status']=='success'
    assert yield_runtime.events==['refresh','publish']


def test_yield_rejects_stale_source_even_if_old_publication_is_ready(yield_runtime,monkeypatch):
    monkeypatch.setattr(yield_runtime.analysis,'get_data_health',lambda *args,**kw:{
        'status':'fresh','refreshed_at':(datetime.now()-timedelta(hours=1)).isoformat()})
    with pytest.raises(ValueError,match='SOURCE_NOT_CURRENT_RUN'):yield_runtime.publish('P1')
    assert yield_runtime.events==['refresh']


def test_yield_refused_refresh_does_not_publish(yield_runtime,monkeypatch):
    monkeypatch.setattr(yield_runtime.analysis,'safe_refresh_snapshots',lambda *args:False)
    with pytest.raises(ValueError,match='SOURCE_REFRESH_FAILED'):yield_runtime.publish('P1')
    assert not yield_runtime.events


def test_yield_missing_limits_cannot_be_published_as_no_alerts(yield_runtime,monkeypatch):
    monkeypatch.setattr(yield_runtime.analysis,'load_static_warning_lines',lambda *args,**kw:{})
    with pytest.raises(ValueError,match='WARNING_LINES_MISSING'):yield_runtime.publish('P1')
    assert yield_runtime.events==['refresh']
