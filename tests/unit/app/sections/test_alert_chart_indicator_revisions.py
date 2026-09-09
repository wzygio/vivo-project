from functools import partial
from contextlib import nullcontext
from types import SimpleNamespace

import pandas as pd

from app.components import indicator_cache
from app.sections.yield_domain import yield_dashboard
from app.sections.inline_domain.spc import spc_dashboard
from app.manager import render_gate


def test_yield_chart_memo_tracks_its_three_inputs_but_not_other_products(tmp_path, monkeypatch):
    monkeypatch.setattr(yield_dashboard, "build_indicator_product_cache_signature",
                        partial(indicator_cache.build_indicator_product_cache_signature, revision_dir=tmp_path))
    def signature():
        return yield_dashboard._alert_charts_signature([("group", "code")], {"code": {"upper": 0.01}}, "M626")
    previous = signature()
    for key in ("yield_lot_oos", "yield_trend_fluctuation", "yield_sheet_oos"):
        indicator_cache.bump_indicator_product_revision(key, "M626", revision_dir=tmp_path)
        assert signature() != previous
        previous = signature()
    indicator_cache.bump_indicator_product_revision("yield_lot_oos", "M673", revision_dir=tmp_path)
    indicator_cache.bump_indicator_product_revision("spc_cpk_trend", "M626", revision_dir=tmp_path)
    assert signature() == previous


def test_spc_chart_memo_tracks_selected_indicator_product(tmp_path, monkeypatch):
    monkeypatch.setattr(spc_dashboard, "build_indicator_product_cache_signature",
                        partial(indicator_cache.build_indicator_product_cache_signature, revision_dir=tmp_path))
    alerts = pd.DataFrame({"param_name": ["same alert"]})
    features = pd.DataFrame({"prod_code": ["M626"]})
    def signature(key):
        return spc_dashboard._alert_charts_signature(alerts, features, indicator_key=key)
    cpk_before = signature("spc_cpk_trend")
    oos_before = signature("spc_sheet_oos")
    indicator_cache.bump_indicator_product_revision("spc_cpk_trend", "M626", revision_dir=tmp_path)
    assert signature("spc_cpk_trend") != cpk_before
    assert signature("spc_sheet_oos") == oos_before
    indicator_cache.bump_indicator_product_revision("spc_sheet_oos", "M673", revision_dir=tmp_path)
    assert signature("spc_sheet_oos") == oos_before


def test_same_yield_alert_codes_rebuild_plot_payload_after_scoped_refresh(tmp_path, monkeypatch):
    monkeypatch.setattr(yield_dashboard, "build_indicator_product_cache_signature",
                        partial(indicator_cache.build_indicator_product_cache_signature, revision_dir=tmp_path))
    monkeypatch.setattr(render_gate, "st", SimpleNamespace(session_state={}, spinner=lambda *_args: nullcontext()))
    builds = []
    def render():
        signature = yield_dashboard._alert_charts_signature([("g", "c")], {}, "M626")
        gate = render_gate.RenderGate()
        gate.stage(lambda: builds.append("built") or len(builds))
        return gate.collect_memoized("alert-charts", signature)
    assert render() == [1]
    assert render() == [1]
    indicator_cache.bump_indicator_product_revision("yield_lot_oos", "M626", revision_dir=tmp_path)
    assert render() == [2]
    assert render() == [2]
