"""Production composition applies the approved 200-hour path specification."""

from datetime import date, datetime

import pandas as pd
import pytest

from app.charts.indicator_domain.qtime.chart import build_qtime_figure
from src.indicator_domain import composition


@pytest.mark.parametrize("action", [None, False, "Delete"])
def test_approved_specification_drives_original_decoration_and_chart(monkeypatch, action):
    class Facts:
        def fetch_details(self, query):
            return pd.DataFrame([
                dict(shop="ARRAY", prodcode="M626", f_step="1A200", t_step="21100",
                     step_desc="1A200->21100", lot_id=lot, timekey="20261101100000",
                     q_spec=None, wait_time=wait, prod_qty=20, sub_prod_type=None)
                for lot, wait in (("ABOVE", 250.), ("EQUAL", 200.), ("BELOW", 100.))
            ])

    class Decisions:
        decoration_path = None

        def load_decisions(self):
            return pd.DataFrame() if action is None else pd.DataFrame([
                dict(prodcode="M626", step_desc="1A200->21100", lot_id="ABOVE",
                     timekey="20261101100000", flag=action),
            ])

    monkeypatch.setattr(composition, "build_qtime_repository", lambda _db: Facts())
    monkeypatch.setattr(composition, "QTimeDecorationRepository", lambda _path: Decisions())
    service = composition.build_qtime_service(object())
    value = service.get_current_monitoring(
        shop="ARRAY", step_descriptions=("1A200->21100",), as_of=date(2026, 11, 2),
    )
    assert value.details.q_spec.eq(200.).all()
    assert value.decoration.lot_id.tolist() == ["ABOVE"]
    assert value.details.set_index("lot_id").loc["BELOW", "wait_time"] == 100.
    assert 170. <= value.details.set_index("lot_id").loc["EQUAL", "wait_time"] <= 190.
    if action == "Delete":
        assert "ABOVE" not in value.details.lot_id.tolist()
        assert value.alerts.empty
    elif action is False:
        assert value.details.set_index("lot_id").loc["ABOVE", "wait_time"] == 250.
        assert value.alerts.wait_time.tolist() == [250.]
    else:
        assert 170. <= value.details.set_index("lot_id").loc["ABOVE", "wait_time"] <= 190.
        assert value.alerts.empty
    figure = build_qtime_figure(value.details)
    assert figure.layout.shapes[0].y0 == 200.


def test_output_policy_change_invalidates_old_publication_without_rewriting(tmp_path, monkeypatch):
    monkeypatch.setattr(composition.ConfigLoader, "get_domain_resource_path", lambda _domain, key, *_a: tmp_path / f"{key}.xlsx")
    monkeypatch.setattr(composition, "_qtime_decision_signature", lambda: None)
    monkeypatch.setattr(pd, "read_sql", lambda *_a, **_k: pytest.fail("Reader must not query a database"))
    current_policy = composition.OUTPUT_POLICY_VERSION
    monkeypatch.setattr(composition, "OUTPUT_POLICY_VERSION", "before-station-lot-dedup")
    results = composition.build_qtime_alert_results()
    results.clock = lambda: datetime(2026, 11, 2, 10)
    results.ttl_seconds = 86400
    facts = pd.DataFrame([dict(
        shop="ARRAY", prodcode="M626", f_step="1A200", t_step="21100",
        step_desc="1A200->21100", lot_id="EARLIEST", timekey="20261029100000",
        q_spec=200., wait_time=250., over_hours=50., flag=False,
    )])
    results.publish("ARRAY", facts, facts, {
        "status": "fresh", "refreshed_at": "2026-11-02T09:30:00",
    }, date(2026, 11, 2))
    assert results.read("M626", date(2026, 11, 2), shops=("ARRAY",))["state"] == "alert"
    before = results.store.path.read_bytes()
    monkeypatch.setattr(composition, "OUTPUT_POLICY_VERSION", current_policy)
    assert results.read("M626", date(2026, 11, 2), shops=("ARRAY",))["state"] == "error"
    assert results.store.path.read_bytes() == before
