"""Production composition applies the approved 200-hour path specification."""

from datetime import date

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
    else:
        assert 170. <= value.details.set_index("lot_id").loc["ABOVE", "wait_time"] <= 190.
        assert value.alerts.wait_time.tolist() == ([250.] if action is False else [])
    figure = build_qtime_figure(value.details)
    assert figure.layout.shapes[0].y0 == 200.
