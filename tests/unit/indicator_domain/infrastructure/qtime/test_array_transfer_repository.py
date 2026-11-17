"""Fixed-route standard facts enter the existing repository and monitor seams."""

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.service import QTimeReportService
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime import repository as module
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository, DETAIL_COLUMNS
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


class Source:
    step_option = QTimeStepOption("1A200->21100", "1A200", "21100")

    def __init__(self):
        self.rows = pd.DataFrame([{
            "step_desc": "1A200->21100", "lot_id": "FIXTURE_LOT", "prod_qty": 20,
            "sub_prod_type": None, "f_step": "1A200", "t_step": "21100", "q_spec": None,
            "wait_time": 2.5, "timekey": "20261101100000", "shop": "ARRAY", "prodcode": "M626",
        }])
        self.failed = False

    def source_signature(self):
        return ("fixed-route-test-v1",)

    def read(self, start_time, end_time):
        if self.failed:
            raise QTimeDataAccessError("fixture source unavailable")
        return self.rows.copy()


@pytest.fixture
def isolated(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(enabled=False))
    monkeypatch.setattr(ConfigLoader, "get_indicator_data_exclusion", lambda: None)
    monkeypatch.setattr(module.pd, "read_sql", lambda *_args, **_kwargs: pd.DataFrame(columns=DETAIL_COLUMNS))


def test_fixed_source_appears_without_old_qtime_record_and_uses_original_monitor(isolated):
    repository = QTimeRepository(SimpleNamespace(engine=object()), supplemental_source=Source())
    assert repository.list_step_options("ARRAY") == (Source.step_option,)
    result = QTimeReportService(repository, constrain_display=True).get_current_monitoring(
        shop="ARRAY", step_descriptions=(Source.step_option.step_desc,), products=("M626",), as_of=date(2026, 11, 2),
    )
    assert result.details.lot_id.tolist() == ["FIXTURE_LOT"]
    assert result.details.wait_time.tolist() == [2.5]
    assert result.details.q_spec.isna().all()
    assert result.alerts.empty and result.decoration.empty


def test_snapshot_refresh_replaces_entire_fixed_route_and_failure_preserves_success(tmp_path, isolated):
    source = Source()
    repository = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path, supplemental_source=source)
    assert repository.refresh_snapshot("ARRAY", as_of=date(2026, 11, 2)).refreshed_from_database
    # A late target changes an earlier start outside the incremental overlap.
    source.rows["wait_time"] = 5.0
    assert repository.refresh_snapshot("ARRAY", as_of=date(2026, 11, 16)).refreshed_from_database
    query = QTimeQuery(start_time="2026-11-01", end_time="2026-11-03", shop="ARRAY",
                       step_descriptions=(source.step_option.step_desc,))
    assert repository.fetch_details(query).wait_time.tolist() == [5.0]
    path = tmp_path / "qtime_source_array.parquet"
    previous = path.read_bytes()
    source.failed = True
    assert not repository.refresh_snapshot("ARRAY", as_of=date(2026, 11, 16)).refreshed_from_database
    assert path.read_bytes() == previous


def test_old_fresh_snapshot_does_not_hide_new_source(tmp_path, isolated):
    old = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path)
    old.refresh_snapshot("ARRAY", as_of=date(2026, 11, 2))
    repository = QTimeRepository(SimpleNamespace(engine=object()), snapshot_dir=tmp_path, supplemental_source=Source())
    result = QTimeReportService(repository).get_current_monitoring(
        shop="ARRAY", step_descriptions=(Source.step_option.step_desc,), as_of=date(2026, 11, 2),
    )
    assert result.details.lot_id.tolist() == ["FIXTURE_LOT"]
