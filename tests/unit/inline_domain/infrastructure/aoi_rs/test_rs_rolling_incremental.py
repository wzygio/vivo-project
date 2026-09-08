import pandas as pd
import pytest

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.snapshot_repository import AoiRsSnapshotRepository
from src.inline_domain.infrastructure.aoi_rs.data_loader import RS_DETAIL_COLUMNS, PASS_THROUGH_COLUMNS
from src.shared_kernel.data_forward import DataForwardPolicy


def _frame(date, quantity=3):
    return pd.DataFrame([dict(factory="ARRAY", prod_code="M1", start_time=pd.Timestamp(date), sheet_id="S1", lot_id="L1", step_id="11629", rs_code="A", code_qty=quantity)])


@pytest.mark.parametrize("kind,columns", [("details", RS_DETAIL_COLUMNS), ("pass_through", PASS_THROUGH_COLUMNS)])
def test_rs_incremental_tail_replacement_and_month_pruning(tmp_path, monkeypatch, kind, columns):
    monkeypatch.setattr("src.shared_kernel.config.ConfigLoader.get_data_forward_policy", lambda: DataForwardPolicy(enabled=False))
    calls = []
    batches = [pd.concat([_frame("2026-05-01"), _frame("2026-06-10"), _frame("2026-08-30")]), pd.concat([_frame("2026-09-01", 4), _frame("2026-09-01", 5), _frame("2026-09-01", 5)])]
    def loader(_db, query):
        calls.append(query.start_date)
        return batches.pop(0).reindex(columns=columns)
    repo = AoiRsSnapshotRepository(tmp_path, object(), **{kind + "_loader": loader})
    method = repo.get_rs_details if kind == "details" else repo.get_pass_through
    query = AoiRsQueryConfig(prod_code="M1", start_date="2026-06-01", end_date="2026-08-31")
    method(query)
    result = method(query.model_copy(update={"end_date": "2026-09-01"}))
    assert calls == ["2026-05-01", "2026-08-29"]
    assert pd.Timestamp("2026-05-01") not in result.start_time.tolist()
    assert pd.Timestamp("2026-08-30") not in result.start_time.tolist()
    assert len(result) == (3 if kind == "details" else 2)
    if kind == "details":
        assert result.code_qty.tolist() == [3, 4, 5]


def test_empty_pair_refresh_is_success_and_cached(tmp_path):
    calls = []
    def loader(*args):
        calls.append(1)
        return pd.DataFrame()
    repo = AoiRsSnapshotRepository(tmp_path, object(), details_loader=loader, pass_through_loader=loader)
    query = AoiRsQueryConfig(prod_code="M1", start_date="2026-09-01", end_date="2026-09-08")
    assert repo.refresh(query)
    assert repo.get_rs_details(query).empty
    assert repo.get_pass_through(query).empty
    assert len(calls) == 2


def test_second_loader_failure_keeps_first_snapshot_and_metadata_untouched(tmp_path):
    failed = False
    def passed(*args):
        if failed:
            raise RuntimeError("pass source unavailable")
        return _frame("2026-09-01").reindex(columns=PASS_THROUGH_COLUMNS)
    repo = AoiRsSnapshotRepository(tmp_path, object(), details_loader=lambda *args: _frame("2026-09-01"), pass_through_loader=passed)
    query = AoiRsQueryConfig(prod_code="M1", start_date="2026-09-01", end_date="2026-09-08")
    assert repo.refresh(query)
    originals = {p: p.read_bytes() for p in tmp_path.iterdir()}
    failed = True
    assert repo.refresh(query) is False
    assert {p: p.read_bytes() for p in tmp_path.iterdir()} == originals
