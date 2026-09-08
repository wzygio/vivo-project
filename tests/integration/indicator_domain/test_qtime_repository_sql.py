from datetime import date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.infrastructure.qtime.repository import QTimeRepository
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.mark.parametrize("microseconds", ["", "519922"])
@pytest.mark.parametrize("use_snapshot", [False, True])
def test_qtime_repository_executes_the_full_filter_contract(
    tmp_path, monkeypatch, microseconds: str, use_snapshot: bool,
) -> None:
    monkeypatch.setattr(
        ConfigLoader, "get_data_forward_policy", lambda: DataForwardPolicy(enabled=False)
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        connection.execute(
            text("CREATE TABLE eda.imp_qtime_tzbjx (productspecname TEXT, f_step_id TEXT, t_step_id TEXT, q_spec TEXT)")
        )
        connection.execute(
            text(
                "CREATE TABLE mdw.qtime_tzbjx ("
                "step_desc TEXT, lot_id TEXT, prod_qty NUMERIC, sub_prod_type TEXT, "
                "f_step TEXT, t_step TEXT, q_spec NUMERIC, wait_time NUMERIC, "
                "timekey TEXT, prodcode TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.imp_qtime_tzbjx VALUES ('M626','15500','15600','370'), ('M678','15500','15600','200')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO mdw.qtime_tzbjx VALUES "
                "('A->B','L2',1,'P','15500','15600',2.5,1.2,'20260802020000','M626'),"
                "('A->B','L1',1,'P','15500','15600',2.5,0.4,'20260802010000','M626'),"
                "('A->B','L3',1,'P','25500','25600',3.0,0.8,'20260802030000','M626'),"
                "('A->B','L4',1,'P','15500','15600',2.5,0.9,'20260901000000','M626'),"
                "('A->B','L5',1,'P','15500','15600',2.5,1.0,'20260802040000','M678')"
                ",('B->C','L6',1,'P','15500','15700',3.0,0.7,'20260802050000','M626')"
            )
        )

        if microseconds:
            connection.execute(
                text("UPDATE mdw.qtime_tzbjx SET timekey = timekey || :suffix"),
                {"suffix": microseconds},
            )

    repository = QTimeRepository(
        SimpleNamespace(engine=engine),
        snapshot_dir=tmp_path if use_snapshot else None,
    )
    query = QTimeQuery(
        start_time=datetime(2026, 8, 2),
        end_time=datetime(2026, 9, 1),
        shop="ARRAY",
        step_descriptions=("A->B", "B->C"),
        products=("M626",),
    )

    assert repository.list_products() == ("M626", "M678")
    assert repository.list_step_options("ARRAY") == (
        QTimeStepOption(step_desc="A->B", f_step="15500", t_step="15600"),
        QTimeStepOption(step_desc="B->C", f_step="15500", t_step="15700"),
    )
    assert repository.fetch_details(query)["lot_id"].tolist() == ["L1", "L2", "L6"]
    assert repository.fetch_details(query)["q_spec"].tolist() == [370, 370, 3]
    if use_snapshot:
        with engine.begin() as connection:
            connection.execute(text("UPDATE eda.imp_qtime_tzbjx SET q_spec='371' WHERE productspecname='M626'"))
        refreshed = repository.refresh_snapshot(
            "ARRAY", as_of=date(2026, 8, 31), full_refresh=True,
        )
        assert refreshed.refreshed_from_database
        assert repository.fetch_details(query)["q_spec"].tolist() == [371, 371, 3]
