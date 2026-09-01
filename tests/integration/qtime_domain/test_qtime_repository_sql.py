from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from src.qtime_domain.application.dtos import QTimeQuery
from src.qtime_domain.infrastructure.qtime_repository import QTimeRepository


def test_qtime_repository_executes_the_full_filter_contract() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        connection.execute(
            text("CREATE TABLE eda.imp_qtime_tzbjx (productspecname TEXT)")
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
                "INSERT INTO eda.imp_qtime_tzbjx VALUES ('M626'), ('M678')"
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

    repository = QTimeRepository(SimpleNamespace(engine=engine))
    query = QTimeQuery(
        start_time=datetime(2026, 8, 2),
        end_time=datetime(2026, 9, 1),
        shop="ARRAY",
        step_descriptions=("A->B", "B->C"),
        products=("M626",),
    )

    assert repository.list_products() == ("M626", "M678")
    assert repository.list_step_descriptions("ARRAY") == ("A->B", "B->C")
    assert repository.fetch_details(query)["lot_id"].tolist() == ["L1", "L2", "L6"]
