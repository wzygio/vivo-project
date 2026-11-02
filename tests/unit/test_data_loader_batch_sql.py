import pytest
from sqlalchemy import create_engine, text

from src.yield_domain.infrastructure import data_loader


class DummyDbManager:
    def __init__(self, engine) -> None:
        self.engine = engine


@pytest.fixture
def database():
    """Exercise the loader SQL against isolated tables, without production I/O."""
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mdw")
            connection.exec_driver_sql(
                "CREATE TABLE dwt_warehousing_pnl (panel_id TEXT, prod_id TEXT, "
                "first_ship_date TEXT, first_ship_time TEXT, last_flag TEXT, sub_prod_type TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE dwr_mes_productspec (productspecname TEXT, productcode TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE dws_dft_warehousing_d (panel_id TEXT, defect_code TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE imp_ct_dft_group (defect_code TEXT, defect_desc TEXT, defect_group TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE mdw.spot_glass_batch_info "
                "(productcode TEXT, lot TEXT, glass_id TEXT, timekey TEXT)"
            )
            connection.exec_driver_sql(
                "INSERT INTO dwr_mes_productspec VALUES ('SPEC-M626', 'M626')"
            )
            connection.exec_driver_sql(
                "INSERT INTO dwt_warehousing_pnl VALUES "
                "('L3MY690340101', 'SPEC-M626', '20261101', '20261101100000', 'Y', 'P'), "
                "('L3MY690340102', 'SPEC-M626', '20261101', '20261101110000', 'Y', 'P')"
            )
        yield DummyDbManager(engine)
    finally:
        engine.dispose()


def insert_batches(database, rows):
    with database.engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO mdw.spot_glass_batch_info "
            "(productcode, lot, glass_id, timekey) "
            "VALUES (:productcode, :lot, :glass_id, :timekey)"
        ), rows)


def load(database, **kwargs):
    arguments = dict(
        db_manager=database,
        start_date="2026-11-01",
        end_date="2026-11-02",
        prod_code="M626",
        work_order_types=["ESLC", "P"],
    )
    arguments.update(kwargs)
    return data_loader.load_panel_details(**arguments)


def batch(product="M626", lot="26/11/01蒸镀批", timekey="20261101", glass="L3MY6903401"):
    return dict(productcode=product, lot=lot, glass_id=glass, timekey=timekey)


def test_batch_matches_productcode_and_sheet_prefix_without_multiplying_panels(database):
    insert_batches(database, [
        batch(lot="26/11/01蒸镀批", glass="L3MY6903401-A"),
        batch(lot="26/11/02蒸镀批", timekey="20261102", glass="L3MY6903401-B"),
        batch(lot="26/11/02蒸镀批", timekey="20261102", glass="L3MY6903401-B"),
        batch(product="M678", lot="26/11/03蒸镀批", timekey="20261102"),
        batch(product="SPEC-M626", lot="26/11/04蒸镀批", timekey="20261102"),
        batch(lot="26/11/05蒸镀批", timekey="20261102", glass="OTHER-SHEET"),
    ])

    result = load(database)

    assert result["panel_id"].tolist() == ["L3MY690340101", "L3MY690340102"]
    assert result["sheet_id"].tolist() == ["L3MY6903401"] * 2
    assert result["batch_no"].tolist() == ["26/11/02"] * 2
    assert result["prod_code"].tolist() == ["M626"] * 2


@pytest.mark.parametrize("timekey,matched", [
    ("20261031", False),
    ("20261101", True),
    ("20261102", True),
    ("20261102235959", True),
    ("20261103", False),
])
def test_batch_partition_window_matches_query_dates_without_extra_lookback(database, timekey, matched):
    insert_batches(database, [batch(timekey=timekey)])

    result = load(database)

    assert len(result) == 2
    if matched:
        assert result["batch_no"].tolist() == ["26/11/01"] * 2
    else:
        assert result["batch_no"].isna().all()


def test_missing_batch_keeps_panels_and_their_defect_rows(database):
    with database.engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO dws_dft_warehousing_d VALUES "
            "('L3MY690340101', 'D1'), ('L3MY690340101', 'D2')"
        )
        connection.exec_driver_sql(
            "INSERT INTO imp_ct_dft_group VALUES ('D1', 'defect1', 'group1'), ('D2', 'defect2', 'group2')"
        )

    result = load(database)

    assert len(result) == 3
    assert result["batch_no"].isna().all()
    assert set(result["defect_code"].dropna()) == {"D1", "D2"}


def test_filter_values_are_bound_and_empty_work_orders_return_no_rows(database):
    assert load(database, work_order_types=[]).empty
    assert load(database, prod_code="M626' OR 1=1 --").empty
    assert load(database, work_order_types=["P' OR 1=1 --"]).empty
