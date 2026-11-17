from contextlib import contextmanager
import pandas as pd
import pytest

from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime.array_transfer_source import ArrayTransferQTimeSource


START = pd.Timestamp("2026-11-17")
END = pd.Timestamp("2026-11-18")


class ReadOnlyConnection:
    def __init__(self):
        self.commands = []

    @contextmanager
    def begin(self):
        yield

    def exec_driver_sql(self, command):
        self.commands.append(command)


class ReadOnlyEngine:
    def __init__(self):
        self.connection = ReadOnlyConnection()

    @contextmanager
    def connect(self):
        yield self.connection


def event(station, timestamp, *, lot="LOT1", spec="ARRAY_SPEC", version="00001", factory="ARRAY"):
    return dict(lot_id=lot, oper_code=station, event_time=pd.Timestamp(timestamp),
                event_timekey=pd.Timestamp(timestamp).strftime("%Y%m%d%H%M%S%f"),
                prod_id=spec, prod_ver=version, factory=factory, prod_qty=30)


def master(*, code="M626", spec="ARRAY_SPEC", version="00001", factory="ARRAY"):
    return dict(productspecname=spec, productspecversion=version,
                productcode=code, factoryname=factory)


def reader(monkeypatch, events, masters):
    engine = ReadOnlyEngine()
    queries = []

    def read_sql(statement, connection, params):
        assert connection is engine.connection
        assert any("READ ONLY" in command for command in connection.commands)
        query = str(statement)
        queries.append((query, params))
        if "mdw.dwr_mes_lot_hist" in query:
            assert "event_time" in query and ">=" in query and "<" in query
            return events.copy()
        assert "mdw.dwr_mes_productspec" in query
        assert "createtime" in query
        return masters.copy()

    monkeypatch.setattr(pd, "read_sql", read_sql)
    return ArrayTransferQTimeSource(engine, enabled_products=("M626", "M678")), queries


def test_fixed_path_reuses_cpt_earliest_target_and_keeps_each_start(monkeypatch):
    first = event("1A200", "2026-11-17 09:00:00.123456")
    events = pd.DataFrame([
        first, first,
        event("1A200", "2026-11-17 10:00:00.123456"),
        event("21100", "2026-11-17 12:15:00.123456", spec="OLED_SPEC", factory="OLED"),
        event("21100", "2026-11-17 11:30:00.123456", spec="OLED_SPEC", factory="OLED"),
    ])
    source, queries = reader(monkeypatch, events, pd.DataFrame([master(), master()]))

    result = source.read(START, END)

    assert result.wait_time.tolist() == [2.5, 1.5]
    assert result.timekey.tolist() == ["20261117090000123456", "20261117100000123456"]
    assert result.prodcode.tolist() == ["M626", "M626"]
    assert result.f_step.eq("1A200").all() and result.t_step.eq("21100").all()
    assert result.step_desc.eq("1A200->21100").all() and result.shop.eq("ARRAY").all()
    assert result.prod_qty.tolist() == [30, 30]
    assert result.q_spec.isna().all() and result.sub_prod_type.isna().all()
    assert set(result) == {"step_desc", "lot_id", "prod_qty", "sub_prod_type", "f_step", "t_step", "q_spec", "wait_time", "timekey", "shop", "prodcode"}
    assert source.step_option.f_step == "1A200"
    assert source.source_signature()
    assert len(queries) == 2


def test_product_identity_uses_start_version_and_factory_and_filters_disabled(monkeypatch):
    events = pd.DataFrame([
        event("1A200", "2026-11-17 09:00"), event("21100", "2026-11-17 11:30"),
        event("1A200", "2026-11-17 09:00", lot="OTHER", spec="OTHER_SPEC"),
        event("21100", "2026-11-17 11:30", lot="OTHER"),
    ])
    source, _ = reader(monkeypatch, events, pd.DataFrame([
        master(), master(code="Z_DISABLED", spec="OTHER_SPEC"),
        master(code="M678", version="00002"), master(code="M678", factory="OLED"),
    ]))

    result = source.read(START, END)

    assert result.lot_id.tolist() == ["LOT1"] and result.prodcode.tolist() == ["M626"]


def test_unrelated_factory_mapping_conflict_cannot_block_current_start_identity(monkeypatch):
    events = pd.DataFrame([event("1A200", "2026-11-17 09:00"), event("21100", "2026-11-17 11:30")])
    source, _ = reader(monkeypatch, events, pd.DataFrame([
        master(), master(code="M678", factory="OLED"), master(code="OTHER", factory="OLED"),
    ]))
    assert source.read(START, END).prodcode.tolist() == ["M626"]


def test_missing_target_does_not_create_completed_record(monkeypatch):
    source, _ = reader(monkeypatch, pd.DataFrame([event("1A200", "2026-11-17 09:00")]), pd.DataFrame([master()]))
    assert source.read(START, END).empty


def test_ambiguous_mapping_is_unavailable_even_when_other_code_disabled(monkeypatch, caplog):
    events = pd.DataFrame([event("1A200", "2026-11-17 09:00"), event("21100", "2026-11-17 11:30")])
    source, _ = reader(monkeypatch, events, pd.DataFrame([master(), master(code="DISABLED")]))
    with pytest.raises(QTimeDataAccessError):
        source.read(START, END)
    assert "mapping" in caplog.text.lower()


def test_negative_duration_is_unavailable_instead_of_rewriting_cpt_rule(monkeypatch):
    events = pd.DataFrame([event("1A200", "2026-11-17 09:00"), event("21100", "2026-11-17 08:00")])
    source, _ = reader(monkeypatch, events, pd.DataFrame([master()]))
    with pytest.raises(QTimeDataAccessError):
        source.read(START, END)


def test_different_microsecond_starts_retain_precision_for_repository_selection(monkeypatch):
    events = pd.DataFrame([
        event("1A200", "2026-11-17 09:00:00.000001"),
        event("1A200", "2026-11-17 09:00:00.000002"),
        event("21100", "2026-11-17 11:30"),
    ])
    source, _ = reader(monkeypatch, events, pd.DataFrame([master()]))
    result = source.read(START, END)
    assert result.timekey.tolist() == ["20261117090000000001", "20261117090000000002"]
    assert len(result) == 2


def test_missing_product_mapping_is_not_treated_as_an_empty_success(monkeypatch):
    events = pd.DataFrame([event("1A200", "2026-11-17 09:00"), event("21100", "2026-11-17 11:30")])
    source, _ = reader(monkeypatch, events, pd.DataFrame(columns=list(master())))
    with pytest.raises(QTimeDataAccessError):
        source.read(START, END)


def test_source_capacity_error_does_not_return_a_truncated_success(monkeypatch):
    row = event("1A200", "2026-11-17 09:00")
    source, _ = reader(monkeypatch, pd.DataFrame([row] * 100001), pd.DataFrame([master()]))
    with pytest.raises(QTimeDataAccessError):
        source.read(START, END)


def test_database_failure_is_wrapped_without_exposing_technical_message(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("INTERNAL_DATABASE_DETAILS")

    monkeypatch.setattr(pd, "read_sql", fail)
    source = ArrayTransferQTimeSource(ReadOnlyEngine(), enabled_products=("M626",))
    with pytest.raises(QTimeDataAccessError) as captured:
        source.read(START, END)
    assert "INTERNAL_DATABASE_DETAILS" not in str(captured.value)


def test_empty_enabled_scope_does_not_query_database(monkeypatch):
    monkeypatch.setattr(pd, "read_sql", lambda *_a, **_k: pytest.fail("empty enabled scope must not read"))
    source = ArrayTransferQTimeSource(ReadOnlyEngine(), enabled_products=())
    assert source.read(START, END).empty
