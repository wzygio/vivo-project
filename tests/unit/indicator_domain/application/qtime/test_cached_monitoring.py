"""qtime 监控结果 L2 缓存测试（PRD §4.3 / 计划 Phase 1）。

缓存键 = (shop, step_descriptions, products, as_of, 决策工作簿 file_stat)；
``as_of=None`` 归一为当天 date，与显式 ``date.today()`` 命中同一缓存条目；
决策工作簿缺失时页面侧用 ``MISSING_DECISION_FILE_STAT`` 哨兵进键。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.indicator_domain.application.qtime import cached_monitoring as cached_module
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_monitoring,
    get_qtime_decision_file_stat,
)
from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.service import QTimeReportService


class CountingQTimeDataPort:
    def __init__(self) -> None:
        self.fetch_calls = 0
        self.received_query: QTimeQuery | None = None

    def list_step_options(self, shop: str) -> tuple[QTimeStepOption, ...]:
        return (QTimeStepOption("M3_DE->M3_STR", "15500", "15600"),)

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        self.fetch_calls += 1
        self.received_query = query
        return pd.DataFrame()


class FakeQTimeDecorationPort:
    decoration_path = Path("resources/indicator_domain/qtime/qtime_oos_decoration.xlsx")

    def load_decisions(self) -> pd.DataFrame:
        return pd.DataFrame()

    def save_decisions(self, decisions: pd.DataFrame) -> None:
        raise AssertionError("cache tests never write decisions")


@pytest.fixture(autouse=True)
def _clear_monitoring_cache():
    cached_module._cached_monitoring.clear()
    yield
    cached_module._cached_monitoring.clear()


def _service(port: CountingQTimeDataPort) -> QTimeReportService:
    return QTimeReportService(port, FakeQTimeDecorationPort())


def _call(service: QTimeReportService, **overrides: object):
    kwargs: dict[str, object] = {
        "shop": "ARRAY",
        "step_descriptions": ("M3_DE->M3_STR",),
        "products": ("M626",),
        "as_of": date(2026, 9, 2),
        "decision_mtime_ns": 100,
        "decision_size": 200,
    }
    kwargs.update(overrides)
    return get_cached_monitoring(service, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 缓存键行为
# ---------------------------------------------------------------------------
def test_same_key_hits_the_cache() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    _call(service)
    _call(service)

    assert port.fetch_calls == 1


def test_decision_file_stat_change_recomputes() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    _call(service)
    _call(service, decision_mtime_ns=101)
    _call(service, decision_mtime_ns=101, decision_size=201)

    assert port.fetch_calls == 3


def test_products_change_recomputes() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    _call(service)
    _call(service, products=())
    _call(service, products=("M626", "M678"))

    assert port.fetch_calls == 3


def test_as_of_none_shares_the_entry_with_explicit_today() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    _call(service, as_of=None)
    _call(service, as_of=date.today())

    assert port.fetch_calls == 1
    # None 归一为当天 date 后才进键，因此底层查询收到的是当天日期。
    assert port.received_query is not None
    assert port.received_query.end_time == datetime.combine(
        date.today() + timedelta(days=1), time.min
    )


def test_missing_workbook_uses_the_deterministic_sentinel() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)
    mtime_ns, size = MISSING_DECISION_FILE_STAT

    _call(service, decision_mtime_ns=mtime_ns, decision_size=size)
    _call(service, decision_mtime_ns=mtime_ns, decision_size=size)

    assert port.fetch_calls == 1


def test_ttl_is_read_from_global_config() -> None:
    """config/global.yaml 的 service_cache.ttl_hours.qtime_monitoring = 12h。"""
    assert cached_module._cached_monitoring._info.ttl == 12 * 60 * 60
    assert cached_module._cached_monitoring._info.max_entries == 32


# ---------------------------------------------------------------------------
# file_stat 探针
# ---------------------------------------------------------------------------
def test_decision_file_stat_missing_file_returns_none(tmp_path: Path) -> None:
    assert get_qtime_decision_file_stat(tmp_path / "missing.xlsx") is None
    assert get_qtime_decision_file_stat(None) is None


def test_decision_file_stat_existing_file(tmp_path: Path) -> None:
    workbook = tmp_path / "qtime_oos_decoration.xlsx"
    workbook.write_bytes(b"decision-ledger")

    stat = get_qtime_decision_file_stat(workbook)

    assert stat is not None
    mtime_ns, size = stat
    assert mtime_ns == int(workbook.stat().st_mtime_ns)
    assert size == len(b"decision-ledger")


# ---------------------------------------------------------------------------
# QTimeReportService.decoration_path 属性
# ---------------------------------------------------------------------------
def test_decoration_path_property_with_decoration_port() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    assert service.decoration_path == FakeQTimeDecorationPort.decoration_path


def test_decoration_path_property_without_decoration_port() -> None:
    service = QTimeReportService(CountingQTimeDataPort())

    assert service.decoration_path is None
