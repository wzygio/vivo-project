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
from pydantic import ValidationError

from src.indicator_domain.application.qtime import cached_monitoring as cached_module
from src.indicator_domain.application.qtime.cached_monitoring import (
    MISSING_DECISION_FILE_STAT,
    get_cached_monitoring,
    get_cached_shop_monitoring,
    get_qtime_cached_funcs,
    get_qtime_decision_file_stat,
)
from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.application.qtime.service import QTimeReportService


class CountingQTimeDataPort:
    def __init__(
        self,
        step_options: tuple[QTimeStepOption, ...] = (
            QTimeStepOption("M3_DE->M3_STR", "15500", "15600"),
        ),
    ) -> None:
        self.fetch_calls = 0
        self.received_query: QTimeQuery | None = None
        self._step_options = step_options

    def list_step_options(self, shop: str) -> tuple[QTimeStepOption, ...]:
        return self._step_options

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


# ---------------------------------------------------------------------------
# get_cached_shop_monitoring：厂别级公共入口（矩阵与 Q-Time 页面共享）
# ---------------------------------------------------------------------------
def _shop_call(service: QTimeReportService, **overrides: object):
    kwargs: dict[str, object] = {
        "shop": "ARRAY",
        "as_of": date(2026, 9, 2),
        "decision_mtime_ns": 100,
        "decision_size": 200,
    }
    kwargs.update(overrides)
    return get_cached_shop_monitoring(service, **kwargs)  # type: ignore[arg-type]


def test_shop_monitoring_queries_all_steps_and_empty_products() -> None:
    """公共入口 = 该厂别全部站点 + products=()（全产品），不新增缓存键维度。"""
    port = CountingQTimeDataPort()
    service = _service(port)

    _shop_call(service)

    assert port.received_query is not None
    assert port.received_query.step_descriptions == ("M3_DE->M3_STR",)
    assert port.received_query.products == ()


def test_shop_monitoring_shares_cache_entry_between_matrix_and_page_paths() -> None:
    """先矩阵式调用再页面式调用：底层 fetch 不增加，且与显式全量键命中同一条目。"""
    port = CountingQTimeDataPort()
    service = _service(port)

    _shop_call(service)  # 矩阵路径（as_of=参考周周一）
    _shop_call(service)  # 页面路径（同 shop、同 as_of）
    get_cached_monitoring(
        service,
        shop="ARRAY",
        step_descriptions=("M3_DE->M3_STR",),
        products=(),
        as_of=date(2026, 9, 2),
        decision_mtime_ns=100,
        decision_size=200,
    )

    assert port.fetch_calls == 1


def test_shop_monitoring_as_of_none_normalizes_like_get_cached_monitoring() -> None:
    port = CountingQTimeDataPort()
    service = _service(port)

    _shop_call(service, as_of=None)
    _shop_call(service, as_of=date.today())

    assert port.fetch_calls == 1


def test_shop_monitoring_shop_without_steps_raises_validation_error() -> None:
    """厂别无站点：与现有错误路径一致（QTimeQuery min_length=1 上抛），由调用方降级。"""
    port = CountingQTimeDataPort(step_options=())
    service = _service(port)

    with pytest.raises(ValidationError):
        _shop_call(service)


def test_shop_monitoring_filter_options_failure_propagates() -> None:
    """取站点失败：域错误上抛，由调用方降级（页面 error / 矩阵单元格 ⬜）。"""

    class FailingOptionsPort(CountingQTimeDataPort):
        def list_step_options(self, shop: str) -> tuple[QTimeStepOption, ...]:
            raise QTimeDataAccessError("Q-Time 数据读取失败")

    service = _service(FailingOptionsPort())

    with pytest.raises(QTimeDataAccessError):
        _shop_call(service)


def test_qtime_cached_funcs_exposes_monitoring_cache_for_page_refresh() -> None:
    """页头「刷新缓存」清理清单：qtime L2 缓存函数必须可 clear。"""
    funcs = get_qtime_cached_funcs()

    assert cached_module._cached_monitoring in funcs
    assert all(hasattr(func, "clear") for func in funcs)
