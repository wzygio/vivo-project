from types import SimpleNamespace

import pandas as pd

from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.dtos import SpcQueryConfig


def _query(prod_code: str = "ALL") -> str:
    return SpcQueryConfig(
        prod_code=prod_code,
        start_date="2026-06-01",
        end_date="2026-08-13",
        data_type_filter="ALL",
    ).model_dump_json()


class _OkRepository:
    """正常替身：无刷新状态结构，调度未异常即视为成功。"""

    def get_spc_measurements(self, config, force_refresh: bool = False) -> pd.DataFrame:
        return pd.DataFrame()


class _ExplodingRepository:
    def get_spc_measurements(self, config, force_refresh: bool = False) -> pd.DataFrame:
        raise RuntimeError("database unavailable")


class _DegradedRepository:
    """模拟真实组合链：DB 失败降级旧快照，raw 仓储暴露 last_refresh_from_db=False。"""

    def __init__(self) -> None:
        raw = SimpleNamespace(last_refresh_from_db=False)
        preparation = SimpleNamespace(raw_measurements=raw)
        self._spc_source = SimpleNamespace(_preparation=preparation)

    def get_spc_measurements(self, config, force_refresh: bool = False) -> pd.DataFrame:
        # 降级路径返回旧快照数据而非抛错，正式假成功的来源
        return pd.DataFrame([{"sheet_id": "STALE"}])


def _patch_products(monkeypatch, products: list[str]) -> None:
    monkeypatch.setattr(
        MonitorAnalysisService,
        "discover_monitor_products",
        staticmethod(lambda _root: products),
    )


def test_safe_refresh_snapshots_succeeds_when_all_products_refresh(
    monkeypatch,
) -> None:
    _patch_products(monkeypatch, ["M626", "M673"])

    result = MonitorAnalysisService.safe_refresh_snapshots(
        lambda _prod: _OkRepository(),
        _query(),
    )

    assert result is True


def test_safe_refresh_snapshots_fails_when_any_product_raises(
    monkeypatch,
) -> None:
    _patch_products(monkeypatch, ["M626", "M673"])
    repositories = {"M626": _OkRepository(), "M673": _ExplodingRepository()}

    result = MonitorAnalysisService.safe_refresh_snapshots(
        lambda prod: repositories[prod],
        _query(),
    )

    assert result is False


def test_safe_refresh_snapshots_fails_when_any_product_degrades_to_stale_snapshot(
    monkeypatch,
) -> None:
    _patch_products(monkeypatch, ["M626", "M673"])
    repositories = {"M626": _OkRepository(), "M673": _DegradedRepository()}

    result = MonitorAnalysisService.safe_refresh_snapshots(
        lambda prod: repositories[prod],
        _query(),
    )

    assert result is False


def test_safe_refresh_snapshots_fails_on_invalid_config() -> None:
    result = MonitorAnalysisService.safe_refresh_snapshots(
        lambda _prod: _OkRepository(),
        "not-a-valid-json",
    )

    assert result is False
