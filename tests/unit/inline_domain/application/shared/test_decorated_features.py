"""Unit tests for the shared decorated-feature pipeline (段2).

Covers:
- scope routing: ``ctq`` reads/writes ``ctq_sheet_oos_decoration.xlsx``,
  ``spc`` uses ``spc_sheet_oos_decoration.xlsx``;
- ``none`` scope skips decoration entirely (preprocess-only features);
- cache key behaviour: (prod_code, scope, start, end, snapshot_signature).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.shared import decorated_features as decorated_features_module
from src.inline_domain.application.shared.decorated_data import DecoratedData
from src.inline_domain.application.shared.decorated_features import (
    InMemoryFeaturesSource,
    fetch_decorated_features,
)
from src.inline_domain.core.shared.sheet_oos_decoration import SheetOosDecorationResult
from src.shared_kernel.config import ConfigLoader

PROD = "M678"
START_DATE = "2026-08-01"
END_DATE = "2026-08-10"

DECORATION_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "param_name",
    "sheet_id",
    "sheet_start_time",
    "sheet_max",
    "sheet_min",
    "sheet_mean",
    "usl",
    "lsl",
    "oos_type",
    "flag",
]


@pytest.fixture(autouse=True)
def _clear_shared_cache():
    fetch_decorated_features.clear()
    yield
    fetch_decorated_features.clear()


@pytest.fixture
def decoration_root(monkeypatch, tmp_path: Path) -> Path:
    """Redirect the decoration workbooks into tmp_path (existing test pattern)."""
    monkeypatch.setattr(
        ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path)
    )
    resources = tmp_path / "resources" / "inline_domain"
    resources.mkdir(parents=True, exist_ok=True)
    return resources


def _measurements_df() -> pd.DataFrame:
    """One sheet with an in-spec point and an OOS point (usl=60)."""
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": PROD,
                "sheet_start_time": "2026-08-05 09:00:00",
                "sheet_id": "S1",
                "step_id": "100",
                "param_name": "THK",
                "site_name": site_name,
                "param_value": value,
                "data_type": "CTQ",
            }
            for site_name, value in (("P1", 50.0), ("P2", 100.0))
        ]
    )


def _spec_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": PROD,
                "step_id": "100",
                "param_name": "THK",
                "usl": 60.0,
                "lsl": 40.0,
                "ucl": 54.0,
                "lcl": 46.0,
                "target": 50.0,
            }
        ]
    )


def _source() -> InMemoryFeaturesSource:
    return InMemoryFeaturesSource(_measurements_df(), _spec_df())


def _write_decoration_workbook(
    resources: Path, file_name: str, flag: object
) -> Path:
    """预写用户决策台账（<产品>__flags）；旧产品 sheet 的 flag 永远不再生效。"""
    path = resources / file_name
    row = {
        "prod_code": PROD,
        "step_id": "100",
        "param_name": "THK",
        "sheet_id": "S1",
        "flag": flag,
    }
    pd.DataFrame([row]).to_excel(
        path, sheet_name=f"{PROD}__flags", index=False, engine="openpyxl"
    )
    return path


# ---------------------------------------------------------------------------
# scope routing
# ---------------------------------------------------------------------------
def test_ctq_scope_uses_ctq_decoration_workbook(decoration_root: Path) -> None:
    # CTQ workbook deletes the OOS sheet; SPC workbook would keep it (flag=False).
    _write_decoration_workbook(decoration_root, "ctq_sheet_oos_decoration.xlsx", "Delete")
    _write_decoration_workbook(decoration_root, "spc_sheet_oos_decoration.xlsx", False)

    payload = fetch_decorated_features(
        _source(), PROD, "ctq", START_DATE, END_DATE, "route-ctq"
    )

    # The Delete flag from the CTQ workbook removed the only sheet's points.
    assert payload["raw_measurements_df"].empty
    assert payload["sheet_features_df"].empty
    decoration = payload["sheet_oos_decoration"]
    assert decoration["decoration_path"].endswith("ctq_sheet_oos_decoration.xlsx")
    assert decoration["decoration_sheet"] == PROD


def test_spc_scope_uses_spc_decoration_workbook(decoration_root: Path) -> None:
    # Inverse flags: SPC keeps the real OOS value, CTQ would delete it.
    _write_decoration_workbook(decoration_root, "ctq_sheet_oos_decoration.xlsx", "Delete")
    _write_decoration_workbook(decoration_root, "spc_sheet_oos_decoration.xlsx", False)

    payload = fetch_decorated_features(
        _source(), PROD, "spc", START_DATE, END_DATE, "route-spc"
    )

    # flag=False keeps the real OOS point value (no clipping, no deletion).
    assert payload["sheet_features_df"]["sheet_max"].tolist() == [100.0]
    decoration = payload["sheet_oos_decoration"]
    assert decoration["decoration_path"].endswith("spc_sheet_oos_decoration.xlsx")
    assert decoration["decoration_sheet"] == PROD


def test_ctq_scope_missing_sheet_means_empty_decoration(decoration_root: Path) -> None:
    # The CTQ workbook exists but has no sheet for this product:
    # the engine treats it as empty decoration semantics -> default clip (flag=True).
    pd.DataFrame(columns=DECORATION_COLUMNS).to_excel(
        decoration_root / "ctq_sheet_oos_decoration.xlsx",
        sheet_name="OTHER_PROD",
        index=False,
        engine="openpyxl",
    )

    payload = fetch_decorated_features(
        _source(), PROD, "ctq", START_DATE, END_DATE, "route-ctq-missing-sheet"
    )

    # Default flag=True clips the OOS point inside the spec.
    assert not payload["sheet_features_df"].empty
    assert payload["sheet_features_df"]["sheet_max"].iloc[0] < 100.0


def test_none_scope_skips_decoration(decoration_root: Path) -> None:
    payload = fetch_decorated_features(
        _source(), PROD, "none", START_DATE, END_DATE, "route-none"
    )

    # Raw OOS value untouched; no decoration payload; no workbook written.
    assert payload["sheet_features_df"]["sheet_max"].tolist() == [100.0]
    assert payload["sheet_oos_decoration"] is None
    assert payload["spec_empty"] is False
    assert list(decoration_root.glob("*.xlsx")) == []


def test_unknown_scope_raises(decoration_root: Path) -> None:
    with pytest.raises(ValueError, match="unknown decoration scope"):
        fetch_decorated_features(
            _source(), PROD, "banana", START_DATE, END_DATE, "route-invalid"
        )


def test_empty_measurements_return_empty_payload(decoration_root: Path) -> None:
    source = InMemoryFeaturesSource(pd.DataFrame(), _spec_df())

    payload = fetch_decorated_features(
        source, PROD, "spc", START_DATE, END_DATE, "route-empty"
    )

    assert payload["sheet_features_df"].empty
    assert payload["raw_measurements_df"].empty
    assert payload["sheet_oos_decoration"] is None
    assert payload["spec_empty"] is False


# ---------------------------------------------------------------------------
# cache key behaviour
# ---------------------------------------------------------------------------
class _CountingSource(InMemoryFeaturesSource):
    def __init__(self) -> None:
        super().__init__(_measurements_df(), _spec_df())
        self.measure_calls = 0

    def get_spc_measurements(self, config, force_refresh: bool = False) -> pd.DataFrame:
        self.measure_calls += 1
        return super().get_spc_measurements(config, force_refresh)


def test_cache_key_covers_window_scope_product_and_signature(
    decoration_root: Path,
) -> None:
    source = _CountingSource()

    # Same key -> single computation, second call hits the cache.
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k1")
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 1

    # Different window -> separate entry (correctness first).
    fetch_decorated_features(source, PROD, "none", START_DATE, "2026-08-09", "k1")
    assert source.measure_calls == 2

    # Different scope -> separate entry.
    fetch_decorated_features(source, PROD, "spc", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 3

    # Different snapshot signature -> separate entry.
    fetch_decorated_features(source, PROD, "none", START_DATE, END_DATE, "k2")
    assert source.measure_calls == 4

    # Different product -> separate entry.
    fetch_decorated_features(source, "M626", "none", START_DATE, END_DATE, "k1")
    assert source.measure_calls == 5


# ---------------------------------------------------------------------------
# 门控参数接线（Phase 4，PRD §5.4/§5.5）
# ---------------------------------------------------------------------------
def test_fetch_threads_gate_params_to_prepare_decorated_data(
    monkeypatch, decoration_root: Path
) -> None:
    """spc/ctq scope 下 fetch 必须把 scope/prod_code/product_revision/decision_signature
    传给 prepare_decorated_data，进而启用 core 刷新门控。"""
    recorded: dict[str, object] = {}

    def fake_prepare(**kwargs):
        recorded.update(kwargs)
        raw_df = kwargs["raw_measurements_df"]
        return DecoratedData(
            raw_measurements_df=raw_df,
            sheet_features_df=pd.DataFrame(),
            sheet_oos_decoration_result=SheetOosDecorationResult(
                raw_measurements_df=raw_df,
                decoration_df=pd.DataFrame(),
                decoration_path=Path("spc_sheet_oos_decoration.xlsx"),
                decoration_sheet=str(kwargs["prod_code"]),
            ),
        )

    monkeypatch.setattr(decorated_features_module, "prepare_decorated_data", fake_prepare)

    fetch_decorated_features(
        _source(),
        PROD,
        "spc",
        START_DATE,
        END_DATE,
        "gate-thread",
        product_revision="rev-1",
        decision_signature="sig-1",
    )

    assert recorded["scope"] == "spc"
    assert recorded["prod_code"] == PROD
    assert recorded["product_revision"] == "rev-1"
    assert recorded["decision_signature"] == "sig-1"


def test_decision_signature_and_product_revision_are_cache_key_parts(
    decoration_root: Path,
) -> None:
    """决策签名/产品 revision 进入共享 L2 缓存键：变化即产生新缓存条目。"""
    source = _CountingSource()

    fetch_decorated_features(
        source, PROD, "none", START_DATE, END_DATE, "k1",
        product_revision="r1", decision_signature="s1",
    )
    fetch_decorated_features(
        source, PROD, "none", START_DATE, END_DATE, "k1",
        product_revision="r1", decision_signature="s1",
    )
    assert source.measure_calls == 1

    # 决策签名变化 -> 新缓存条目（用户编辑 __flags 后触发 L2 miss）。
    fetch_decorated_features(
        source, PROD, "none", START_DATE, END_DATE, "k1",
        product_revision="r1", decision_signature="s2",
    )
    assert source.measure_calls == 2

    # 产品 revision 变化 -> 新缓存条目。
    fetch_decorated_features(
        source, PROD, "none", START_DATE, END_DATE, "k1",
        product_revision="r2", decision_signature="s1",
    )
    assert source.measure_calls == 3


def test_gate_params_default_to_legacy_behavior(decoration_root: Path) -> None:
    """不传新参数时行为与旧契约一致（aoi/既有调用零改动）。"""
    payload = fetch_decorated_features(
        _source(), PROD, "none", START_DATE, END_DATE, "gate-default"
    )

    assert payload["sheet_features_df"]["sheet_max"].tolist() == [100.0]
    assert payload["sheet_oos_decoration"] is None


# ---------------------------------------------------------------------------
# 决策台账载荷（PRD §5.9/§7.2）：缓存 payload 必须携带 decision_df 等字段
# ---------------------------------------------------------------------------
def test_payload_carries_decision_ledger_and_refresh_reason(
    decoration_root: Path,
) -> None:
    """spc scope 下 payload 的 sheet_oos_decoration 必须含决策台账、决策 sheet 名
    与刷新原因，否则 service 层重建的 SheetOosDecorationResult 会丢掉 decision_df。"""
    workbook = decoration_root / "spc_sheet_oos_decoration.xlsx"
    pd.DataFrame(
        [
            {
                "prod_code": PROD,
                "step_id": "100",
                "param_name": "THK",
                "sheet_id": "S1",
                "flag": False,
            }
        ]
    ).to_excel(workbook, sheet_name=f"{PROD}__flags", index=False, engine="openpyxl")

    payload = fetch_decorated_features(
        _source(), PROD, "spc", START_DATE, END_DATE, "payload-decision-ledger"
    )

    decoration = payload["sheet_oos_decoration"]
    assert decoration["decision_sheet"] == f"{PROD}__flags"
    decision_df = decoration["decision_df"]
    assert isinstance(decision_df, pd.DataFrame)
    assert decision_df["flag"].tolist() == [False]
    assert isinstance(decoration["refresh_reason"], str)
    assert decoration["refresh_reason"]


# ---------------------------------------------------------------------------
# 缓存容量（Phase 2，PRD §4.4）：矩阵模式 7 产品 × 3 scope = 21 条目不淘汰
# ---------------------------------------------------------------------------
MATRIX_PRODUCTS = ("M626", "M673", "M678", "Z517", "Z553", "Z571", "Z576")
MATRIX_SCOPES = ("spc", "ctq", "none")


def test_max_entries_keeps_all_matrix_product_scope_entries(
    decoration_root: Path,
) -> None:
    source = _CountingSource()

    for prod_code in MATRIX_PRODUCTS:
        for scope in MATRIX_SCOPES:
            fetch_decorated_features(
                source, prod_code, scope, START_DATE, END_DATE, "capacity"
            )
    expected_entries = len(MATRIX_PRODUCTS) * len(MATRIX_SCOPES)
    assert source.measure_calls == expected_entries

    # 最早写入的条目仍须命中缓存：被淘汰则 measure_calls 增加。
    fetch_decorated_features(
        source, MATRIX_PRODUCTS[0], MATRIX_SCOPES[0], START_DATE, END_DATE, "capacity"
    )

    assert source.measure_calls == expected_entries
