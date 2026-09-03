"""自动预警看板"产品 × 监控参数"矩阵数据服务测试（PRD §4.1 / 计划 Phase 3）。

四态契约：ok（达标）/ alert（不达标）/ no_data（无数据）/ error（加载失败）。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from app.sections.inline_domain.monitor.alert_matrix_service import (
    CELL_STATE_ALERT,
    CELL_STATE_ERROR,
    CELL_STATE_NO_DATA,
    CELL_STATE_OK,
    MATRIX_ROWS,
    AlertMatrixContext,
    build_alert_matrix_payload,
)

REFERENCE_DATE = date(2026, 9, 2)  # 周三：上一 ISO 周 = 2026-08-24 ~ 2026-08-30


def _make_context(**overrides) -> AlertMatrixContext:
    kwargs = {"reference_date": REFERENCE_DATE}
    kwargs.update(overrides)
    return AlertMatrixContext(**kwargs)


# ---------------------------------------------------------------------------
# 行注册表
# ---------------------------------------------------------------------------
def test_row_registry_matches_prd_order() -> None:
    assert [row.row_key for row in MATRIX_ROWS] == [
        "aoi_rs_sheet_oos",
        "aoi_tt_sheet_oos",
        "spc_sheet_oos",
        "spc_cpk_trend",
        "ctq_sheet_oos",
        "yield_lot_oos",
        "yield_trend_fluctuation",
        "qtime_sheet_oos",
    ]
    for row in MATRIX_ROWS:
        assert row.display_name
        assert row.module_group
        assert row.time_scope
        assert callable(row.evaluator)


# ---------------------------------------------------------------------------
# payload schema
# ---------------------------------------------------------------------------
def test_payload_schema_and_cartesian_cells(tmp_path: Path) -> None:
    products = ["M678", "Z571"]
    payload = build_alert_matrix_payload(
        reference_date=REFERENCE_DATE,
        products=products,
        context=_make_context(inline_resource_dir=tmp_path),
        signature="sig-1",
    )

    assert payload["products"] == products
    assert [row["row_key"] for row in payload["rows"]] == [
        row.row_key for row in MATRIX_ROWS
    ]
    assert payload["signature"] == "sig-1"
    assert payload["generated_at"]
    assert payload["reference_week"]["label"] == "2026-W35"
    assert payload["reference_week"]["start"] == "2026-08-24"
    assert payload["reference_week"]["end"] == "2026-08-31"

    cells = payload["cells"]
    assert len(cells) == len(MATRIX_ROWS) * len(products)
    for row in MATRIX_ROWS:
        for prod in products:
            cell = cells[(row.row_key, prod)]
            assert cell["state"] in {
                CELL_STATE_OK,
                CELL_STATE_ALERT,
                CELL_STATE_NO_DATA,
                CELL_STATE_ERROR,
            }
            assert cell["detail_key"] == f"{row.row_key}|{prod}"
            assert isinstance(cell["message"], str)


def test_default_products_come_from_enabled_products(tmp_path: Path) -> None:
    payload = build_alert_matrix_payload(
        reference_date=REFERENCE_DATE,
        context=_make_context(inline_resource_dir=tmp_path),
    )
    assert payload["products"] == ConfigLoader.get_enabled_products()


# ---------------------------------------------------------------------------
# sheet OOS 行（aoi_rs / aoi_tt / spc / ctq）
# ---------------------------------------------------------------------------
from app.sections.inline_domain.monitor.alert_matrix_service import MATRIX_ROW_MAP
from src.inline_domain.application.shared.decorated_data import SCOPE_DECORATION_FILE_NAME
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
)

LAST_WEEK = "2026-08-26 10:00:00"  # 上一 ISO 周内
THIS_WEEK = "2026-09-01 10:00:00"  # 本周（应被排除）
TWO_WEEKS_AGO = "2026-08-17 10:00:00"  # 上上周（应被排除）
PROD = "M678"


def _write_decoration_workbook(
    product_dir: Path,
    scope: str,
    rows: list[dict],
    *,
    sheet_name: str = PROD,
) -> Path:
    product_dir.mkdir(parents=True, exist_ok=True)
    workbook_path = product_dir / SCOPE_DECORATION_FILE_NAME[scope]
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    return workbook_path


def _spc_row(sheet_id: str, flag: object, start_time: str) -> dict:
    return {
        "factory": "OLED",
        "prod_code": PROD,
        "step_id": "21200",
        "param_name": "CD_X",
        "sheet_id": sheet_id,
        "sheet_start_time": start_time,
        "sheet_max": 1.0,
        "sheet_min": -1.0,
        "sheet_mean": 0.0,
        "usl": 0.5,
        "lsl": -0.5,
        "oos_type": "USL",
        "flag": flag,
    }


def _evaluate(row_key: str, context: AlertMatrixContext, prod: str = PROD) -> dict:
    return MATRIX_ROW_MAP[row_key].evaluator(prod, context)


def test_spc_sheet_oos_flag_false_in_previous_week_is_alert(tmp_path: Path) -> None:
    _write_decoration_workbook(
        tmp_path,
        "spc",
        [
            _spc_row("S1", False, LAST_WEEK),
            _spc_row("S2", True, LAST_WEEK),
            _spc_row("S3", False, THIS_WEEK),
            _spc_row("S4", False, TWO_WEEKS_AGO),
        ],
    )

    cell = _evaluate("spc_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_ALERT
    assert cell["detail_key"] == f"spc_sheet_oos|{PROD}"


def test_spc_sheet_oos_only_outside_previous_week_is_ok(tmp_path: Path) -> None:
    _write_decoration_workbook(
        tmp_path,
        "spc",
        [
            _spc_row("S1", False, THIS_WEEK),
            _spc_row("S2", False, TWO_WEEKS_AGO),
            _spc_row("S3", True, LAST_WEEK),
        ],
    )

    cell = _evaluate("spc_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_OK


def test_sheet_oos_missing_workbook_is_no_data(tmp_path: Path) -> None:
    for row_key in ("spc_sheet_oos", "ctq_sheet_oos", "aoi_rs_sheet_oos", "aoi_tt_sheet_oos"):
        cell = _evaluate(row_key, _make_context(inline_resource_dir=tmp_path))
        assert cell["state"] == CELL_STATE_NO_DATA, row_key


def test_sheet_oos_missing_product_sheet_is_no_data(tmp_path: Path) -> None:
    _write_decoration_workbook(
        tmp_path, "spc", [_spc_row("S1", False, LAST_WEEK)], sheet_name="Z571"
    )

    cell = _evaluate("spc_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_NO_DATA


def test_sheet_oos_read_failure_is_error(tmp_path: Path, monkeypatch) -> None:
    workbook = _write_decoration_workbook(tmp_path, "spc", [_spc_row("S1", False, LAST_WEEK)])
    assert workbook.exists()

    def _raise(*args, **kwargs):
        raise SheetOosDecorationReadError("Unable to read existing Sheet OOS decoration file")

    monkeypatch.setattr(
        "app.sections.inline_domain.monitor.alert_matrix_service.load_sheet_oos_decoration",
        _raise,
    )

    payload = build_alert_matrix_payload(
        products=[PROD],
        context=_make_context(inline_resource_dir=tmp_path),
    )

    cell = payload["cells"][("spc_sheet_oos", PROD)]
    assert cell["state"] == CELL_STATE_ERROR
    assert "修饰工作簿读取失败" in cell["message"]
    assert "Traceback" not in cell["message"]


def test_aoi_tt_uses_start_time_column(tmp_path: Path) -> None:
    """aoi_tt 行时间列为 start_time（非 sheet_start_time），且走自定义键列。"""
    _write_decoration_workbook(
        tmp_path,
        "aoi_tt",
        [
            {
                "factory": "ARRAY",
                "prod_code": PROD,
                "step_id": "11629",
                "tt_name": "TT_A",
                "sheet_id": "S1",
                "lot_id": "L1",
                "start_time": LAST_WEEK,
                "tt_qty": 3,
                "usl": 1,
                "flag": False,
            }
        ],
    )

    cell = _evaluate("aoi_tt_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_ALERT


def test_aoi_rs_uses_custom_key_columns_and_sheet_start_time(tmp_path: Path) -> None:
    """aoi_rs 行保留 chart_kind 等自定义列（键列不被裁剪为 SPC 口径）。"""
    _write_decoration_workbook(
        tmp_path,
        "aoi_rs",
        [
            {
                "prod_code": PROD,
                "factory": "ARRAY",
                "step_id": "11629",
                "rs_code": "A1PPS",
                "chart_kind": "lot",
                "point_id": "L1",
                "value": 2.5,
                "spec": 1.0,
                "sheet_start_time": LAST_WEEK,
                "flag": False,
            }
        ],
    )

    cell = _evaluate("aoi_rs_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_ALERT


def test_ctq_sheet_oos_flag_false_in_previous_week_is_alert(tmp_path: Path) -> None:
    _write_decoration_workbook(
        tmp_path, "ctq", [_spc_row("S1", False, LAST_WEEK)]
    )

    cell = _evaluate("ctq_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_ALERT


# ---------------------------------------------------------------------------
# 单元格级降级（PRD §3.1-4）
# ---------------------------------------------------------------------------
from app.sections.inline_domain.monitor import alert_matrix_service as matrix_module


def test_single_cell_exception_does_not_block_other_cells(tmp_path: Path, monkeypatch) -> None:
    """同一行内某产品 evaluator 抛异常：仅该单元格 error，其余单元格正常。"""
    _write_decoration_workbook(
        tmp_path, "spc", [_spc_row("S1", False, LAST_WEEK)], sheet_name="M678"
    )
    _write_decoration_workbook(
        tmp_path, "spc", [_spc_row("S1", True, LAST_WEEK)], sheet_name="Z571"
    )
    original_load = matrix_module.load_sheet_oos_decoration

    def flaky_load(product_dir, file_name=None, sheet_name=None, key_columns=None):
        if sheet_name == "M678":
            raise RuntimeError("模拟底层读取异常")
        return original_load(
            product_dir,
            file_name=file_name,
            sheet_name=sheet_name,
            key_columns=key_columns,
        )

    monkeypatch.setattr(matrix_module, "load_sheet_oos_decoration", flaky_load)

    payload = build_alert_matrix_payload(
        products=["M678", "Z571"],
        context=_make_context(inline_resource_dir=tmp_path),
    )

    broken = payload["cells"][("spc_sheet_oos", "M678")]
    assert broken["state"] == CELL_STATE_ERROR
    assert broken["message"] == "模拟底层读取异常"
    assert payload["cells"][("spc_sheet_oos", "Z571")]["state"] == CELL_STATE_OK


def test_product_with_no_data_degrades_only_its_own_column(tmp_path: Path) -> None:
    """单产品整体数据缺失 → 该产品各单元格 no_data，其他产品列不受影响。"""
    _write_decoration_workbook(
        tmp_path, "spc", [_spc_row("S1", False, LAST_WEEK)], sheet_name="M678"
    )

    payload = build_alert_matrix_payload(
        products=["M678", "Z571"],
        context=_make_context(inline_resource_dir=tmp_path),
    )

    cells = payload["cells"]
    assert cells[("spc_sheet_oos", "M678")]["state"] == CELL_STATE_ALERT
    assert cells[("spc_sheet_oos", "Z571")]["state"] == CELL_STATE_NO_DATA
    # 无数据产品不影响整板：所有 (行, 产品) 单元格齐全
    assert len(cells) == len(MATRIX_ROWS) * 2


# ---------------------------------------------------------------------------
# spc 趋势波动行（CPK）
# ---------------------------------------------------------------------------
def _cpk_row(period_label: str, cpk: float, decorated: bool = False) -> dict:
    return {
        "factory": "OLED",
        "step_id": "21200",
        "param_name": "CD_X",
        "period_type": "week",
        "period_label": period_label,
        "cpk": cpk,
        "cpk_decorated": decorated,
    }


PREV_WEEK_LABEL = "2026-W35"  # 参考日 2026-09-02 的上一 ISO 周


def test_spc_cpk_below_threshold_previous_week_is_alert() -> None:
    capability_df = pd.DataFrame(
        [
            _cpk_row(PREV_WEEK_LABEL, 1.10),
            _cpk_row(PREV_WEEK_LABEL, 1.20, decorated=True),  # 已修饰不报警
            _cpk_row("2026-W36", 0.90),  # 本周不参与
        ]
    )
    context = _make_context(spc_cpk_loader=lambda prod: capability_df)

    cell = _evaluate("spc_cpk_trend", context)

    assert cell["state"] == CELL_STATE_ALERT


def test_spc_cpk_all_above_threshold_is_ok() -> None:
    capability_df = pd.DataFrame([_cpk_row(PREV_WEEK_LABEL, 1.50)])
    context = _make_context(spc_cpk_loader=lambda prod: capability_df)

    assert _evaluate("spc_cpk_trend", context)["state"] == CELL_STATE_OK


def test_spc_cpk_loader_returns_none_is_no_data() -> None:
    context = _make_context(spc_cpk_loader=lambda prod: None)

    assert _evaluate("spc_cpk_trend", context)["state"] == CELL_STATE_NO_DATA


def test_spc_cpk_loader_exception_is_error() -> None:
    def _raise(prod: str):
        raise RuntimeError("CPK 快照不可用")

    context = _make_context(spc_cpk_loader=_raise)

    cell = _evaluate("spc_cpk_trend", context)
    assert cell["state"] == CELL_STATE_ERROR
    assert "CPK 快照不可用" in cell["message"]


# ---------------------------------------------------------------------------
# yield 单片异常行（lot 超规，呈现层过滤上一 ISO 周）
# ---------------------------------------------------------------------------
def _lot_data(warehousing_times: list[str], defect_rate: float = 0.5) -> dict:
    return {
        "code_level_details": {
            "OLED_Mura": pd.DataFrame(
                [
                    {
                        "lot_id": f"LOT{i}",
                        "defect_desc": "OLED_Mura",
                        "defect_rate": defect_rate,
                        "defect_panel_count": 3,
                        "warehousing_time": w_time,
                        "array_input_time": w_time,
                    }
                    for i, w_time in enumerate(warehousing_times)
                ]
            )
        }
    }


WARNING_LINES = {"OLED_Mura": {"upper": 0.10, "lower": 0.0}}


def test_yield_lot_oos_in_previous_week_is_alert() -> None:
    # 2026-08-26（上周）超规 + 2026-09-01（本周）超规
    lot_data = _lot_data(["20260826", "20260901"])
    context = _make_context(
        yield_lot_loader=lambda prod: (lot_data, WARNING_LINES)
    )

    assert _evaluate("yield_lot_oos", context)["state"] == CELL_STATE_ALERT


def test_yield_lot_oos_only_this_week_is_ok() -> None:
    lot_data = _lot_data(["20260901"])
    context = _make_context(
        yield_lot_loader=lambda prod: (lot_data, WARNING_LINES)
    )

    assert _evaluate("yield_lot_oos", context)["state"] == CELL_STATE_OK


def test_yield_lot_loader_returns_none_is_no_data() -> None:
    context = _make_context(yield_lot_loader=lambda prod: None)

    assert _evaluate("yield_lot_oos", context)["state"] == CELL_STATE_NO_DATA


# ---------------------------------------------------------------------------
# yield 趋势波动行（结构化记录非空即 alert，period 制）
# ---------------------------------------------------------------------------
def _trend_data(spike: bool) -> tuple[dict, dict]:
    rates = [0.001, 0.01] if spike else [0.001, 0.0011]
    monthly = pd.DataFrame(
        {
            "defect_group": ["OLED_Mura", "OLED_Mura"],
            "time_period": ["2026-07", "2026-08"],
            "defect_rate": rates,
        }
    )
    return {"monthly": monthly}, {"monthly": pd.DataFrame()}


def test_yield_trend_with_records_is_alert() -> None:
    context = _make_context(yield_trend_loader=lambda prod: _trend_data(spike=True))

    assert _evaluate("yield_trend_fluctuation", context)["state"] == CELL_STATE_ALERT


def test_yield_trend_without_records_is_ok() -> None:
    context = _make_context(yield_trend_loader=lambda prod: _trend_data(spike=False))

    assert _evaluate("yield_trend_fluctuation", context)["state"] == CELL_STATE_OK


def test_yield_trend_loader_returns_none_is_no_data() -> None:
    context = _make_context(yield_trend_loader=lambda prod: None)

    assert _evaluate("yield_trend_fluctuation", context)["state"] == CELL_STATE_NO_DATA


# ---------------------------------------------------------------------------
# qtime 单片异常行（prodcode 拆分 + timekey 过滤上一 ISO 周）
# ---------------------------------------------------------------------------
def _qtime_frames(
    alerts: list[dict], details: list[dict] | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return pd.DataFrame(details or []), pd.DataFrame(alerts)


def _qtime_alert(prodcode: str, timekey: str) -> dict:
    return {
        "shop": "OLED",
        "prodcode": prodcode,
        "f_step": "21200",
        "t_step": "21300",
        "step_desc": "M3_DE->M3_STR",
        "lot_id": "L1",
        "timekey": timekey,
        "q_spec": 24.0,
        "wait_time": 30.0,
        "over_hours": 6.0,
        "flag": False,
    }


def test_qtime_alert_in_previous_week_is_alert() -> None:
    details, alerts = _qtime_frames(
        [_qtime_alert(PROD, "20260826100000")],
        details=[{"prodcode": PROD, "timekey": "20260826100000"}],
    )
    context = _make_context(qtime_monitoring_loader=lambda: (details, alerts))

    assert _evaluate("qtime_sheet_oos", context)["state"] == CELL_STATE_ALERT


def test_qtime_alert_outside_previous_week_is_ok() -> None:
    details, alerts = _qtime_frames(
        [
            _qtime_alert(PROD, "20260901100000"),  # 本周
            _qtime_alert(PROD, "20260817100000"),  # 上上周
        ],
        details=[{"prodcode": PROD, "timekey": "20260901100000"}],
    )
    context = _make_context(qtime_monitoring_loader=lambda: (details, alerts))

    assert _evaluate("qtime_sheet_oos", context)["state"] == CELL_STATE_OK


def test_qtime_product_absent_is_no_data() -> None:
    details, alerts = _qtime_frames(
        [_qtime_alert("Z571", "20260826100000")],
        details=[{"prodcode": "Z571", "timekey": "20260826100000"}],
    )
    context = _make_context(qtime_monitoring_loader=lambda: (details, alerts))

    assert _evaluate("qtime_sheet_oos", context)["state"] == CELL_STATE_NO_DATA


def test_qtime_loader_fetched_once_for_all_products() -> None:
    calls = []

    def loader():
        calls.append(1)
        details, alerts = _qtime_frames(
            [_qtime_alert("M678", "20260826100000"), _qtime_alert("Z571", "20260826100000")]
        )
        return details, alerts

    context = _make_context(qtime_monitoring_loader=loader)
    payload = build_alert_matrix_payload(products=["M678", "Z571"], context=context)

    assert len(calls) == 1
    assert payload["cells"][("qtime_sheet_oos", "M678")]["state"] == CELL_STATE_ALERT
    assert payload["cells"][("qtime_sheet_oos", "Z571")]["state"] == CELL_STATE_ALERT


def test_qtime_loader_failure_degrades_all_qtime_cells_only() -> None:
    def loader():
        raise TimeoutError("Q-Time 生产库查询超时")

    _workbook_ctx = _make_context(qtime_monitoring_loader=loader)
    payload = build_alert_matrix_payload(products=["M678"], context=_workbook_ctx)

    qtime_cell = payload["cells"][("qtime_sheet_oos", "M678")]
    assert qtime_cell["state"] == CELL_STATE_ERROR
    assert "超时" in qtime_cell["message"]
    # 其他行不受影响（本上下文未配数据源 → no_data，而非 error）
    assert payload["cells"][("spc_cpk_trend", "M678")]["state"] == CELL_STATE_NO_DATA


# ---------------------------------------------------------------------------
# 缓存签名（ADR-0001：键全部为原生可哈希类型）
# ---------------------------------------------------------------------------
from app.sections.inline_domain.monitor.alert_matrix_service import (
    build_alert_matrix_signature,
)


def _components() -> dict:
    return {
        "product_revisions": {"M678": "r1", "Z571": "r2"},
        "scope_decision_signatures": {"spc|M678": "d1", "ctq|M678": "d2"},
        "qtime_decision_file_stat": [100, 200],
    }


def test_signature_stable_for_same_components() -> None:
    sig_a = build_alert_matrix_signature(products=["M678", "Z571"], components=_components())
    sig_b = build_alert_matrix_signature(products=["M678", "Z571"], components=_components())
    assert sig_a == sig_b


def test_signature_changes_with_any_component() -> None:
    base = build_alert_matrix_signature(products=["M678", "Z571"], components=_components())

    by_revision = _components()
    by_revision["product_revisions"]["M678"] = "r9"
    assert build_alert_matrix_signature(products=["M678", "Z571"], components=by_revision) != base

    by_decision = _components()
    by_decision["scope_decision_signatures"]["spc|M678"] = "d9"
    assert build_alert_matrix_signature(products=["M678", "Z571"], components=by_decision) != base

    by_qtime = _components()
    by_qtime["qtime_decision_file_stat"] = [101, 200]
    assert build_alert_matrix_signature(products=["M678", "Z571"], components=by_qtime) != base

    assert build_alert_matrix_signature(products=["M678"], components=_components()) != base


# ---------------------------------------------------------------------------
# 缓存入口（st.cache_data 包装，周归一 + 调用计数验证）
# ---------------------------------------------------------------------------
from app.sections.inline_domain.monitor import alert_matrix_cache as cache_module
from app.sections.inline_domain.monitor.alert_matrix_cache import (
    get_alert_matrix_week_start,
    get_cached_alert_matrix,
)


@pytest.fixture(autouse=True)
def _clear_matrix_cache():
    cache_module._cached_alert_matrix_payload.clear()
    yield
    cache_module._cached_alert_matrix_payload.clear()


def _counting_context_factory(calls: list, context: AlertMatrixContext):
    def factory() -> AlertMatrixContext:
        calls.append(1)
        return context

    return factory


def test_get_cached_alert_matrix_hits_within_same_week(tmp_path: Path) -> None:
    calls: list = []
    factory = _counting_context_factory(
        calls, _make_context(inline_resource_dir=tmp_path)
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cache_module, "build_default_signature_components", lambda products: _components())
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 2), products=["M678"], _context_factory=factory
        )
        # 同周另一天 + 同签名 → 命中缓存，不重建
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 6), products=["M678"], _context_factory=factory
        )

    assert len(calls) == 1


def test_get_cached_alert_matrix_rebuilds_on_signature_change(tmp_path: Path) -> None:
    calls: list = []
    factory = _counting_context_factory(
        calls, _make_context(inline_resource_dir=tmp_path)
    )

    with pytest.MonkeyPatch.context() as mp:
        components = _components()
        mp.setattr(
            cache_module,
            "build_default_signature_components",
            lambda products: components,
        )
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 2), products=["M678"], _context_factory=factory
        )
        components["product_revisions"]["M678"] = "r9"  # 任一签名分量变化
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 2), products=["M678"], _context_factory=factory
        )

    assert len(calls) == 2


def test_get_cached_alert_matrix_rebuilds_on_week_change(tmp_path: Path) -> None:
    calls: list = []
    factory = _counting_context_factory(
        calls, _make_context(inline_resource_dir=tmp_path)
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cache_module, "build_default_signature_components", lambda products: _components())
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 2), products=["M678"], _context_factory=factory
        )
        get_cached_alert_matrix(
            reference_date=date(2026, 9, 9), products=["M678"], _context_factory=factory
        )

    assert len(calls) == 2


def test_week_start_normalizes_to_monday() -> None:
    assert get_alert_matrix_week_start(date(2026, 9, 2)) == date(2026, 8, 31)
    assert get_alert_matrix_week_start(date(2026, 8, 31)) == date(2026, 8, 31)
    assert get_alert_matrix_week_start(date(2026, 9, 6)) == date(2026, 8, 31)


def test_cache_ttl_is_read_from_global_config() -> None:
    """config/global.yaml 的 service_cache.ttl_hours.alert_matrix_payload = 12h。"""
    assert cache_module._cached_alert_matrix_payload._info.ttl == 12 * 60 * 60


def test_default_signature_components_cover_all_dimensions(monkeypatch) -> None:
    monkeypatch.setattr(
        cache_module, "get_product_cache_revision", lambda prod: f"rev-{prod}"
    )
    monkeypatch.setattr(
        cache_module,
        "get_scope_decision_signature",
        lambda scope, prod: f"sig-{scope}-{prod}",
    )
    monkeypatch.setattr(
        cache_module, "get_qtime_decision_file_stat", lambda path: (111, 222)
    )

    components = cache_module.build_default_signature_components(["M678"])

    assert components["product_revisions"] == {"M678": "rev-M678"}
    for scope in ("spc", "ctq", "aoi_tt", "aoi_rs"):
        assert components["scope_decision_signatures"][f"{scope}|M678"] == f"sig-{scope}-M678"
    assert components["qtime_decision_file_stat"] == [111, 222]


# ---------------------------------------------------------------------------
# 单元格 alert_factories 与行级厂别细分标记（Phase 8：矩阵厂别筛选）
# ---------------------------------------------------------------------------
def test_sheet_oos_alert_cell_carries_sorted_unique_factories(tmp_path: Path) -> None:
    """预警单元格附带 alert_factories：flag=FALSE 且上周记录的 factory 排序去重。"""
    _write_decoration_workbook(
        tmp_path,
        "spc",
        [
            {**_spc_row("S1", False, LAST_WEEK), "factory": "TP"},
            {**_spc_row("S2", False, LAST_WEEK), "factory": "OLED"},
            {**_spc_row("S3", False, LAST_WEEK), "factory": "tp"},  # 大小写归一去重
            {**_spc_row("S4", True, LAST_WEEK), "factory": "ARRAY"},  # 已修饰不计入
            {**_spc_row("S5", False, THIS_WEEK), "factory": "ARRAY"},  # 本周不计入
        ],
    )

    cell = _evaluate("spc_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_ALERT
    assert cell["alert_factories"] == ["OLED", "TP"]


def test_sheet_oos_ok_cell_has_empty_alert_factories(tmp_path: Path) -> None:
    _write_decoration_workbook(
        tmp_path, "spc", [_spc_row("S1", True, LAST_WEEK)]
    )

    cell = _evaluate("spc_sheet_oos", _make_context(inline_resource_dir=tmp_path))

    assert cell["state"] == CELL_STATE_OK
    assert cell["alert_factories"] == []


def test_spc_cpk_alert_cell_carries_factories() -> None:
    capability_df = pd.DataFrame(
        [
            _cpk_row(PREV_WEEK_LABEL, 1.10),  # factory=OLED
            {**_cpk_row(PREV_WEEK_LABEL, 1.20), "factory": "TP"},
            _cpk_row(PREV_WEEK_LABEL, 1.50),  # 高于阈值不计入
        ]
    )
    context = _make_context(spc_cpk_loader=lambda prod: capability_df)

    cell = _evaluate("spc_cpk_trend", context)

    assert cell["state"] == CELL_STATE_ALERT
    assert cell["alert_factories"] == ["OLED", "TP"]


def test_qtime_alert_cell_carries_shop_factories() -> None:
    """qtime 行 alert_factories 取自预警记录的 shop 打标（shop 即厂别）。"""
    details, alerts = _qtime_frames(
        [
            {**_qtime_alert(PROD, "20260826100000"), "shop": "TP"},
            {**_qtime_alert(PROD, "20260827100000"), "shop": "OLED"},
            {**_qtime_alert(PROD, "20260901100000"), "shop": "ARRAY"},  # 本周不计入
        ],
        details=[{"prodcode": PROD, "timekey": "20260826100000"}],
    )
    context = _make_context(qtime_monitoring_loader=lambda: (details, alerts))

    cell = _evaluate("qtime_sheet_oos", context)

    assert cell["state"] == CELL_STATE_ALERT
    assert cell["alert_factories"] == ["OLED", "TP"]


def test_yield_rows_do_not_support_factory_filter() -> None:
    """yield 两行记录结构无厂别列：行标记不支持厂别细分，单元格 alert_factories 为空。"""
    payload = build_alert_matrix_payload(
        products=[PROD],
        context=_make_context(
            yield_lot_loader=lambda prod: (_lot_data(["20260826"]), WARNING_LINES),
            yield_trend_loader=lambda prod: _trend_data(spike=True),
        ),
    )

    row_flags = {
        row["row_key"]: row["factory_filter_supported"] for row in payload["rows"]
    }
    assert row_flags["yield_lot_oos"] is False
    assert row_flags["yield_trend_fluctuation"] is False
    for row_key, flag in row_flags.items():
        if not row_key.startswith("yield_"):
            assert flag is True, row_key

    assert payload["cells"][("yield_lot_oos", PROD)]["state"] == CELL_STATE_ALERT
    assert payload["cells"][("yield_lot_oos", PROD)]["alert_factories"] == []
    assert payload["cells"][("yield_trend_fluctuation", PROD)]["alert_factories"] == []


def test_load_all_product_qtime_monitoring_tags_shop_on_union(monkeypatch) -> None:
    """全产品 qtime union 时按 shop 打标（ARRAY/OLED/TP 本身即厂别）。"""
    from types import SimpleNamespace

    from src.indicator_domain.application.qtime.service import QTimeMonitoringResult

    def _result(shop: str) -> QTimeMonitoringResult:
        return QTimeMonitoringResult(
            details=pd.DataFrame([{"prodcode": "M678", "timekey": "20260826100000"}]),
            alerts=pd.DataFrame([{"prodcode": "M678", "timekey": "20260826100000"}]),
            decoration=pd.DataFrame(),
            decisions=pd.DataFrame(),
            decoration_path=None,
        )

    monkeypatch.setattr(
        cache_module,
        "build_qtime_service",
        lambda db: SimpleNamespace(decoration_path=None),
    )
    monkeypatch.setattr(
        cache_module, "get_qtime_decision_file_stat", lambda path: None
    )
    monkeypatch.setattr(
        cache_module,
        "get_cached_shop_monitoring",
        lambda service, *, shop, as_of, decision_mtime_ns, decision_size: _result(shop),
    )

    details_df, alerts_df = cache_module.load_all_product_qtime_monitoring(
        object(), date(2026, 9, 2)
    )

    assert alerts_df["shop"].tolist() == ["ARRAY", "OLED", "TP"]
    assert "shop" not in details_df.columns
