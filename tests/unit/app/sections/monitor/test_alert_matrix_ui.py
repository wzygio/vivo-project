"""自动预警矩阵 UI 与点击详情懒加载的 AppTest 测试（PRD §4.2 / Phase 4+5）。

覆盖：
- 矩阵区渲染存在性、四态字符、图例（含 period 制口径注明）、⬜ tooltip；
- 点击 🔴 懒加载详情（loader 只在选中后调用）；
- 再次打开同单元格命中 st.cache_data（loader 不重算）；
- 点击 🟢/⚪/⬜ 只显示说明文案、不产生详情计算；
- 详情加载失败降级为 error，不影响矩阵；
- 矩阵整体失败降级为 info；
- sheet OOS / CPK / yield 详情的渲染管线接线（chart key 前缀 matrix_detail）。
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_detail
from app.sections.inline_domain.monitor.alert_matrix import (
    MATRIX_SELECTION_STATE_KEY,
    matrix_cell_button_key,
)
from app.sections.inline_domain.spc import spc_dashboard
from app.sections.yield_domain import yield_dashboard

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "alert_matrix_app.py"

PRODUCTS = ["M678", "Z571"]
ROW_COUNT = 8  # PRD §3.1 八行


def _new_app(nonce: str) -> AppTest:
    app = AppTest.from_file(str(FIXTURE_PATH))
    app.session_state["fixture_nonce"] = nonce
    return app


def _inject_loaders(monkeypatch: pytest.MonkeyPatch, loaders: dict) -> None:
    monkeypatch.setattr(
        alert_matrix_detail,
        "build_default_detail_loaders",
        lambda db_manager=None: loaders,
    )


def _qtime_bundle() -> dict:
    alerts = pd.DataFrame(
        [
            {
                "f_step": "15500",
                "t_step": "15600",
                "step_desc": "M3_DE->M3_STR",
                "timekey": "20260826020000",
                "lot_id": "L3MY67005AA",
                "q_spec": 1.0,
                "wait_time": 1.5,
                "over_hours": 0.5,
                "prodcode": "M678",
            }
        ]
    )
    details = pd.DataFrame(
        [
            {
                "step_desc": "M3_DE->M3_STR",
                "lot_id": "L3MY67005AA",
                "q_spec": 1.0,
                "wait_time": 1.5,
                "timekey": "20260826020000",
                "prodcode": "M678",
            }
        ]
    )
    return {
        "kind": "qtime",
        "alerts_df": alerts,
        "details_df": details,
        "total_lots": 1,
    }


def _spc_sheet_oos_bundle() -> dict:
    alerts = pd.DataFrame(
        [
            {
                "厂别": "TP",
                "站点": "41260",
                "参数名称": "4PP_Rs",
                "Sheet ID": "S1",
                "超规时间": "2026-08-26 10:00:00",
                "超规类型": "OOS",
            }
        ]
    )
    frames = {
        "period_capability_df": pd.DataFrame(
            [
                {
                    "factory": "TP",
                    "step_id": "41260",
                    "param_name": "4PP_Rs",
                    "period_type": "week",
                    "period_label": "2026-W35",
                    "cpk": 1.0,
                }
            ]
        ),
        "sheet_features_df": pd.DataFrame(
            [{"factory": "TP", "step_id": "41260", "param_name": "4PP_Rs", "sheet_id": "S1"}]
        ),
        "raw_measurements_df": pd.DataFrame(
            [
                {
                    "factory": "TP",
                    "step_id": "41260",
                    "param_name": "4PP_Rs",
                    "sheet_id": "S1",
                    "point_value": 1.0,
                }
            ]
        ),
    }
    return {
        "kind": "sheet_oos",
        "scope": "spc",
        "alerts_df": alerts,
        "frames": frames,
        "end_date": "2026-08-31",
    }


def _spc_cpk_bundle() -> dict:
    alerts = pd.DataFrame(
        [
            {
                "厂别": "TP",
                "站点": "41260",
                "参数名称": "4PP_Rs",
                "超规周次": "2026-W35",
                "CPK值": 1.278,
            }
        ]
    )
    bundle = _spc_sheet_oos_bundle()
    return {"kind": "spc_cpk", "alerts_df": alerts, "frames": bundle["frames"]}


def _yield_trend_bundle() -> dict:
    records = [
        {
            "level": "code",
            "defect_group": "OLED_Mura",
            "defect_desc": "Black Spot",
            "period_scope": "weekly",
        }
    ]
    return {
        "kind": "yield",
        "yield_mode": "trend",
        "records": records,
        "mwd_code_data": {},
        "lot_data": {},
        "sheet_data": {},
        "mapping_data": pd.DataFrame(),
        "warning_lines": {},
        "hotspot_scripts": [],
        "mapping_layout": None,
        "product_code": "M678",
    }


# ---------------------------------------------------------------------------
# Phase 4：矩阵本体
# ---------------------------------------------------------------------------
def test_matrix_renders_title_legend_groups_and_four_states() -> None:
    app = _new_app("render-basic").run()

    assert not app.exception
    assert any("预警矩阵（上一周 2026-W35）" in m.value for m in app.markdown)

    captions = [c.value for c in app.caption]
    assert any("🟢" in c and "🔴" in c and "⚪" in c and "⬜" in c for c in captions)
    # 时间口径图例：上一 ISO 周 + yield 良率波动 period 制注明
    assert any("上一 ISO 周" in c and "2026-08-24" in c for c in captions)
    assert any("period 制" in c and "Yield 趋势波动" in c for c in captions)
    # 模块分组可视
    for group_label in ("AOI_RS", "AOI_TT", "SPC", "CTQ", "Yield", "Q-Time"):
        assert any(c == group_label for c in captions), group_label

    cell_buttons = [
        b for b in app.button if b.key and b.key.startswith("matrix_cell_")
    ]
    assert len(cell_buttons) == ROW_COUNT * len(PRODUCTS)

    by_key = {b.key: b for b in cell_buttons}
    assert by_key[matrix_cell_button_key("qtime_sheet_oos", "M678")].label == "🔴"
    assert by_key[matrix_cell_button_key("ctq_sheet_oos", "M678")].label == "⚪"
    assert by_key[matrix_cell_button_key("yield_lot_oos", "Z571")].label == "⬜"
    assert by_key[matrix_cell_button_key("aoi_rs_sheet_oos", "M678")].label == "🟢"


def test_error_cell_tooltip_carries_message() -> None:
    app = _new_app("render-tooltip").run()

    error_button = app.button(key=matrix_cell_button_key("yield_lot_oos", "Z571"))
    assert "加载失败" in error_button.help
    assert "修饰工作簿读取失败" in error_button.help
    no_data_button = app.button(key=matrix_cell_button_key("ctq_sheet_oos", "M678"))
    assert "修饰工作簿不存在" in no_data_button.help


def test_board_degrades_to_info_when_payload_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(**_kwargs):
        raise RuntimeError("签名采集异常")

    monkeypatch.setattr(alert_matrix, "get_cached_alert_matrix", _raise)
    app = _new_app("board-degrade")
    app.session_state["fixture_mode"] = "board"
    app.run()

    assert not app.exception
    assert any("预警矩阵暂时不可用" in m.value for m in app.info)


# ---------------------------------------------------------------------------
# Phase 5：点击详情懒加载
# ---------------------------------------------------------------------------
def test_no_selection_triggers_no_detail_computation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory_calls: list = []
    monkeypatch.setattr(
        alert_matrix_detail,
        "build_default_detail_loaders",
        lambda db_manager=None: factory_calls.append(1) or {},
    )
    app = _new_app("no-selection").run()

    assert not app.exception
    assert factory_calls == []
    assert not any("预警详情" in m.value for m in app.markdown)


def test_click_alert_cell_lazy_loads_qtime_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    loader_calls: list[tuple[str, date]] = []

    def fake_qtime_loader(prod_code: str, reference_date: date) -> dict:
        loader_calls.append((prod_code, reference_date))
        return _qtime_bundle()

    _inject_loaders(monkeypatch, {"qtime_sheet_oos": fake_qtime_loader})
    app = _new_app("click-qtime").run()
    assert loader_calls == []  # 未点击不产生详情计算

    app.button(key=matrix_cell_button_key("qtime_sheet_oos", "M678")).click().run()

    assert not app.exception
    assert app.session_state[MATRIX_SELECTION_STATE_KEY] == "qtime_sheet_oos|M678"
    # loader 以 (产品, 参考周周一=reference_week.end) 懒加载一次
    assert loader_calls == [("M678", date(2026, 8, 31))]
    assert any("预警详情｜M678 × Q-Time 单片异常" in m.value for m in app.markdown)
    assert any("Q-Time 超规预警中心" in e.label for e in app.expander)
    assert len(app.get("plotly_chart")) == 1
    # 明细表（预警中心 dataframe）
    assert len(app.dataframe) >= 1


def test_detail_cache_hit_when_reopening_same_cell(monkeypatch: pytest.MonkeyPatch) -> None:
    loader_calls: list = []

    def fake_qtime_loader(prod_code: str, reference_date: date) -> dict:
        loader_calls.append(prod_code)
        return _qtime_bundle()

    _inject_loaders(monkeypatch, {"qtime_sheet_oos": fake_qtime_loader})
    app = _new_app("cache-hit").run()
    app.button(key=matrix_cell_button_key("qtime_sheet_oos", "M678")).click().run()
    assert len(loader_calls) == 1
    assert len(app.get("plotly_chart")) == 1

    # 同一会话普通 rerun（相当于再次打开同单元格）：命中缓存不重算
    app.run()
    assert not app.exception
    assert len(loader_calls) == 1
    assert any("预警详情｜M678 × Q-Time 单片异常" in m.value for m in app.markdown)
    assert len(app.get("plotly_chart")) == 1


def test_click_ok_cell_shows_explanation_without_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory_calls: list = []
    monkeypatch.setattr(
        alert_matrix_detail,
        "build_default_detail_loaders",
        lambda db_manager=None: factory_calls.append(1) or {},
    )
    app = _new_app("click-ok").run()
    app.button(key=matrix_cell_button_key("aoi_rs_sheet_oos", "M678")).click().run()

    assert not app.exception
    assert app.session_state[MATRIX_SELECTION_STATE_KEY] == "aoi_rs_sheet_oos|M678"
    assert any("达标" in s.value for s in app.success)
    assert factory_calls == []


def test_click_error_cell_shows_message_without_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory_calls: list = []
    monkeypatch.setattr(
        alert_matrix_detail,
        "build_default_detail_loaders",
        lambda db_manager=None: factory_calls.append(1) or {},
    )
    app = _new_app("click-error").run()
    app.button(key=matrix_cell_button_key("yield_lot_oos", "Z571")).click().run()

    assert not app.exception
    assert any("修饰工作簿读取失败" in w.value for w in app.warning)
    assert factory_calls == []


def test_detail_loader_failure_degrades_without_breaking_matrix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raising_loader(prod_code: str, reference_date: date) -> dict:
        raise RuntimeError("数据库连接失败")

    _inject_loaders(monkeypatch, {"qtime_sheet_oos": raising_loader})
    app = _new_app("loader-error").run()
    app.button(key=matrix_cell_button_key("qtime_sheet_oos", "M678")).click().run()

    assert not app.exception
    assert any("预警详情加载失败" in e.value for e in app.error)
    # 矩阵本体不受影响
    cell_buttons = [
        b for b in app.button if b.key and b.key.startswith("matrix_cell_")
    ]
    assert len(cell_buttons) == ROW_COUNT * len(PRODUCTS)


def test_switching_selection_updates_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    loader_calls: list[str] = []

    def fake_qtime_loader(prod_code: str, reference_date: date) -> dict:
        loader_calls.append(prod_code)
        return _qtime_bundle()

    _inject_loaders(
        monkeypatch,
        {
            "qtime_sheet_oos": fake_qtime_loader,
            "spc_sheet_oos": lambda prod_code, reference_date: _spc_sheet_oos_bundle(),
        },
    )
    # spc 图像渲染替换为记录器，避免真实图表构建
    rendered: list[dict] = []
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: rendered.append(kwargs),
    )

    app = _new_app("switch-cell").run()
    app.button(key=matrix_cell_button_key("qtime_sheet_oos", "M678")).click().run()
    assert any("Q-Time 单片异常" in m.value for m in app.markdown)

    app.button(key=matrix_cell_button_key("spc_sheet_oos", "M678")).click().run()
    assert app.session_state[MATRIX_SELECTION_STATE_KEY] == "spc_sheet_oos|M678"
    assert any("预警详情｜M678 × SPC 单片异常" in m.value for m in app.markdown)
    assert loader_calls == ["M678"]  # qtime 只算过一次
    assert len(rendered) == 1


# ---------------------------------------------------------------------------
# 详情渲染管线接线（chart key 前缀 matrix_detail / memo 化）
# ---------------------------------------------------------------------------
def test_spc_sheet_oos_detail_wires_alert_table_and_charts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rendered: list[dict] = []
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: rendered.append(kwargs),
    )
    _inject_loaders(
        monkeypatch, {"spc_sheet_oos": lambda prod_code, reference_date: _spc_sheet_oos_bundle()}
    )
    app = _new_app("wiring-spc")
    app.session_state[MATRIX_SELECTION_STATE_KEY] = "spc_sheet_oos|M678"
    app.run()

    assert not app.exception
    # 预警明细表（render_sheet_oos_alert_center 真实渲染）
    assert any("检测到 1 条单片异常预警" in e.value for e in app.error)
    assert len(app.dataframe) >= 1
    assert any("自动预警指标图像（1 个指标）" in e.label for e in app.expander)
    # 图像管线：matrix_detail 前缀 + collect_memoized 签名
    assert len(rendered) == 1
    kwargs = rendered[0]
    assert kwargs["chart_key_prefix"] == "matrix_detail_spc_oos"
    assert kwargs["memo_state_key"] == "matrix_detail_spc_oos_charts_memo"
    assert kwargs["memo_signature"].startswith("matrix_detail|spc_sheet_oos|M678|")
    assert not kwargs["sheet_features_df"].empty


def test_spc_cpk_detail_wires_capability_alert_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rendered: list[dict] = []
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **kwargs: rendered.append(kwargs),
    )
    _inject_loaders(
        monkeypatch, {"spc_cpk_trend": lambda prod_code, reference_date: _spc_cpk_bundle()}
    )
    app = _new_app("wiring-cpk")
    app.session_state[MATRIX_SELECTION_STATE_KEY] = "spc_cpk_trend|M678"
    app.run()

    assert not app.exception
    assert any("CPK 预警明细" in e.label for e in app.expander)
    assert any("检测到 1 条 CPK 预警" in e.value for e in app.error)
    assert len(rendered) == 1
    kwargs = rendered[0]
    assert kwargs["chart_key_prefix"] == "matrix_detail_spc_cpk"
    assert kwargs["memo_state_key"] == "matrix_detail_cpk_charts_memo"
    assert kwargs["memo_signature"].startswith("matrix_detail|spc_cpk_trend|M678|")


def test_yield_trend_detail_wires_alert_code_expanders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rendered: list[dict] = []
    monkeypatch.setattr(
        yield_dashboard,
        "render_alert_code_expanders",
        lambda *args, **kwargs: rendered.append(kwargs),
    )
    bundle = _yield_trend_bundle()
    _inject_loaders(
        monkeypatch, {"yield_trend_fluctuation": lambda prod_code, reference_date: bundle}
    )
    app = _new_app("wiring-yield")
    app.session_state[MATRIX_SELECTION_STATE_KEY] = "yield_trend_fluctuation|M678"
    app.run()

    assert not app.exception
    assert any("良率波动预警明细" in e.label for e in app.expander)
    assert len(rendered) == 1
    kwargs = rendered[0]
    assert kwargs["trend_records"] == bundle["records"]
    assert kwargs["lot_oos_records"] is None
    assert kwargs["product_code"] == "M678"
    assert kwargs["chart_key_prefix"] == "matrix_detail_yield"
    assert kwargs["memo_state_key"] == "matrix_detail_yield_charts_memo"


# ---------------------------------------------------------------------------
# 缓存边界（不依赖 AppTest 的键语义）
# ---------------------------------------------------------------------------
def test_alert_matrix_cached_funcs_registered_for_page_refresh() -> None:
    """页头「刷新缓存」清理清单：矩阵 payload / 详情包 / qtime 监控必须可 clear。"""
    from app.sections.inline_domain.monitor.alert_matrix_cache import (
        get_alert_matrix_cached_funcs,
    )

    funcs = get_alert_matrix_cached_funcs()
    assert alert_matrix_detail._cached_matrix_detail_bundle in funcs
    assert all(hasattr(func, "clear") for func in funcs)


def test_detail_bundle_cache_key_semantics() -> None:
    alert_matrix_detail._cached_matrix_detail_bundle.clear()
    calls: list = []
    detail_key = f"qtime_sheet_oos|M678|{uuid.uuid4()}"

    def loader() -> dict:
        calls.append(1)
        return {"kind": "qtime", "alerts_df": pd.DataFrame(), "details_df": pd.DataFrame(), "total_lots": 0}

    kwargs = {
        "detail_key": detail_key,
        "reference_date": date(2026, 8, 31),
        "signature": "sig-a",
        "_loader": loader,
    }
    alert_matrix_detail.get_cached_matrix_detail(**kwargs)
    alert_matrix_detail.get_cached_matrix_detail(**kwargs)
    assert len(calls) == 1  # 同键命中

    alert_matrix_detail.get_cached_matrix_detail(**{**kwargs, "signature": "sig-b"})
    assert len(calls) == 2  # 签名变化触发重算
    alert_matrix_detail._cached_matrix_detail_bundle.clear()
