"""自动预警矩阵集成测试（计划 Phase 3.4）。

多产品模拟数据（tmp 工作簿 + fake yield/qtime 数据源）下，矩阵单元格状态
与直接调用各单域判据（build_sheet_oos_alerts / build_weekly_cpk_alerts /
compute_lot_oos_records / get_dashboard_alert_records / qtime timekey 过滤）
的结果一致；且矩阵构建全程只读（工作簿字节与 mtime 不变）。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from app.components.alert_center import compute_lot_oos_records
from app.sections.inline_domain.monitor.alert_matrix_service import (
    AlertMatrixContext,
    build_alert_matrix_payload,
)
from app.sections.inline_domain.spc.spc_dashboard import build_weekly_cpk_alerts
from src.inline_domain.application.shared.decorated_data import (
    SCOPE_DECORATION_FILE_NAME,
)
from src.inline_domain.core.shared.sheet_oos_alerts import (
    build_sheet_oos_alerts,
    previous_iso_week_range,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    load_sheet_oos_decoration,
)
from yield_domain.application.alert_service import AlertService

REFERENCE_DATE = date(2026, 9, 2)  # 上一 ISO 周 = [2026-08-24, 2026-08-31)
PRODUCTS = ["M678", "Z571"]
PREV_WEEK_LABEL = "2026-W35"

WARNING_LINES = {"OLED_Mura": {"upper": 0.10, "lower": 0.0}}


def _write_scope_workbook(
    resource_dir: Path, scope: str, rows_by_sheet: dict[str, list[dict]]
) -> Path:
    resource_dir.mkdir(parents=True, exist_ok=True)
    workbook_path = resource_dir / SCOPE_DECORATION_FILE_NAME[scope]
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        for sheet_name, rows in rows_by_sheet.items():
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    return workbook_path


def _spc_decoration_row(prod: str, sheet_id: str, flag: object, start_time: str) -> dict:
    return {
        "factory": "OLED",
        "prod_code": prod,
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


def _capability_df(prod: str, cpk: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "OLED",
                "step_id": "21200",
                "param_name": f"CD_X_{prod}",
                "period_type": "week",
                "period_label": PREV_WEEK_LABEL,
                "cpk": cpk,
                "cpk_decorated": False,
            }
        ]
    )


def _lot_data(*warehousing_times: str) -> dict:
    return {
        "code_level_details": {
            "OLED_Mura": pd.DataFrame(
                [
                    {
                        "lot_id": f"LOT{i}",
                        "defect_desc": "OLED_Mura",
                        "defect_rate": 0.5,
                        "defect_panel_count": 3,
                        "warehousing_time": w_time,
                        "array_input_time": w_time,
                    }
                    for i, w_time in enumerate(warehousing_times)
                ]
            )
        }
    }


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


def _build_simulated_context(resource_dir: Path) -> AlertMatrixContext:
    cpk_by_prod = {"M678": _capability_df("M678", 1.10), "Z571": _capability_df("Z571", 1.50)}
    lot_by_prod = {
        "M678": (_lot_data("20260826"), WARNING_LINES),  # 上周超规
        "Z571": (_lot_data("20260901"), WARNING_LINES),  # 仅本周超规
    }
    trend_by_prod = {"M678": _trend_data(spike=True), "Z571": _trend_data(spike=False)}
    qtime_frames = (
        pd.DataFrame(
            [
                {"prodcode": "M678", "timekey": "20260826100000"},
                {"prodcode": "Z571", "timekey": "20260826100000"},
            ]
        ),
        pd.DataFrame(
            [
                _qtime_alert("M678", "20260826100000"),  # 上周
                _qtime_alert("Z571", "20260901100000"),  # 本周
            ]
        ),
    )
    return AlertMatrixContext(
        reference_date=REFERENCE_DATE,
        inline_resource_dir=resource_dir,
        spc_cpk_loader=lambda prod: cpk_by_prod.get(prod),
        yield_lot_loader=lambda prod: lot_by_prod.get(prod),
        yield_trend_loader=lambda prod: trend_by_prod.get(prod),
        qtime_monitoring_loader=lambda: qtime_frames,
    )


def test_matrix_cells_match_per_domain_criteria(tmp_path: Path) -> None:
    """矩阵单元格状态与直接调用各单域判据的结果一致。"""
    resource_dir = tmp_path / "inline_resources"
    workbook = _write_scope_workbook(
        resource_dir,
        "spc",
        {
            "M678": [_spc_decoration_row("M678", "S1", False, "2026-08-26 10:00:00")],
            "Z571": [_spc_decoration_row("Z571", "S1", True, "2026-08-26 10:00:00")],
        },
    )
    bytes_before = workbook.read_bytes()
    mtime_before = workbook.stat().st_mtime_ns

    context = _build_simulated_context(resource_dir)
    payload = build_alert_matrix_payload(products=PRODUCTS, context=context)
    cells = payload["cells"]

    # --- spc 单片异常：直接判据 = load + build_sheet_oos_alerts 非空 ---
    for prod in PRODUCTS:
        decoration_df = load_sheet_oos_decoration(
            resource_dir, file_name=SCOPE_DECORATION_FILE_NAME["spc"], sheet_name=prod
        )
        expected = (
            "alert"
            if not build_sheet_oos_alerts(
                decoration_df,
                time_column="sheet_start_time",
                reference_date=REFERENCE_DATE,
            ).empty
            else "ok"
        )
        assert cells[("spc_sheet_oos", prod)]["state"] == expected, prod

    # --- spc 趋势波动：直接判据 = build_weekly_cpk_alerts 非空 ---
    cpk_by_prod = {"M678": _capability_df("M678", 1.10), "Z571": _capability_df("Z571", 1.50)}
    for prod, capability_df in cpk_by_prod.items():
        expected = (
            "alert"
            if not build_weekly_cpk_alerts(
                capability_df, reference_date=REFERENCE_DATE
            ).empty
            else "ok"
        )
        assert cells[("spc_cpk_trend", prod)]["state"] == expected, prod

    # --- yield 单片异常：直接判据 = compute_lot_oos_records 过滤上一 ISO 周 ---
    start, end = previous_iso_week_range(REFERENCE_DATE)
    lot_by_prod = {"M678": _lot_data("20260826"), "Z571": _lot_data("20260901")}
    for prod, lot_data in lot_by_prod.items():
        records, _ = compute_lot_oos_records(lot_data, WARNING_LINES)
        weekly = [
            r
            for r in records
            if start <= pd.to_datetime(r["入库时间"], format="%Y/%m/%d") < end
        ]
        expected = "alert" if weekly else "ok"
        assert cells[("yield_lot_oos", prod)]["state"] == expected, prod

    # --- yield 趋势波动：直接判据 = get_dashboard_alert_records 非空 ---
    trend_by_prod = {"M678": _trend_data(spike=True), "Z571": _trend_data(spike=False)}
    for prod, (group_data, code_data) in trend_by_prod.items():
        expected = (
            "alert"
            if AlertService.get_dashboard_alert_records(group_data, code_data)
            else "ok"
        )
        assert cells[("yield_trend_fluctuation", prod)]["state"] == expected, prod

    # --- qtime 单片异常：prodcode 拆分 + timekey 上一 ISO 周过滤 ---
    assert cells[("qtime_sheet_oos", "M678")]["state"] == "alert"
    assert cells[("qtime_sheet_oos", "Z571")]["state"] == "ok"

    # --- 矩阵构建全程只读：工作簿字节与 mtime 不变 ---
    assert workbook.read_bytes() == bytes_before
    assert workbook.stat().st_mtime_ns == mtime_before


def test_matrix_degrades_missing_product_without_blocking_others(tmp_path: Path) -> None:
    """整列降级：无任何数据的产品各单元格 no_data，其余产品列不受影响。"""
    resource_dir = tmp_path / "inline_resources"
    _write_scope_workbook(
        resource_dir,
        "spc",
        {"M678": [_spc_decoration_row("M678", "S1", False, "2026-08-26 10:00:00")]},
    )
    context = AlertMatrixContext(
        reference_date=REFERENCE_DATE,
        inline_resource_dir=resource_dir,
    )

    payload = build_alert_matrix_payload(products=PRODUCTS, context=context)
    cells = payload["cells"]

    assert cells[("spc_sheet_oos", "M678")]["state"] == "alert"
    for row in payload["rows"]:
        assert cells[(row["row_key"], "Z571")]["state"] in {"no_data", "ok"}, row["row_key"]
    assert cells[("spc_sheet_oos", "Z571")]["state"] == "no_data"
    assert len(cells) == len(payload["rows"]) * len(PRODUCTS)
