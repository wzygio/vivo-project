"""自动预警看板"产品 × 监控参数"矩阵数据服务（PRD §4.1，纯计算层）。

- 行清单与判据全部复用既有实现（PRD §3.1），本模块只做"记录集合 → 四态"映射；
- 单元格四态契约：ok（达标）/ alert（不达标）/ no_data（无数据）/ error（加载失败）；
  alert 单元格附带 ``alert_factories``（预警记录涉及的厂别，排序去重），
  供矩阵厂别筛选做客户端切片；无厂别信息的行（yield 两行）在行声明上以
  ``supports_factory_filter=False`` 标记；
- 时间窗统一为上一 ISO 周（半开 [上周一, 本周一)）；yield 趋势波动为例外
  （period 制口径，探测记录非空即 alert，与既有看板一致）；
- 只读：sheet OOS 行经 ``load_sheet_oos_decoration`` 只读加载，绝不调用
  prepare_* 写盘路径；yield 侧经 read_only 入口（见 yield_service）；
- 任一 (行, 产品) 异常被捕获并降级为 error 态，不阻断其他单元格。

本模块不 import streamlit；缓存包装见 ``alert_matrix_cache.py``。
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.components.alert_center import compute_lot_oos_records
from app.sections.inline_domain.spc.spc_dashboard import (
    CPK_ALERT_THRESHOLD,
    build_weekly_cpk_alerts,
)
from src.inline_domain.application.shared.decorated_data import (
    SCOPE_DECORATION_FILE_NAME,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import AOI_RS_OOS_KEY_COLUMNS
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import AOI_TT_OOS_KEY_COLUMNS
from src.inline_domain.core.shared.sheet_oos_alerts import (
    build_sheet_oos_alerts,
    previous_iso_week_range,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    SheetOosDecorationReadError,
    load_sheet_oos_decoration,
)
from src.shared_kernel.config import ConfigLoader
from yield_domain.application.alert_service import AlertService

logger = logging.getLogger(__name__)

CELL_STATE_OK = "ok"
CELL_STATE_ALERT = "alert"
CELL_STATE_NO_DATA = "no_data"
CELL_STATE_ERROR = "error"
VALID_CELL_STATES = frozenset(
    {CELL_STATE_OK, CELL_STATE_ALERT, CELL_STATE_NO_DATA, CELL_STATE_ERROR}
)

_PREVIOUS_WEEK_SCOPE = "上一 ISO 周"

_UNSET = object()


@dataclass
class AlertMatrixContext:
    """矩阵计算的依赖注入容器（测试可全部替换为 fake）。

    - ``inline_resource_dir``：4 个 sheet OOS 修饰工作簿所在目录；
      None 时按 ``ConfigLoader.get_domain_resource_dir("inline_domain")`` 解析；
    - ``spc_cpk_loader``：(prod_code) -> period_capability_df | None；
    - ``yield_lot_loader``：(prod_code) -> (lot_data, warning_lines) | None；
    - ``yield_trend_loader``：(prod_code) -> (mwd_group_data, mwd_code_data) | None；
    - ``qtime_monitoring_loader``：() -> (details_df, alerts_df)，全产品一次拉取，
      首次调用后在本次构建内 memoize（含异常），避免逐产品重复打库。
    """

    reference_date: date
    inline_resource_dir: Path | None = None
    spc_cpk_loader: Callable[[str], pd.DataFrame | None] | None = None
    yield_lot_loader: Callable[[str], tuple[Any, Mapping[str, Any]] | None] | None = None
    yield_trend_loader: Callable[[str], tuple[Any, Any] | None] | None = None
    qtime_monitoring_loader: Callable[[], tuple[pd.DataFrame, pd.DataFrame]] | None = None
    _qtime_monitoring_memo: Any = field(default=_UNSET, repr=False)

    def get_qtime_monitoring(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """全产品 Q-Time (details, alerts)，本次构建内只拉取一次；异常同样被记住并重抛。"""
        if self._qtime_monitoring_memo is _UNSET:
            try:
                loader = self.qtime_monitoring_loader
                self._qtime_monitoring_memo = ("ok", loader() if loader else None)
            except Exception as exc:  # noqa: BLE001 - 异常入 memo，由各单元格统一降级
                self._qtime_monitoring_memo = ("error", exc)
        status, payload = self._qtime_monitoring_memo
        if status == "error":
            raise payload
        return payload


@dataclass(frozen=True)
class AlertMatrixRow:
    """矩阵行声明：显示信息 + 单元格 evaluator（(prod_code, context) -> cell dict）。

    ``supports_factory_filter``：该行预警记录是否携带厂别信息（矩阵厂别筛选做
    客户端单元格切片）；False 的行（yield 两行）在厂别筛选时保持原状态。
    """

    row_key: str
    display_name: str
    module_group: str
    time_scope: str
    evaluator: Callable[[str, AlertMatrixContext], dict[str, Any]]
    supports_factory_filter: bool = True


def _cell(
    row_key: str,
    prod_code: str,
    state: str,
    message: str = "",
    alert_factories: Iterable[str] = (),
) -> dict[str, Any]:
    return {
        "state": state,
        "detail_key": f"{row_key}|{prod_code}",
        "message": message,
        # 有预警记录涉及的厂别列表（排序去重）；非 alert 态或无厂别信息时为空
        "alert_factories": list(alert_factories),
    }


def _extract_factories(frame: pd.DataFrame | None, column: str) -> tuple[str, ...]:
    """从预警记录帧提取厂别列表：排序去重、大小写归一；缺列/空帧返回空。"""
    if frame is None or frame.empty or column not in frame.columns:
        return ()
    values = {str(value).strip().upper() for value in frame[column].dropna()}
    return tuple(sorted(value for value in values if value))


def _error_message(exc: Exception) -> str:
    """用户可读的简短原因：不泄露堆栈，已知读取失败给固定文案。"""
    if isinstance(exc, SheetOosDecorationReadError):
        return "修饰工作簿读取失败，请确认文件可正常打开且未被锁定"
    text = str(exc).strip()
    if text:
        return text[:200]
    return f"加载失败（{type(exc).__name__}）"


def _alerts_cell(
    row_key: str,
    prod_code: str,
    has_alerts: bool,
    alert_factories: Iterable[str] = (),
) -> dict[str, Any]:
    return _cell(
        row_key,
        prod_code,
        CELL_STATE_ALERT if has_alerts else CELL_STATE_OK,
        alert_factories=alert_factories if has_alerts else (),
    )


# ---------------------------------------------------------------------------
# sheet OOS 行（aoi_rs / aoi_tt / spc / ctq）：flag=FALSE 且时间 ∈ 上一 ISO 周
# ---------------------------------------------------------------------------
def _sheet_oos_evaluator(
    *,
    row_key: str,
    scope: str,
    time_column: str,
    key_columns: Iterable[str] | None = None,
) -> Callable[[str, AlertMatrixContext], dict[str, str]]:
    def evaluate(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
        resource_dir = (
            Path(context.inline_resource_dir)
            if context.inline_resource_dir is not None
            else ConfigLoader.get_domain_resource_dir("inline_domain")
        )
        file_name = SCOPE_DECORATION_FILE_NAME[scope]
        if not (resource_dir / file_name).exists():
            return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "修饰工作簿不存在")
        decoration_df = load_sheet_oos_decoration(
            resource_dir,
            file_name=file_name,
            sheet_name=prod_code,
            key_columns=key_columns,
        )
        if decoration_df.empty or "flag" not in decoration_df.columns:
            return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无该产品修饰数据")
        if time_column not in decoration_df.columns:
            return _cell(
                row_key, prod_code, CELL_STATE_NO_DATA, f"缺少时间列 {time_column}"
            )
        alerts_df = build_sheet_oos_alerts(
            decoration_df,
            time_column=time_column,
            reference_date=context.reference_date,
        )
        return _alerts_cell(
            row_key,
            prod_code,
            not alerts_df.empty,
            _extract_factories(alerts_df, "factory"),
        )

    return evaluate


# ---------------------------------------------------------------------------
# spc 趋势波动行：既有 CPK 周度预警（cpk < 1.33 且未被修饰）
# ---------------------------------------------------------------------------
def _evaluate_spc_cpk_trend(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
    row_key = "spc_cpk_trend"
    if context.spc_cpk_loader is None:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "未配置 CPK 数据源")
    capability_df = context.spc_cpk_loader(prod_code)
    if capability_df is None or capability_df.empty:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无周期能力数据")
    alerts_df = build_weekly_cpk_alerts(
        capability_df,
        threshold=CPK_ALERT_THRESHOLD,
        reference_date=context.reference_date,
    )
    # CPK 预警记录为中文列（厂别/站点/参数名称/超规周次/CPK值）
    return _alerts_cell(
        row_key, prod_code, not alerts_df.empty, _extract_factories(alerts_df, "厂别")
    )


# ---------------------------------------------------------------------------
# yield 单片异常行：既有 lot 超规判据 + 呈现层过滤到上一 ISO 周
# ---------------------------------------------------------------------------
def _parse_display_date(value: object) -> pd.Timestamp:
    return pd.to_datetime(str(value), format="%Y/%m/%d", errors="coerce")


def _evaluate_yield_lot_oos(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
    row_key = "yield_lot_oos"
    if context.yield_lot_loader is None:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "未配置 Yield 数据源")
    bundle = context.yield_lot_loader(prod_code)
    if not bundle:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无 Lot 良率数据")
    lot_data, warning_lines = bundle
    oos_records, _ = compute_lot_oos_records(lot_data, warning_lines)
    start, end = previous_iso_week_range(context.reference_date)
    weekly_records = [
        record
        for record in oos_records
        if start <= _parse_display_date(record.get("入库时间")) < end
    ]
    return _alerts_cell(row_key, prod_code, bool(weekly_records))


# ---------------------------------------------------------------------------
# yield 趋势波动行：既有结构化探测记录非空即 alert（period 制，无 ISO 周过滤）
# ---------------------------------------------------------------------------
def _evaluate_yield_trend(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
    row_key = "yield_trend_fluctuation"
    if context.yield_trend_loader is None:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "未配置 Yield 数据源")
    bundle = context.yield_trend_loader(prod_code)
    if not bundle:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无趋势数据")
    mwd_group_data, mwd_code_data = bundle
    if not mwd_group_data and not mwd_code_data:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无趋势数据")
    records = AlertService.get_dashboard_alert_records(
        mwd_group_data or {},
        mwd_code_data or {},
    )
    return _alerts_cell(row_key, prod_code, bool(records))


# ---------------------------------------------------------------------------
# qtime 单片异常行：全产品一次拉取，按 prodcode 拆分 + timekey 过滤上一 ISO 周
# ---------------------------------------------------------------------------
def _parse_qtime_timekey(values: pd.Series) -> pd.Series:
    return pd.to_datetime(
        values.astype(str).str.slice(0, 14),
        format="%Y%m%d%H%M%S",
        errors="coerce",
    )


def _evaluate_qtime_sheet_oos(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
    row_key = "qtime_sheet_oos"
    if context.qtime_monitoring_loader is None:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "未配置 Q-Time 数据源")
    monitoring = context.get_qtime_monitoring()
    if monitoring is None:
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无 Q-Time 数据")
    details_df, alerts_df = monitoring

    def _has_product(frame: pd.DataFrame) -> bool:
        return (
            isinstance(frame, pd.DataFrame)
            and not frame.empty
            and "prodcode" in frame.columns
            and bool((frame["prodcode"].astype(str) == str(prod_code)).any())
        )

    if not _has_product(details_df) and not _has_product(alerts_df):
        return _cell(row_key, prod_code, CELL_STATE_NO_DATA, "无该产品 Q-Time 数据")
    if alerts_df.empty or "timekey" not in alerts_df.columns:
        return _cell(row_key, prod_code, CELL_STATE_OK)
    start, end = previous_iso_week_range(context.reference_date)
    prod_alerts = alerts_df.loc[alerts_df["prodcode"].astype(str) == str(prod_code)]
    if prod_alerts.empty:
        return _cell(row_key, prod_code, CELL_STATE_OK)
    times = _parse_qtime_timekey(prod_alerts["timekey"])
    weekly_mask = (times >= start) & (times < end)
    weekly_alerts = prod_alerts.loc[weekly_mask]
    # shop 打标列由 load_all_product_qtime_monitoring 在 union 时写入（shop 即厂别）
    return _alerts_cell(
        row_key,
        prod_code,
        not weekly_alerts.empty,
        _extract_factories(weekly_alerts, "shop"),
    )


def _guarded_evaluator(
    row_key: str,
    evaluator: Callable[[str, AlertMatrixContext], dict[str, Any]],
) -> Callable[[str, AlertMatrixContext], dict[str, Any]]:
    """给 evaluator 装上单元格级降级：任何异常 → error 态，非法状态 → error 态。"""

    def evaluate(prod_code: str, context: AlertMatrixContext) -> dict[str, str]:
        try:
            cell = evaluator(prod_code, context)
        except Exception as exc:  # noqa: BLE001 - 单元格级降级是契约要求
            logger.warning(
                "[alert-matrix] cell %s 求值失败: %s",
                f"{row_key}|{prod_code}",
                exc,
                exc_info=True,
            )
            return _cell(row_key, prod_code, CELL_STATE_ERROR, _error_message(exc))
        if cell.get("state") not in VALID_CELL_STATES:
            return _cell(
                row_key,
                prod_code,
                CELL_STATE_ERROR,
                f"evaluator 返回非法状态: {cell.get('state')!r}",
            )
        return cell

    return evaluate


# ---------------------------------------------------------------------------
# 行注册表（PRD §3.1 顺序）
# ---------------------------------------------------------------------------
MATRIX_ROWS: tuple[AlertMatrixRow, ...] = (
    AlertMatrixRow(
        row_key="aoi_rs_sheet_oos",
        display_name="AOI_RS 单片异常",
        module_group="aoi_rs",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator(
            "aoi_rs_sheet_oos",
            _sheet_oos_evaluator(
                row_key="aoi_rs_sheet_oos",
                scope="aoi_rs",
                time_column="sheet_start_time",
                key_columns=AOI_RS_OOS_KEY_COLUMNS,
            ),
        ),
    ),
    AlertMatrixRow(
        row_key="aoi_tt_sheet_oos",
        display_name="AOI_TT 单片异常",
        module_group="aoi_tt",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator(
            "aoi_tt_sheet_oos",
            _sheet_oos_evaluator(
                row_key="aoi_tt_sheet_oos",
                scope="aoi_tt",
                time_column="start_time",
                key_columns=AOI_TT_OOS_KEY_COLUMNS,
            ),
        ),
    ),
    AlertMatrixRow(
        row_key="spc_sheet_oos",
        display_name="SPC 单片异常",
        module_group="spc",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator(
            "spc_sheet_oos",
            _sheet_oos_evaluator(
                row_key="spc_sheet_oos",
                scope="spc",
                time_column="sheet_start_time",
            ),
        ),
    ),
    AlertMatrixRow(
        row_key="spc_cpk_trend",
        display_name="SPC 趋势波动（CPK）",
        module_group="spc",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator("spc_cpk_trend", _evaluate_spc_cpk_trend),
    ),
    AlertMatrixRow(
        row_key="ctq_sheet_oos",
        display_name="CTQ 单片异常",
        module_group="ctq",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator(
            "ctq_sheet_oos",
            _sheet_oos_evaluator(
                row_key="ctq_sheet_oos",
                scope="ctq",
                time_column="sheet_start_time",
            ),
        ),
    ),
    AlertMatrixRow(
        row_key="yield_lot_oos",
        display_name="Yield 单片异常（Lot 超规）",
        module_group="yield",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator("yield_lot_oos", _evaluate_yield_lot_oos),
        # compute_lot_oos_records 记录无厂别列 → 不支持厂别细分
        supports_factory_filter=False,
    ),
    AlertMatrixRow(
        row_key="yield_trend_fluctuation",
        display_name="Yield 趋势波动",
        module_group="yield",
        time_scope="月/周环比（period 制）",
        evaluator=_guarded_evaluator("yield_trend_fluctuation", _evaluate_yield_trend),
        # get_dashboard_alert_records 记录无厂别列 → 不支持厂别细分
        supports_factory_filter=False,
    ),
    AlertMatrixRow(
        row_key="qtime_sheet_oos",
        display_name="Q-Time 单片异常",
        module_group="qtime",
        time_scope=_PREVIOUS_WEEK_SCOPE,
        evaluator=_guarded_evaluator("qtime_sheet_oos", _evaluate_qtime_sheet_oos),
    ),
)

MATRIX_ROW_MAP: dict[str, AlertMatrixRow] = {row.row_key: row for row in MATRIX_ROWS}


# ---------------------------------------------------------------------------
# payload 构建
# ---------------------------------------------------------------------------
def _evaluate_cell(
    row: AlertMatrixRow,
    prod_code: str,
    context: AlertMatrixContext,
) -> dict[str, Any]:
    """单 (行, 产品) 求值；evaluator 已带降级守卫（见 _guarded_evaluator）。"""
    return row.evaluator(prod_code, context)


def build_alert_matrix_payload(
    *,
    reference_date: date | pd.Timestamp | None = None,
    products: Sequence[str] | None = None,
    context: AlertMatrixContext | None = None,
    signature: str = "",
) -> dict[str, Any]:
    """构建矩阵 payload（PRD §4.1 schema）。纯计算，无 st.* 调用。

    ``context`` 提供全部外部依赖；缺省时仅按 ``reference_date`` 构造空上下文
    （sheet OOS 行走默认资源目录，其余行无数据源 → no_data）。
    context 提供时以其 reference_date 为准。
    """
    if context is None:
        reference = pd.Timestamp(reference_date or date.today()).date()
        context = AlertMatrixContext(reference_date=reference)
    reference = context.reference_date
    product_list = list(products) if products is not None else ConfigLoader.get_enabled_products()

    week_start, week_end = previous_iso_week_range(reference)
    iso = week_start.isocalendar()

    cells: dict[tuple[str, str], dict[str, Any]] = {}
    for row in MATRIX_ROWS:
        for prod_code in product_list:
            cells[(row.row_key, prod_code)] = _evaluate_cell(row, prod_code, context)

    return {
        "products": product_list,
        "rows": [
            {
                "row_key": row.row_key,
                "display_name": row.display_name,
                "module_group": row.module_group,
                "time_scope": row.time_scope,
                "factory_filter_supported": row.supports_factory_filter,
            }
            for row in MATRIX_ROWS
        ],
        "cells": cells,
        "signature": signature,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "reference_week": {
            "label": f"{iso.year}-W{iso.week:02d}",
            "start": week_start.date().isoformat(),
            "end": week_end.date().isoformat(),
        },
    }


def build_alert_matrix_signature(
    *,
    products: Sequence[str],
    components: Mapping[str, Any] | None = None,
) -> str:
    """矩阵缓存签名：产品集合 + 各域签名分量的确定性摘要。

    ``components`` 缺省时由缓存装配层采集（逐产品 revision、逐 (prod, scope)
    决策签名、qtime 决策文件 stat）；测试应显式注入。
    """
    product_list = list(products)
    if components is None:
        from app.sections.inline_domain.monitor.alert_matrix_cache import (
            build_default_signature_components,
        )

        components = build_default_signature_components(product_list)
    payload_text = json.dumps(
        {"products": product_list, "components": components},
        sort_keys=True,
        default=str,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
