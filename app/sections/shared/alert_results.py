"""One file-only alert consumer for report pages and the automatic alert matrix.

Only native payloads are cached. Reading a result never starts a report producer,
loads raw measurement snapshots, updates decisions, or queries a database.
"""
from __future__ import annotations

from datetime import date
import hashlib
import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from src.shared_kernel.config import ConfigLoader
from app.components.indicator_cache import get_indicator_product_revision
from app.sections.shared import published_alert_snapshots as snapshots

logger = logging.getLogger(__name__)
UNAVAILABLE = "预警结果暂不可用，请更新对应报表后重试。"
INLINE_SCOPES = ("spc", "ctq", "aoi_rs", "aoi_tt")
SHEET_ROWS = tuple(f"{scope}_sheet_oos" for scope in INLINE_SCOPES)
CAPABILITY_ROWS = ("spc_cpk_trend", "spc_cpm_trend")
TREND_ROWS = ("aoi_rs_trend", "aoi_tt_trend")
ALL_ROWS = (*SHEET_ROWS, *CAPABILITY_ROWS, *TREND_ROWS,
            "yield_lot_oos", "yield_trend_fluctuation", "qtime_sheet_oos",
            *snapshots.PUBLISHED_ALERT_ROWS)

_COMMON_MAP = {"factory": "厂别", "step_id": "站点", "param_name": "参数名称",
               "sheet_id": "Sheet ID", "sheet_start_time": "超规时间", "oos_type": "超规类型"}
_SHEET_MAPS = {
    "spc": _COMMON_MAP, "ctq": _COMMON_MAP,
    "aoi_tt": {"factory": "厂别", "step_id": "站点", "tt_name": "TT名称",
               "sheet_id": "Sheet ID", "start_time": "超规时间", "tt_qty": "TT数量", "usl": "规格上限"},
    "aoi_rs": {"factory": "厂别", "step_id": "站点", "rs_code": "RS Code",
               "chart_kind": "图类型", "point_id": "点位ID", "sheet_start_time": "超规时间",
               "value": "实测值", "spec": "规格上限"},
}


def _result(state: str, records: pd.DataFrame | None = None, *,
            alerts: pd.DataFrame | None = None, factory_column: str = "factory",
            message: str = "", metadata: dict | None = None) -> dict:
    frame = records.copy() if records is not None else pd.DataFrame()
    factories = sorted({str(value).strip().upper() for value in frame.get(
        factory_column, pd.Series(dtype=str)).dropna() if str(value).strip()})
    return {"state": state, "records": frame, "alerts": frame.copy() if alerts is None else alerts,
            "alert_factories": factories, "message": message, "metadata": metadata or {}}


def _file_version(path: Path) -> str:
    try:
        stat = path.stat()
        return f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
    except FileNotFoundError:
        return f"{path.resolve()}:missing"


def alert_result_signature(row_key: str, product: str, reference_date: date, **selection) -> str:
    """Policy/file/time validity is checked before consulting cached selection."""
    if row_key in SHEET_ROWS:
        from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
        from src.inline_domain.core.monitor.monitor_param_classifier import PARAM_CLASSIFICATION_SIGNATURE
        scope = row_key.removesuffix("_sheet_oos")
        parts = [_file_version(scope_decoration_path(scope)), PARAM_CLASSIFICATION_SIGNATURE,
                 repr(ConfigLoader.get_inline_data_exclusion())]
        if scope == "spc":
            parts.append(repr(ConfigLoader.get_spc_sheet_oos_alert_param_exclusions()))
    elif row_key in CAPABILITY_ROWS:
        from src.inline_domain.composition import build_capability_latest_reader
        parts = [build_capability_latest_reader(row_key.split("_")[1]).source_signature(),
                 repr(ConfigLoader.get_inline_data_exclusion()),
                 str(ConfigLoader.get_spc_capability_alert_min_span_hours())]
    elif row_key in snapshots.PUBLISHED_ALERT_ROWS:
        parts = [snapshots.published_alert_signature(row_key, product, reference_date)]
    elif row_key.startswith("yield_"):
        from src.yield_domain.composition import build_yield_alert_results
        kind = "lot" if row_key == "yield_lot_oos" else "trend"
        parts = [build_yield_alert_results().source_signature(product, kind, reference_date, **selection)]
    elif row_key == "qtime_sheet_oos":
        from src.indicator_domain.composition import qtime_alert_result_signature
        parts = [qtime_alert_result_signature(product, reference_date, **selection)]
    elif row_key in TREND_ROWS:
        from src.inline_domain.composition import build_trend_alert_results
        parts = [build_trend_alert_results(row_key.removesuffix("_trend")).source_signature(product, reference_date)]
    else:
        raise ValueError(f"Unknown alert row: {row_key}")
    return hashlib.sha256(repr(("saved-alert-v1", row_key, product, str(reference_date), parts, sorted(selection.items()))).encode()).hexdigest()


def _read_inline(row_key: str, product: str, reference_date: date) -> dict:
    from src.inline_domain.application.monitor.excel_alarm_reader import ExcelAlarmReader
    from src.inline_domain.infrastructure.monitor.excel_alarm_store import ExcelAlarmStore
    from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
    from src.inline_domain.core.shared.sheet_oos_alerts import build_sheet_oos_alerts

    scope = row_key.removesuffix("_sheet_oos")
    reader = ExcelAlarmReader(ExcelAlarmStore({scope: scope_decoration_path(scope)}))
    detail = reader.read_detail(scope, product)
    mapping = _SHEET_MAPS[scope]
    empty_display = pd.DataFrame(columns=list(mapping.values()))
    if detail is None:
        return _result("no_data", alerts=empty_display, message="当前产品暂无预警记录。")
    time_column = "start_time" if scope == "aoi_tt" else "sheet_start_time"
    rows = build_sheet_oos_alerts(
        detail, time_column=time_column, reference_date=reference_date,
        excluded_param_name_contains=ConfigLoader.get_spc_sheet_oos_alert_param_exclusions() if scope == "spc" else (),
    )
    display = rows.rename(columns=mapping).reindex(columns=list(mapping.values())).copy()
    if not display.empty:
        display["超规时间"] = display["超规时间"].astype(str)
        if scope == "aoi_rs":
            display["图类型"] = display["图类型"].replace({"lot": "By Lot", "sheet": "By Sheet"})
    return _result("alert" if len(rows) else "ok", rows, alerts=display)


def _read_capability(row_key: str, product: str, reference_date: date) -> dict:
    from src.inline_domain.composition import build_capability_latest_reader
    from src.inline_domain.core.monitor.cpk_latest import latest_capability_alerts
    from src.inline_domain.core.shared.date_exclusion import exclude_inline_period_records
    from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range

    metric = row_key.split("_")[1]
    frame = build_capability_latest_reader(metric).read_product(product)
    columns = {"factory": "厂别", "step_id": "站点", "param_name": "参数名称",
               "period_label": "超规周次", metric: f"{metric.upper()}值"}
    if frame is None or frame.empty:
        return _result("no_data", alerts=pd.DataFrame(columns=list(columns.values())))
    frame = exclude_inline_period_records(frame, ConfigLoader.get_inline_data_exclusion(), as_of=pd.Timestamp(reference_date))
    iso = previous_iso_week_range(reference_date)[0].isocalendar()
    weekly = frame.loc[frame.period_type.eq("week") & frame.period_label.eq(f"{iso.year}-W{iso.week:02d}")]
    rows = latest_capability_alerts(frame, reference_date, metric=metric,
        date_exclusion=ConfigLoader.get_inline_data_exclusion(),
        min_span_hours=ConfigLoader.get_spc_capability_alert_min_span_hours())
    state = "no_data" if weekly.empty or weekly["status"].isin(["无法判定", "数据跨度不足"]).all() else "ok"
    return _result("alert" if len(rows) else state, rows, alerts=rows.rename(columns=columns).reindex(columns=list(columns.values())))


def _read_uncached(row_key: str, product: str, reference_date: date, selection: dict) -> dict:
    if row_key in SHEET_ROWS:
        return _read_inline(row_key, product, reference_date)
    if row_key in CAPABILITY_ROWS:
        return _read_capability(row_key, product, reference_date)
    if row_key in snapshots.PUBLISHED_ALERT_ROWS:
        result = snapshots.read_published_alerts(row_key, product, reference_date)
        return {**result, "records": result["alerts"].copy(), "metadata": {}}
    if row_key.startswith("yield_"):
        from src.yield_domain.composition import build_yield_alert_results
        from src.yield_domain.application.alert_results import RESULT_COLUMNS
        kind = "lot" if row_key == "yield_lot_oos" else "trend"
        result = build_yield_alert_results().read(product, kind, reference_date, **selection)
        if result.status not in ("ready", "no_data"):
            return _result("error", message=UNAVAILABLE, metadata=result.metadata)
        state = "no_data" if result.status == "no_data" else "alert" if len(result.records) else "ok"
        return _result(state, result.records.reindex(columns=RESULT_COLUMNS[kind]), metadata=result.metadata)
    if row_key == "qtime_sheet_oos":
        from src.indicator_domain.composition import read_qtime_alert_result
        result = read_qtime_alert_result(product, reference_date, **selection)
        return {**result, "records": result.get("records", result.get("alerts", pd.DataFrame())),
                "alerts": result.get("alerts", result.get("records", pd.DataFrame()))}
    if row_key in TREND_ROWS:
        from src.inline_domain.composition import build_trend_alert_results
        return build_trend_alert_results(row_key.removesuffix("_trend")).read(product, reference_date)
    raise ValueError(f"Unknown alert row: {row_key}")


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=512, show_spinner=False)
def _cached_alert_result(row_key: str, product: str, reference_date: str,
                         signature: str, revision: int, selection: dict) -> dict:
    del signature, revision
    return _read_uncached(row_key, product, date.fromisoformat(reference_date), selection)


def read_alert_result(row_key: str, product: str, reference_date: date, **selection) -> dict:
    """Public report/matrix read. Empty product aggregates the enabled product views."""
    try:
        if not product and row_key != "parts_oos":
            results = [read_alert_result(row_key, item, reference_date, **selection) for item in ConfigLoader.get_enabled_products()]
            if not results:
                return _result("no_data")
            if any(item["state"] == "error" for item in results):
                return _result("error", message=UNAVAILABLE)
            frames = [item["alerts"] for item in results if not item["alerts"].empty]
            alerts = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True) if frames else results[0]["alerts"].iloc[:0].copy()
            state = "alert" if len(alerts) else "no_data" if all(item["state"] == "no_data" for item in results) else "ok"
            records = [item["records"] for item in results if not item["records"].empty]
            result = _result(state, alerts=alerts)
            result["records"] = pd.concat(records, ignore_index=True).drop_duplicates().reset_index(drop=True) if records else pd.DataFrame()
            result["alert_factories"] = sorted({factory for item in results for factory in item["alert_factories"]})
            return result
        signature = alert_result_signature(row_key, product, reference_date, **selection)
        revision = get_indicator_product_revision(row_key, product)
        return _cached_alert_result(row_key, product, reference_date.isoformat(), signature, revision, selection)
    except Exception:
        logger.exception("Published alert read failed: %s/%s", row_key, product)
        return _result("error", message=UNAVAILABLE)
