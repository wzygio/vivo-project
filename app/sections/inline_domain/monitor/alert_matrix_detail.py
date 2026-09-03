"""自动预警矩阵点击详情的懒加载与渲染（PRD §4.2 / D1，Phase 5）。

契约：

- **懒加载**：未选中单元格（session_state 无 ``MATRIX_SELECTION_STATE_KEY``）时
  不产生任何详情计算；选中后按 ``detail_key`` 经 ``get_cached_matrix_detail``
  （st.cache_data，键 = detail_key + 参考周 + 矩阵签名）加载，再次打开同一
  单元格命中缓存不重算；
- **ADR-0001**：详情数据包只含原生载荷（DataFrame / dict / list / 标量），
  各域 ViewModel 在缓存边界外即时消费，不进入缓存；
- **图像复用**：SPC（单片异常 / CPK）走 ``render_spc_indicator_sections`` 的
  RenderGate ``collect_memoized``；CTQ 走 ``render_ctq_indicator_sections``
  （同款 memo 参数）；Yield 走 ``render_alert_code_expanders``
  （collect_memoized）；Q-Time 由本层 RenderGate ``collect_memoized`` 包装
  ``build_qtime_figure``；AOI_TT / AOI_RS 复用各自
  ``render_*_indicator_sections``（其页面同样每次 rerun 重建图像，数据层已
  全部命中缓存）。chart key 统一 ``matrix_detail`` 前缀，与其他区域隔离；
- **降级**：详情加载失败仅在该单元格详情区显示 error，不影响矩阵本体。
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Mapping
from datetime import date
from functools import partial
from typing import Any

import pandas as pd
import streamlit as st

from app.components.alert_center import compute_lot_oos_records
from app.components.page_header import (
    build_product_cache_signature,
    get_product_cache_revision,
)
from app.manager.render_gate import RenderGate
from app.sections.inline_domain.monitor.alert_matrix import MATRIX_SELECTION_STATE_KEY
from app.sections.inline_domain.monitor.alert_matrix_cache import (
    MATRIX_CACHE_BASE_SIGNATURE,
    YIELD_SNAPSHOT_SIGNATURE_BASE,
    load_all_product_qtime_monitoring,
)
from app.sections.inline_domain.monitor.alert_matrix_service import (
    CELL_STATE_ALERT,
    CELL_STATE_ERROR,
    CELL_STATE_NO_DATA,
    CELL_STATE_OK,
)
from app.sections.inline_domain.shared.alert_center import (
    build_sheet_oos_alert_display,
    filter_report_by_alert_keys,
    render_sheet_oos_alert_center,
)
from src.inline_domain.application.shared.decorated_data import (
    SCOPE_DECORATION_FILE_NAME,
)
from src.inline_domain.core.shared.sheet_oos_alerts import (
    build_sheet_oos_alerts,
    previous_iso_week_range,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    load_sheet_oos_decoration,
)
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)

MATRIX_DETAIL_CHART_KEY_PREFIX = "matrix_detail"

# (prod_code, reference_date) -> 详情数据包（原生载荷）
DetailLoader = Callable[[str, date], dict[str, Any]]


# ---------------------------------------------------------------------------
# 缓存边界（ADR-0001：原生载荷进出）
# ---------------------------------------------------------------------------
@st.cache_data(
    show_spinner=False,
    max_entries=16,
    ttl=ConfigLoader.get_service_cache_ttl_seconds("alert_matrix_payload", default_hours=12),
)
def _cached_matrix_detail_bundle(
    detail_key: str,
    reference_date: str,
    signature: str,
    _loader: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """详情数据包缓存：键 = detail_key + 参考周 + 矩阵签名；loader 下划线排除哈希。"""
    del detail_key, reference_date, signature  # 仅作为缓存键参与
    return _loader()


def get_cached_matrix_detail(
    *,
    detail_key: str,
    reference_date: date,
    signature: str,
    _loader: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """详情数据包的缓存入口（再次打开同单元格命中缓存，不重算）。"""
    return _cached_matrix_detail_bundle(
        detail_key,
        reference_date.isoformat(),
        signature,
        _loader,
    )


# ---------------------------------------------------------------------------
# 生产 loader 装配（仅在点击 🔴 后构建；全部复用各域既有缓存服务）
# ---------------------------------------------------------------------------
def _report_end_date() -> date:
    """与各单产品报表页一致的时间窗终点（起点由各域 get_default_*_start_date 推导）。"""
    from src.inline_domain.application.monitor.monitor_service import (
        MonitorAnalysisService,
    )

    _, end_dt = MonitorAnalysisService.get_time_window()
    return end_dt.date()


def _load_sheet_oos_alerts_display(
    scope: str,
    prod_code: str,
    reference_date: date,
) -> pd.DataFrame:
    """只读加载 scope 修饰工作簿并产出中文预警展示表（与各域页面同口径）。"""
    from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
        AOI_RS_ALERT_CHART_KIND_LABELS,
        AOI_RS_ALERT_COLUMN_MAP,
        AOI_RS_ALERT_COLUMNS,
    )
    from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
        AOI_TT_ALERT_COLUMN_MAP,
        AOI_TT_ALERT_OUTPUT_COLUMNS,
    )
    from app.sections.inline_domain.ctq.ctq_dashboard import (
        CTQ_OOS_ALERT_COLUMN_MAP,
        CTQ_OOS_ALERT_COLUMNS,
    )
    from app.sections.inline_domain.spc.spc_dashboard import (
        SPC_OOS_ALERT_COLUMN_MAP,
        SPC_OOS_ALERT_COLUMNS,
    )
    from src.inline_domain.core.aoi_rs.aoi_rs_decoration import AOI_RS_OOS_KEY_COLUMNS
    from src.inline_domain.core.aoi_tt.aoi_tt_decoration import AOI_TT_OOS_KEY_COLUMNS

    scope_config = {
        "spc": ("sheet_start_time", None, SPC_OOS_ALERT_COLUMN_MAP, SPC_OOS_ALERT_COLUMNS),
        "ctq": ("sheet_start_time", None, CTQ_OOS_ALERT_COLUMN_MAP, CTQ_OOS_ALERT_COLUMNS),
        "aoi_tt": ("start_time", AOI_TT_OOS_KEY_COLUMNS, AOI_TT_ALERT_COLUMN_MAP, AOI_TT_ALERT_OUTPUT_COLUMNS),
        "aoi_rs": ("sheet_start_time", AOI_RS_OOS_KEY_COLUMNS, AOI_RS_ALERT_COLUMN_MAP, AOI_RS_ALERT_COLUMNS),
    }
    time_column, key_columns, column_map, output_columns = scope_config[scope]

    resource_dir = ConfigLoader.get_domain_resource_dir("inline_domain")
    decoration_df = load_sheet_oos_decoration(
        resource_dir,
        file_name=SCOPE_DECORATION_FILE_NAME[scope],
        sheet_name=prod_code,
        key_columns=key_columns,
    )
    if decoration_df.empty:
        return pd.DataFrame(columns=output_columns)
    alerts_df = build_sheet_oos_alerts(
        decoration_df,
        time_column=time_column,
        reference_date=reference_date,
    )
    display_df = build_sheet_oos_alert_display(
        alerts_df,
        column_map=column_map,
        output_columns=output_columns,
    )
    if scope == "aoi_rs" and "图类型" in display_df.columns:
        display_df["图类型"] = display_df["图类型"].astype(str).replace(
            AOI_RS_ALERT_CHART_KIND_LABELS
        )
    if "超规时间" in display_df.columns:
        display_df["超规时间"] = display_df["超规时间"].astype(str)
    return display_df


def _load_spc_view(db_manager: Any, prod_code: str):
    """SPC 报表 ViewModel（复用 SPC 页装配；payload 层 L2 缓存）。"""
    from app.sections.inline_domain.spc.spc_dashboard import get_default_spc_start_date
    from src.inline_domain.application.shared.decision_signature import (
        get_scope_decision_signature,
    )
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.inline_domain.composition import build_spc_repository

    end_date = _report_end_date()
    start_date = get_default_spc_start_date(end_date)
    query_config = SpcQueryConfig(
        prod_code=prod_code,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        data_type_filter="SPC",
    )
    return SpcReportService.get_spc_report_data(
        _data_port=build_spc_repository(db_manager, prod_code),
        query_config_json=query_config.model_dump_json(),
        # 与矩阵 CPK 行同一 snapshot 基签名：复用矩阵构建时已填充的 L2 条目。
        snapshot_signature=build_product_cache_signature(
            MATRIX_CACHE_BASE_SIGNATURE, prod_code
        ),
        period_sigma_source=ConfigLoader.get_spc_period_sigma_source(),
        product_revision=get_product_cache_revision(prod_code),
        decision_signature=get_scope_decision_signature("spc", prod_code),
    )


def _make_sheet_oos_loader(scope: str, db_manager: Any) -> DetailLoader:
    """sheet OOS 四行（aoi_rs/aoi_tt/spc/ctq）的详情 loader：预警表 + 命中指标帧。"""

    def load(prod_code: str, reference_date: date) -> dict[str, Any]:
        alerts_df = _load_sheet_oos_alerts_display(scope, prod_code, reference_date)
        end_date = _report_end_date()
        frames: dict[str, pd.DataFrame] = {}

        if scope == "spc":
            from app.sections.inline_domain.spc.spc_dashboard import (
                CPK_ALERT_KEY_COLUMN_MAP,
            )

            view = _load_spc_view(db_manager, prod_code)
            for name, frame in (
                ("period_capability_df", view.period_capability_df),
                ("sheet_features_df", view.sheet_features_df),
                ("raw_measurements_df", view.raw_measurements_df),
            ):
                frames[name] = filter_report_by_alert_keys(
                    frame, alerts_df, CPK_ALERT_KEY_COLUMN_MAP
                )
        elif scope == "ctq":
            from app.sections.inline_domain.ctq.ctq_dashboard import (
                CTQ_OOS_ALERT_KEY_COLUMN_MAP,
                get_default_ctq_start_date,
            )
            from src.inline_domain.application.ctq.ctq_service import CtqReportService
            from src.inline_domain.application.shared.decision_signature import (
                get_scope_decision_signature,
            )
            from src.inline_domain.application.spc.dtos import SpcQueryConfig
            from src.inline_domain.composition import build_ctq_repository

            start_date = get_default_ctq_start_date(end_date)
            query_config = SpcQueryConfig(
                prod_code=prod_code,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                data_type_filter="CTQ",
            )
            view = CtqReportService.get_ctq_report_data(
                _data_port=build_ctq_repository(db_manager, prod_code),
                query_config_json=query_config.model_dump_json(),
                snapshot_signature=build_product_cache_signature(
                    MATRIX_CACHE_BASE_SIGNATURE, prod_code
                ),
                product_revision=get_product_cache_revision(prod_code),
                decision_signature=get_scope_decision_signature("ctq", prod_code),
            )
            for name, frame in (
                ("sheet_features_df", view.sheet_features_df),
                ("raw_measurements_df", view.raw_measurements_df),
            ):
                frames[name] = filter_report_by_alert_keys(
                    frame, alerts_df, CTQ_OOS_ALERT_KEY_COLUMN_MAP
                )
        elif scope == "aoi_tt":
            from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
                AOI_TT_ALERT_KEY_MAP,
                get_default_aoi_tt_start_date,
            )
            from src.inline_domain.application.aoi_tt.aoi_tt_service import (
                AoiTtReportService,
            )
            from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
            from src.inline_domain.application.shared.decision_signature import (
                get_scope_decision_signature,
            )
            from src.inline_domain.composition import build_aoi_tt_repository

            start_date = get_default_aoi_tt_start_date(end_date)
            query_config = AoiTtQueryConfig(
                prod_code=prod_code,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
            view = AoiTtReportService.get_aoi_tt_report_data(
                _data_port=build_aoi_tt_repository(db_manager, prod_code),
                query_config_json=query_config.model_dump_json(),
                snapshot_signature=build_product_cache_signature(
                    MATRIX_CACHE_BASE_SIGNATURE, prod_code
                ),
                product_revision=get_product_cache_revision(prod_code),
                decision_signature=get_scope_decision_signature("aoi_tt", prod_code),
            )
            frames["tt_details_df"] = filter_report_by_alert_keys(
                view.tt_details_df, alerts_df, AOI_TT_ALERT_KEY_MAP
            )
            frames["indicators_df"] = filter_report_by_alert_keys(
                view.indicators_df, alerts_df, AOI_TT_ALERT_KEY_MAP
            )
            frames["spec_df"] = view.spec_df
        elif scope == "aoi_rs":
            from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
                AOI_RS_ALERT_KEY_MAP,
                AOI_RS_ALERT_PASS_THROUGH_KEY_MAP,
                get_default_aoi_rs_start_date,
            )
            from src.inline_domain.application.aoi_rs.aoi_rs_service import (
                AoiRsReportService,
            )
            from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
            from src.inline_domain.application.shared.decision_signature import (
                get_scope_decision_signature,
            )
            from src.inline_domain.composition import build_aoi_rs_repository

            start_date = get_default_aoi_rs_start_date(end_date)
            query_config = AoiRsQueryConfig(
                prod_code=prod_code,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
            view = AoiRsReportService.get_aoi_rs_report_data(
                _data_port=build_aoi_rs_repository(db_manager, prod_code),
                query_config_json=query_config.model_dump_json(),
                snapshot_signature=build_product_cache_signature(
                    MATRIX_CACHE_BASE_SIGNATURE, prod_code
                ),
                product_revision=get_product_cache_revision(prod_code),
                decision_signature=get_scope_decision_signature("aoi_rs", prod_code),
            )
            frames["rs_details_df"] = filter_report_by_alert_keys(
                view.rs_details_df, alerts_df, AOI_RS_ALERT_KEY_MAP
            )
            frames["pass_through_df"] = filter_report_by_alert_keys(
                view.pass_through_df, alerts_df, AOI_RS_ALERT_PASS_THROUGH_KEY_MAP
            )
            frames["indicators_df"] = filter_report_by_alert_keys(
                view.indicators_df, alerts_df, AOI_RS_ALERT_KEY_MAP
            )
            frames["lot_points_df"] = filter_report_by_alert_keys(
                view.lot_points_df, alerts_df, AOI_RS_ALERT_KEY_MAP
            )
            frames["sheet_points_df"] = filter_report_by_alert_keys(
                view.sheet_points_df, alerts_df, AOI_RS_ALERT_KEY_MAP
            )
            frames["spec_df"] = view.spec_df

        return {
            "kind": "sheet_oos",
            "scope": scope,
            "alerts_df": alerts_df,
            "frames": frames,
            "end_date": end_date.isoformat(),
        }

    return load


def _make_spc_cpk_loader(db_manager: Any) -> DetailLoader:
    """SPC 趋势波动（CPK）行：上一周 CPK 预警表 + 命中指标帧。"""

    def load(prod_code: str, reference_date: date) -> dict[str, Any]:
        from app.sections.inline_domain.spc.spc_dashboard import (
            build_weekly_cpk_alerts,
            filter_spc_report_by_alerts,
        )

        view = _load_spc_view(db_manager, prod_code)
        alerts_df = build_weekly_cpk_alerts(
            view.period_capability_df,
            reference_date=reference_date,
        )
        return {
            "kind": "spc_cpk",
            "alerts_df": alerts_df,
            "frames": {
                "period_capability_df": filter_spc_report_by_alerts(
                    view.period_capability_df, alerts_df
                ),
                "sheet_features_df": filter_spc_report_by_alerts(
                    view.sheet_features_df, alerts_df
                ),
                "raw_measurements_df": filter_spc_report_by_alerts(
                    view.raw_measurements_df, alerts_df
                ),
            },
        }

    return load


def _make_yield_loader(db_manager: Any, mode: str) -> DetailLoader:
    """yield 两行（lot 超规 / 趋势波动）：命中记录 + 出图所需全量数据（只读）。"""

    def load(prod_code: str, reference_date: date) -> dict[str, Any]:
        from yield_domain.application.alert_service import AlertService
        from yield_domain.application.yield_service import YieldAnalysisService

        config = ConfigLoader.load_config(prod_code)
        product_dir = ConfigLoader.get_domain_resource_dir("yield_domain") / prod_code
        snapshot_signature = build_product_cache_signature(
            YIELD_SNAPSHOT_SIGNATURE_BASE, prod_code
        )
        # read_only=True：矩阵详情只读消费，不触发良损修饰表回写（与矩阵一致）。
        mwd_group_data = YieldAnalysisService.get_mwd_trend_data(
            config, product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
            config, product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        lot_data = YieldAnalysisService.get_lot_defect_rates(
            config, product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        sheet_data = YieldAnalysisService.get_sheet_defect_rates(
            config, product_dir,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            read_only=True,
        )
        mapping_data = YieldAnalysisService.get_mapping_data(
            config,
            _db_manager=db_manager,
            snapshot_signature=snapshot_signature,
            product_dir=product_dir,
            read_only=True,
        )
        warning_lines = YieldAnalysisService.load_static_warning_lines(
            config, product_dir, snapshot_signature
        )

        if mode == "lot":
            oos_records, _ = compute_lot_oos_records(lot_data, warning_lines)
            start, end = previous_iso_week_range(reference_date)
            records = [
                record
                for record in oos_records
                if start
                <= pd.to_datetime(str(record.get("入库时间")), format="%Y/%m/%d", errors="coerce")
                < end
            ]
        else:
            records = AlertService.get_dashboard_alert_records(
                mwd_group_data or {},
                mwd_code_data or {},
            )

        return {
            "kind": "yield",
            "yield_mode": mode,
            "records": records,
            "mwd_code_data": mwd_code_data or {},
            "lot_data": lot_data or {},
            "sheet_data": sheet_data or {},
            "mapping_data": mapping_data,
            "warning_lines": warning_lines or {},
            "hotspot_scripts": config.processing.get("mapping_hotspot_script", []),
            "mapping_layout": config.processing.get("mapping_layout"),
            "product_code": prod_code,
        }

    return load


def _make_qtime_loader(db_manager: Any) -> DetailLoader:
    """qtime 行：全产品监控（与矩阵同一 L2 缓存）→ 按产品 + 上一 ISO 周过滤。"""

    def load(prod_code: str, reference_date: date) -> dict[str, Any]:
        details_df, alerts_df = load_all_product_qtime_monitoring(
            db_manager, reference_date
        )

        def _for_product(frame: pd.DataFrame) -> pd.DataFrame:
            if frame.empty or "prodcode" not in frame.columns:
                return frame.iloc[0:0].copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
            return frame.loc[frame["prodcode"].astype(str) == str(prod_code)].copy()

        prod_details = _for_product(details_df)
        prod_alerts = _for_product(alerts_df)
        if not prod_alerts.empty and "timekey" in prod_alerts.columns:
            start, end = previous_iso_week_range(reference_date)
            times = pd.to_datetime(
                prod_alerts["timekey"].astype(str).str.slice(0, 14),
                format="%Y%m%d%H%M%S",
                errors="coerce",
            )
            prod_alerts = prod_alerts.loc[
                (times >= start) & (times < end)
            ].copy()
        total_lots = (
            int(prod_details["lot_id"].nunique())
            if not prod_details.empty and "lot_id" in prod_details.columns
            else 0
        )
        return {
            "kind": "qtime",
            "alerts_df": prod_alerts,
            "details_df": prod_details,
            "total_lots": total_lots,
        }

    return load


def build_default_detail_loaders(db_manager: Any = None) -> dict[str, DetailLoader]:
    """生产装配：8 行详情 loader。仅在点击 🔴 单元格后调用（懒加载边界）。"""
    if db_manager is None:
        from src.shared_kernel.infrastructure.db_handler import DatabaseManager

        db_manager = DatabaseManager()
    return {
        "aoi_rs_sheet_oos": _make_sheet_oos_loader("aoi_rs", db_manager),
        "aoi_tt_sheet_oos": _make_sheet_oos_loader("aoi_tt", db_manager),
        "spc_sheet_oos": _make_sheet_oos_loader("spc", db_manager),
        "spc_cpk_trend": _make_spc_cpk_loader(db_manager),
        "ctq_sheet_oos": _make_sheet_oos_loader("ctq", db_manager),
        "yield_lot_oos": _make_yield_loader(db_manager, "lot"),
        "yield_trend_fluctuation": _make_yield_loader(db_manager, "trend"),
        "qtime_sheet_oos": _make_qtime_loader(db_manager),
    }


# ---------------------------------------------------------------------------
# 渲染（图像走各域既有管线；chart key 统一 matrix_detail 前缀）
# ---------------------------------------------------------------------------
def _detail_charts_signature(
    row_key: str,
    prod_code: str,
    alerts_df: pd.DataFrame,
    memo_base: str,
) -> str:
    """详情图像的 memo 签名：产品 revision + 矩阵签名 + 预警内容指纹。

    memo_base 含矩阵 payload 的 generated_at：「刷新缓存」后 payload 重建，
    签名必变、图像必重建；同一版数据重复 rerun 命中 memo 直接复用。
    """
    revision = get_product_cache_revision(prod_code)
    fingerprint = hashlib.sha256(
        f"{len(alerts_df)}|{alerts_df.astype(str).to_csv(index=False)}".encode("utf-8")
    ).hexdigest()[:16]
    return f"matrix_detail|{row_key}|{prod_code}|rev={revision}|{memo_base}|alerts={fingerprint}"


def _indicator_count(frame: pd.DataFrame, key_columns: list[str]) -> int:
    if frame.empty or not set(key_columns).issubset(frame.columns):
        return 0
    return int(frame.groupby(key_columns).ngroups)


def _render_sheet_oos_detail(
    bundle: Mapping[str, Any],
    *,
    row_key: str,
    prod_code: str,
    week_label: str,
    step_desc_map: dict[str, str] | None,
    memo_base: str,
) -> None:
    from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
        render_aoi_rs_indicator_sections,
    )
    from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
        render_aoi_tt_indicator_sections,
    )
    from app.sections.inline_domain.ctq.ctq_dashboard import (
        render_ctq_indicator_sections,
    )
    from app.sections.inline_domain.spc.spc_dashboard import (
        render_spc_indicator_sections,
    )

    scope = str(bundle["scope"])
    alerts_df = bundle["alerts_df"]
    frames = bundle["frames"]

    render_sheet_oos_alert_center(
        alerts_df,
        title=f"单片异常预警明细（上一周 {week_label}）",
        has_source_data=True,
        step_desc_map=step_desc_map,
    )
    if alerts_df.empty:
        return

    key_columns = {
        "spc": ["factory", "step_id", "param_name"],
        "ctq": ["factory", "step_id", "param_name"],
        "aoi_tt": ["factory", "step_id", "tt_name"],
        "aoi_rs": ["factory", "step_id", "rs_code"],
    }[scope]
    indicator_frame = frames.get("indicators_df")
    if indicator_frame is None:
        indicator_frame = frames.get("sheet_features_df", pd.DataFrame())
    count = _indicator_count(indicator_frame, key_columns)

    with st.expander(f"🚨 自动预警指标图像（{count} 个指标）", expanded=False):
        st.caption("以下图像由单片异常预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        if scope == "spc":
            if frames["sheet_features_df"].empty:
                st.warning("预警指标暂无可绘制的 Sheet 数据。")
                return
            render_spc_indicator_sections(
                period_capability_df=frames["period_capability_df"],
                sheet_features_df=frames["sheet_features_df"],
                raw_measurements_df=frames["raw_measurements_df"],
                period_box_source=ConfigLoader.get_spc_period_box_source(),
                memo_signature=_detail_charts_signature(
                    row_key, prod_code, alerts_df, memo_base
                ),
                memo_state_key="matrix_detail_spc_oos_charts_memo",
                chart_key_prefix=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_spc_oos",
                step_desc_map=step_desc_map,
            )
        elif scope == "ctq":
            if frames["sheet_features_df"].empty:
                st.warning("预警指标暂无可绘制的 Sheet 数据。")
                return
            render_ctq_indicator_sections(
                sheet_features_df=frames["sheet_features_df"],
                raw_measurements_df=frames["raw_measurements_df"],
                period_box_source=ConfigLoader.get_spc_period_box_source(),
                step_desc_map=step_desc_map,
                chart_key_prefix=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_ctq_oos",
                memo_signature=_detail_charts_signature(
                    row_key, prod_code, alerts_df, memo_base
                ),
                memo_state_key="matrix_detail_ctq_oos_charts_memo",
            )
        elif scope == "aoi_tt":
            if frames["indicators_df"].empty or frames["tt_details_df"].empty:
                st.warning("预警指标暂无可绘制的 AOI TT 数据。")
                return
            render_aoi_tt_indicator_sections(
                tt_details_df=frames["tt_details_df"],
                spec_df=frames["spec_df"],
                indicators_df=frames["indicators_df"],
                end_date=date.fromisoformat(bundle["end_date"]),
                step_desc_map=step_desc_map,
            )
        elif scope == "aoi_rs":
            if frames["indicators_df"].empty:
                st.warning("单片异常预警指标暂无可绘制的 AOI RS 数据。")
                return
            render_aoi_rs_indicator_sections(
                rs_details_df=frames["rs_details_df"],
                pass_through_df=frames["pass_through_df"],
                spec_df=frames["spec_df"],
                indicators_df=frames["indicators_df"],
                lot_points_df=frames["lot_points_df"],
                sheet_points_df=frames["sheet_points_df"],
                end_date=date.fromisoformat(bundle["end_date"]),
                chart_key_prefix=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_aoi_rs",
                step_desc_map=step_desc_map,
            )


def _render_spc_cpk_detail(
    bundle: Mapping[str, Any],
    *,
    row_key: str,
    prod_code: str,
    week_label: str,
    step_desc_map: dict[str, str] | None,
    memo_base: str,
) -> None:
    from app.sections.inline_domain.spc.spc_dashboard import (
        CPK_ALERT_THRESHOLD,
        render_spc_indicator_sections,
    )
    from app.utils.step_labels import format_step_label

    alerts_df = bundle["alerts_df"]
    frames = bundle["frames"]
    with st.expander(
        f"CPK 预警明细（上一周 {week_label}，CPK < {CPK_ALERT_THRESHOLD:.2f}）",
        expanded=True,
    ):
        if alerts_df.empty:
            st.info("当前已无上一周 CPK 预警（数据可能已更新）。")
            return
        st.error(f"检测到 {len(alerts_df)} 条 CPK 预警，请关注。")
        display_df = alerts_df
        if step_desc_map and "站点" in alerts_df.columns:
            display_df = alerts_df.copy()
            display_df["站点"] = display_df["站点"].map(
                lambda step: format_step_label(step, step_desc_map)
            )
        st.dataframe(
            display_df,
            column_config={
                "CPK值": st.column_config.NumberColumn("CPK值", format="%.3f")
            },
            hide_index=True,
            width="stretch",
        )

    if frames["sheet_features_df"].empty:
        st.warning("预警指标暂无可绘制的 Sheet 数据。")
        return
    count = _indicator_count(frames["sheet_features_df"], ["factory", "step_id", "param_name"])
    with st.expander(f"🚨 CPK 自动预警指标图像（{count} 个指标）", expanded=False):
        st.caption("以下图像由 CPK 预警自动匹配，无需通过筛选器查询；每个指标保留独立的子折叠面板。")
        render_spc_indicator_sections(
            period_capability_df=frames["period_capability_df"],
            sheet_features_df=frames["sheet_features_df"],
            raw_measurements_df=frames["raw_measurements_df"],
            period_box_source=ConfigLoader.get_spc_period_box_source(),
            memo_signature=_detail_charts_signature(row_key, prod_code, alerts_df, memo_base),
            memo_state_key="matrix_detail_cpk_charts_memo",
            chart_key_prefix=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_spc_cpk",
            step_desc_map=step_desc_map,
        )


def _render_yield_detail(
    bundle: Mapping[str, Any],
    *,
    prod_code: str,
    week_label: str,
) -> None:
    from app.sections.yield_domain.yield_dashboard import render_alert_code_expanders

    mode = str(bundle["yield_mode"])
    records = list(bundle["records"])
    title = (
        f"Lot 超规预警明细（上一周 {week_label}）"
        if mode == "lot"
        else "良率波动预警明细（月/周环比，period 制口径）"
    )
    with st.expander(title, expanded=True):
        if not records:
            st.info("当前已无预警记录（数据可能已更新）。")
            return
        st.error(f"检测到 {len(records)} 条预警记录，请关注。")
        st.dataframe(pd.DataFrame(records), hide_index=True, width="stretch")

    render_alert_code_expanders(
        trend_records=records if mode == "trend" else None,
        lot_oos_records=records if mode == "lot" else None,
        warning_lines=bundle["warning_lines"],
        mwd_code_data=bundle["mwd_code_data"],
        lot_data=bundle["lot_data"],
        sheet_data=bundle["sheet_data"],
        mapping_data=bundle["mapping_data"],
        hotspot_scripts=bundle["hotspot_scripts"],
        product_code=prod_code,
        mapping_layout=bundle["mapping_layout"],
        memo_state_key="matrix_detail_yield_charts_memo",
        chart_key_prefix=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_yield",
    )


def _render_qtime_detail(
    bundle: Mapping[str, Any],
    *,
    row_key: str,
    prod_code: str,
    memo_base: str,
) -> None:
    from app.charts.indicator_domain.qtime.chart import build_qtime_figure
    from app.sections.indicator_domain.qtime.alert_center import (
        render_qtime_alert_center,
    )

    alerts_df = bundle["alerts_df"]
    details_df = bundle["details_df"]
    render_qtime_alert_center(alerts_df, total_lots=int(bundle["total_lots"]))

    if details_df.empty:
        st.info("当前产品暂无 Q-Time 明细数据。")
        return

    gate = RenderGate()
    gate.stage(
        partial(
            build_qtime_figure,
            details_df,
            title=f"北极星QTime监控｜{prod_code}",
        )
    )
    figures = gate.collect_memoized(
        "matrix_detail_qtime_chart_memo",
        _detail_charts_signature(row_key, prod_code, alerts_df, memo_base),
    )
    if figures:
        st.plotly_chart(
            figures[0],
            width="stretch",
            key=f"{MATRIX_DETAIL_CHART_KEY_PREFIX}_qtime_{prod_code}",
        )


def _render_detail_bundle(
    bundle: Mapping[str, Any],
    *,
    row_key: str,
    prod_code: str,
    week_label: str,
    step_desc_map: dict[str, str] | None,
    memo_base: str,
) -> None:
    kind = bundle.get("kind")
    if kind == "sheet_oos":
        _render_sheet_oos_detail(
            bundle,
            row_key=row_key,
            prod_code=prod_code,
            week_label=week_label,
            step_desc_map=step_desc_map,
            memo_base=memo_base,
        )
    elif kind == "spc_cpk":
        _render_spc_cpk_detail(
            bundle,
            row_key=row_key,
            prod_code=prod_code,
            week_label=week_label,
            step_desc_map=step_desc_map,
            memo_base=memo_base,
        )
    elif kind == "yield":
        _render_yield_detail(bundle, prod_code=prod_code, week_label=week_label)
    elif kind == "qtime":
        _render_qtime_detail(
            bundle, row_key=row_key, prod_code=prod_code, memo_base=memo_base
        )
    else:
        st.info("该行暂不支持详情查看。")


def _close_detail() -> None:
    st.session_state.pop(MATRIX_SELECTION_STATE_KEY, None)


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
def render_alert_matrix_detail(
    payload: Mapping[str, Any],
    *,
    db_manager: Any = None,
    step_desc_map: dict[str, str] | None = None,
    loaders: Mapping[str, DetailLoader] | None = None,
) -> None:
    """按选中单元格懒加载并渲染详情；未选中时不产生任何详情计算。"""
    detail_key = st.session_state.get(MATRIX_SELECTION_STATE_KEY)
    if not detail_key:
        return

    row_key, _, prod_code = str(detail_key).partition("|")
    rows_by_key = {row["row_key"]: row for row in payload.get("rows", [])}
    row_meta = rows_by_key.get(row_key)
    cell = payload.get("cells", {}).get((row_key, prod_code))
    if row_meta is None or cell is None:
        return

    week = payload.get("reference_week", {})
    week_label = week.get("label", "")
    # 矩阵 reference_date 归一为本周一，即 reference_week["end"]。
    reference_date = date.fromisoformat(week["end"])
    memo_base = f"{payload.get('signature', '')}|{payload.get('generated_at', '')}"

    with st.container(border=True):
        title_column, close_column = st.columns([11, 1], vertical_alignment="center")
        title_column.markdown(
            f"**🔍 预警详情｜{prod_code} × {row_meta['display_name']}**"
            f"（{row_meta['time_scope']}）"
        )
        close_column.button(
            "✖",
            key="matrix_detail_close",
            help="关闭详情",
            on_click=_close_detail,
            width="stretch",
        )

        state = cell.get("state")
        if state == CELL_STATE_OK:
            st.success("该产品该项上一周期无预警（达标）。")
            return
        if state == CELL_STATE_NO_DATA:
            st.info(cell.get("message") or "无数据。")
            return
        if state == CELL_STATE_ERROR:
            st.warning(f"加载失败：{cell.get('message') or '未知原因'}")
            return
        if state != CELL_STATE_ALERT:
            st.info(f"当前状态（{state}）无详情可查看。")
            return

        # 仅 🔴 单元格进入懒加载：loader 注册表在此才构建。
        active_loaders = (
            dict(loaders) if loaders is not None else build_default_detail_loaders(db_manager)
        )
        loader = active_loaders.get(row_key)
        if loader is None:
            st.info("该行暂不支持详情查看。")
            return

        try:
            bundle = get_cached_matrix_detail(
                detail_key=str(detail_key),
                reference_date=reference_date,
                signature=str(payload.get("signature", "")),
                _loader=lambda: loader(prod_code, reference_date),
            )
        except Exception as exc:  # noqa: BLE001 - 详情级降级，不影响矩阵本体
            logger.exception("[alert-matrix] 详情加载失败 %s: %s", detail_key, exc)
            st.error(f"预警详情加载失败：{exc}")
            return

        _render_detail_bundle(
            bundle,
            row_key=row_key,
            prod_code=prod_code,
            week_label=week_label,
            step_desc_map=step_desc_map,
            memo_base=memo_base,
        )
