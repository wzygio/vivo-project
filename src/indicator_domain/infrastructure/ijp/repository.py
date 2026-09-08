"""PostgreSQL adapter for the IJP overflow report datasets.

PANEL_ID/PANEL_LOCATION 派生与 BOTTOM0~9 展开在 core 纯 Python 完成
（split_part 为 PG 专属），本仓储只查询原始列。`EVENT_TIME` 是 varchar，
仅 PostgreSQL 方言下使用 `::TIMESTAMP` 与 `<> 'NaT'` 过滤；SQLite
契约测试走纯文本比较分支。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.core.ijp.overflow import (
    IJP_EQUIPMENTS,
    IJP_RS_CODES,
    build_image_url,
    extract_panel_id,
    map_bottom_breakout,
    map_panel_location,
)
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


DETAIL_COLUMNS = [
    "print_time",
    "productcode",
    "glass_id",
    "printer",
    "panel_id",
    "image_url",
    "panel_location",
    "rs_code",
    "code_ratio",
]
GLASS_DIMENSIONS = ["productcode", "line", "printer", "glass_id"]
GLASS_COLUMNS = [*GLASS_DIMENSIONS, "rs_code", "code_num", "ratio"]
RAW_DETAIL_COLUMNS = [
    "print_time",
    "productcode",
    "glass_id",
    "printer",
    "rs_code",
    "image_name",
]
SAFE_DATA_ERROR = "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
logger = logging.getLogger(__name__)

_JOINED_FROM = """
    FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
    LEFT JOIN DWR_MES_PRODUCTSPEC P
           ON P.PRODUCTSPECNAME = D.PRODUCT_SPEC
    LEFT JOIN EDA.OLED_CHAMBER_HST_T H
           ON D.GLASS_ID = H.CUT_ID
          AND H.CUT_START_TIME >= :start_time
          AND H.CUT_START_TIME <= :end_time
    LEFT JOIN DWR_MES_PRODUCTREQUEST_V V
           ON H.ITEM5 = V.SUB_PROD_ID
    LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T
           ON D.GLASS_ID = T.GLASS_ID
    WHERE H.SUB_EQUIP_ID IN :equip_whitelist
      AND D.RS_CODE IN :code_whitelist
      AND D.GLASS_START_TIME >= :start_time
      AND D.GLASS_START_TIME <= :end_time
"""

# (IjpQuery 字段, SQL 片段) —— 空集合 = 不过滤，与 FineReport IF(LEN()=0) 语义一致。
_OPTIONAL_FILTERS = (
    ("product_codes", "AND P.productcode IN :product_codes"),
    ("lines", "AND SUBSTR(H.sub_equip_id, 1, 6) IN :lines"),
    ("work_order_types", "AND V.sub_prod_type IN :work_order_types"),
    ("codes", "AND D.RS_CODE IN :codes"),
    ("picis", "AND T.PICI IN :picis"),
)


class IjpRepository:
    """Read IJP overflow options, glass ratios and details from the report database."""

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self._engine = db_manager.engine
        self._data_forward_policy = ConfigLoader.get_data_forward_policy()

    def list_product_codes(self) -> tuple[str, ...]:
        frame = self._read_frame(
            text(
                "SELECT DISTINCT product_code FROM DWR_MES_PRODUCTSPEC_V "
                "WHERE factory LIKE '%OLED%' ORDER BY product_code"
            ),
        )
        return self._options(frame, "product_code")

    def list_picis(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...],
    ) -> tuple[str, ...]:
        sql = (
            "SELECT PICI FROM EDA.DWD_GLASS_OLED_CYCLE_V3 WHERE "
            + self._event_time_filter()
        )
        params = self._window_params(start_time, end_time)
        expanding: list[str] = []
        if product_codes:
            sql += " AND PROD_CODE IN :product_codes"
            params["product_codes"] = product_codes
            expanding.append("product_codes")
        frame = self._read_frame(
            self._statement(f"{sql} GROUP BY PICI ORDER BY PICI", expanding),
            params=params,
        )
        return self._options(frame, "pici")

    def fetch_glass_ratios(self, query: IjpQuery) -> pd.DataFrame:
        select = (
            "SELECT P.PRODUCTCODE AS productcode, "
            "SUBSTR(H.SUB_EQUIP_ID, 1, 6) AS line, "
            "H.SUB_EQUIP_ID AS printer, "
            "D.GLASS_ID AS glass_id, "
            "D.RS_CODE AS rs_code, COUNT(*) AS code_num"
        )
        group_by = (
            " GROUP BY P.PRODUCTCODE, SUBSTR(H.SUB_EQUIP_ID, 1, 6), "
            "H.SUB_EQUIP_ID, "
            "D.GLASS_ID, D.RS_CODE"
            " ORDER BY productcode, line, printer, glass_id, rs_code"
        )
        statement, params = self._filtered_statement(
            query, select, group_by, query.start_time
        )
        frame = self._read_frame(statement, params=params)
        frame = self._normalize(frame, [*GLASS_DIMENSIONS, "rs_code", "code_num"])
        if frame.empty:
            return frame.reindex(columns=GLASS_COLUMNS)
        frame["code_num"] = pd.to_numeric(frame["code_num"], errors="coerce").fillna(0)
        totals = frame.groupby(GLASS_DIMENSIONS)["code_num"].transform("sum")
        frame["ratio"] = (frame["code_num"] / totals.where(totals > 0)).round(3)
        return frame.reindex(columns=GLASS_COLUMNS)

    def fetch_daily_counts(self, query: IjpQuery) -> pd.DataFrame:
        """Untruncated raw counts by display day, for monthly/weekly aggregation."""
        select = (
            "SELECT P.PRODUCTCODE AS productcode, "
            "SUBSTR(H.SUB_EQUIP_ID, 1, 6) AS line, H.SUB_EQUIP_ID AS printer, "
            "SUBSTR(CAST(D.GLASS_START_TIME AS TEXT), 1, 10) AS day, "
            "D.RS_CODE AS rs_code, COUNT(*) AS code_num"
        )
        tail = (
            " GROUP BY P.PRODUCTCODE, H.SUB_EQUIP_ID, "
            "SUBSTR(CAST(D.GLASS_START_TIME AS TEXT), 1, 10), D.RS_CODE"
            " ORDER BY productcode, printer, day, rs_code"
        )
        statement, params = self._filtered_statement(query, select, tail, query.start_time)
        columns = ["productcode", "line", "printer", "day", "rs_code", "code_num"]
        frame = self._normalize(self._read_frame(statement, params=params), columns)
        if not frame.empty:
            frame["day"] = (pd.to_datetime(frame["day"]) + pd.Timedelta(
                days=self._data_forward_policy.effective_days,
            )).dt.strftime("%Y-%m-%d")
        return frame

    def fetch_details(self, query: IjpQuery) -> pd.DataFrame:
        select = (
            "SELECT D.GLASS_START_TIME AS print_time, P.PRODUCTCODE AS productcode, "
            "D.GLASS_ID AS glass_id, H.SUB_EQUIP_ID AS printer, "
            "D.RS_CODE AS rs_code, D.RS_DEFECT_IMAGE_NAME AS image_name"
        )
        tail = (
            " ORDER BY P.PRODUCTCODE, D.GLASS_START_TIME, D.RS_CODE, "
            f"D.GLASS_ID, H.SUB_EQUIP_ID LIMIT {int(query.detail_limit)}"
        )
        statement, params = self._filtered_statement(
            query, select, tail, query.start_time
        )
        frame = self._read_frame(statement, params=params)
        return self._shape_details(self._normalize(frame, RAW_DETAIL_COLUMNS), query)

    def _filtered_statement(
        self,
        query: IjpQuery,
        select: str,
        tail: str,
        window_start: datetime,
    ) -> tuple[object, dict[str, object]]:
        sql = f"{select}\n{_JOINED_FROM}"
        source_start, source_end = self._data_forward_policy.to_source_window(
            pd.Timestamp(window_start),
            pd.Timestamp(query.end_time),
        )
        params: dict[str, object] = {
            "start_time": _format_timestamp(source_start.to_pydatetime()),
            "end_time": _format_timestamp(source_end.to_pydatetime()),
            "equip_whitelist": IJP_EQUIPMENTS,
            "code_whitelist": IJP_RS_CODES,
        }
        expanding = ["equip_whitelist", "code_whitelist"]
        for field, clause in _OPTIONAL_FILTERS:
            values = getattr(query, field)
            if values:
                sql += f"\n      {clause}"
                params[field] = values
                expanding.append(field)
        return self._statement(f"{sql}{tail}", expanding), params

    @staticmethod
    def _statement(sql: str, expanding: list[str]):
        statement = text(sql)
        if expanding:
            statement = statement.bindparams(
                *(bindparam(name, expanding=True) for name in expanding)
            )
        return statement

    def _event_time_filter(self) -> str:
        if self._dialect_name() == "postgresql":
            return (
                "EVENT_TIME::TIMESTAMP >= :start_time "
                "AND EVENT_TIME::TIMESTAMP <= :end_time "
                "AND EVENT_TIME <> 'NaT'"
            )
        return "EVENT_TIME >= :start_time AND EVENT_TIME <= :end_time"

    def _dialect_name(self) -> str:
        return getattr(getattr(self._engine, "dialect", None), "name", "") or ""

    def _window_params(self, start_time: datetime, end_time: datetime) -> dict[str, object]:
        source_start, source_end = self._data_forward_policy.to_source_window(
            pd.Timestamp(start_time),
            pd.Timestamp(end_time),
        )
        return {
            "start_time": _format_timestamp(source_start.to_pydatetime()),
            "end_time": _format_timestamp(source_end.to_pydatetime()),
        }

    def _read_frame(
        self,
        statement: object,
        *,
        params: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        try:
            if self._engine is None:
                raise RuntimeError("database engine unavailable")
            if params is None:
                return pd.read_sql(statement, self._engine)
            return pd.read_sql(statement, self._engine, params=params)
        except Exception as exc:
            logger.error("IJP database read failed: %s", type(exc).__name__)
            raise IjpDataAccessError(SAFE_DATA_ERROR) from exc

    @staticmethod
    def _normalize(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        return normalized.reindex(columns=columns)

    @staticmethod
    def _options(frame: pd.DataFrame, column: str) -> tuple[str, ...]:
        if frame.empty or column not in frame.columns.str.lower():
            return ()
        values = frame.iloc[:, 0].dropna().astype(str).str.strip()
        return tuple(sorted(value for value in values.unique() if value))

    def _shape_details(self, frame: pd.DataFrame, query: IjpQuery) -> pd.DataFrame:
        if frame.empty:
            return frame.reindex(columns=DETAIL_COLUMNS)
        shaped = self._data_forward_policy.shift_frame(frame, ("print_time",))
        shaped["panel_id"] = shaped["image_name"].map(extract_panel_id)
        shaped["image_url"] = shaped["image_name"].map(build_image_url)
        shaped["panel_location"] = [
            map_panel_location(rs_code, image_name)
            for rs_code, image_name in zip(
                shaped["rs_code"], shaped["image_name"], strict=True
            )
        ]
        totals = shaped.groupby("glass_id")["rs_code"].transform("size")
        counts = shaped.groupby(["glass_id", "rs_code"])["rs_code"].transform("size")
        shaped["code_ratio"] = (counts / totals.where(totals > 0)).round(3)

        # SERACH1 UNION ALL 第二支：B0~B9 额外展开为 BOTTOM0~BOTTOM9。
        breakout = shaped["image_name"].map(map_bottom_breakout)
        extra = shaped[breakout.notna()].copy()
        extra["panel_location"] = breakout[breakout.notna()]
        shaped = pd.concat([shaped, extra], ignore_index=True)

        shaped = shaped.sort_values(
            ["productcode", "print_time", "rs_code", "glass_id", "printer"],
            kind="stable",
            na_position="last",
        )
        return shaped.reindex(columns=DETAIL_COLUMNS).reset_index(drop=True)


def _format_timestamp(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="microseconds" if value.microsecond else "seconds")
