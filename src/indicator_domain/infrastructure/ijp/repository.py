"""PostgreSQL adapter for the IJP overflow report datasets.

PANEL_ID/PANEL_LOCATION 派生与 BOTTOM0~9 展开在 core 纯 Python 完成
（split_part 为 PG 专属），本仓储只查询原始列。`EVENT_TIME` 是 varchar，
仅 PostgreSQL 方言下使用 `::TIMESTAMP` 与 `<> 'NaT'` 过滤；SQLite
契约测试走纯文本比较分支。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
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
DAILY_COLUMNS = ["day", "rs_code", "code_num", "ratio"]
RAW_DETAIL_COLUMNS = [
    "print_time",
    "productcode",
    "glass_id",
    "printer",
    "rs_code",
    "image_name",
]
SAFE_DATA_ERROR = "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"
DAILY_LOOKBACK_DAYS = 7
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
    ("product_names", "AND P.productspecname IN :product_names"),
    ("product_codes", "AND P.productcode IN :product_codes"),
    ("lines", "AND SUBSTR(H.sub_equip_id, 1, 6) IN :lines"),
    ("equipments", "AND H.sub_equip_id IN :equipments"),
    ("glass_ids", "AND D.GLASS_ID IN :glass_ids"),
    ("sub_prod_types", "AND V.sub_prod_type IN :sub_prod_types"),
    ("codes", "AND D.RS_CODE IN :codes"),
    ("picis", "AND T.PICI IN :picis"),
    ("cycles", "AND T.CYCLE_ID IN :cycles"),
)


class IjpRepository:
    """Read IJP overflow options, daily ratios and details from the report database."""

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

    def list_product_names(self, product_codes: tuple[str, ...]) -> tuple[str, ...]:
        sql = (
            "SELECT DISTINCT PROD_ID FROM DWR_MES_PRODUCTSPEC_V "
            "WHERE FACTORY = 'OLED'"
        )
        params: dict[str, object] = {}
        expanding: list[str] = []
        if product_codes:
            sql += " AND product_code IN :product_codes"
            params["product_codes"] = product_codes
            expanding.append("product_codes")
        frame = self._read_frame(
            self._statement(f"{sql} ORDER BY PROD_ID", expanding),
            params=params,
        )
        return self._options(frame, "prod_id")

    def list_sub_prod_types(self) -> tuple[str, ...]:
        frame = self._read_frame(
            text(
                "SELECT DISTINCT SUB_PROD_TYPE FROM DWR_MES_PRODUCTREQUEST_V "
                "WHERE FACTORY = 'OLED' AND SUB_PROD_TYPE IS NOT NULL "
                "ORDER BY SUB_PROD_TYPE"
            ),
        )
        return self._options(frame, "sub_prod_type")

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

    def list_cycles(
        self,
        start_time: datetime,
        end_time: datetime,
        product_codes: tuple[str, ...],
        picis: tuple[str, ...],
    ) -> tuple[str, ...]:
        sql = (
            "SELECT CYCLE_ID FROM EDA.DWD_GLASS_OLED_CYCLE_V3 WHERE "
            + self._event_time_filter()
        )
        params = self._window_params(start_time, end_time)
        expanding: list[str] = []
        if product_codes:
            sql += " AND PROD_CODE IN :product_codes"
            params["product_codes"] = product_codes
            expanding.append("product_codes")
        if picis:
            sql += " AND PICI IN :picis"
            params["picis"] = picis
            expanding.append("picis")
        frame = self._read_frame(
            self._statement(f"{sql} GROUP BY CYCLE_ID ORDER BY CYCLE_ID", expanding),
            params=params,
        )
        return self._options(frame, "cycle_id")

    def fetch_daily_ratios(self, query: IjpQuery) -> pd.DataFrame:
        window_start = query.start_time - timedelta(days=DAILY_LOOKBACK_DAYS)
        select = (
            "SELECT SUBSTR(CAST(D.GLASS_START_TIME AS TEXT), 1, 10) AS day, "
            "D.RS_CODE AS rs_code, COUNT(*) AS code_num"
        )
        group_by = (
            " GROUP BY SUBSTR(CAST(D.GLASS_START_TIME AS TEXT), 1, 10), D.RS_CODE"
            " ORDER BY day, rs_code"
        )
        statement, params = self._filtered_statement(
            query, select, group_by, window_start
        )
        frame = self._read_frame(statement, params=params)
        frame = self._normalize(frame, ["day", "rs_code", "code_num"])
        if frame.empty:
            return frame.reindex(columns=DAILY_COLUMNS)
        frame["code_num"] = pd.to_numeric(frame["code_num"], errors="coerce").fillna(0)
        shifted_day = pd.to_datetime(frame["day"], errors="coerce") + pd.Timedelta(
            days=self._data_forward_policy.effective_days
        )
        frame["day"] = shifted_day.dt.strftime("%Y-%m-%d")
        totals = frame.groupby("day")["code_num"].transform("sum")
        frame["ratio"] = (frame["code_num"] / totals.where(totals > 0)).round(3)
        return frame.reindex(columns=DAILY_COLUMNS)

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

        if query.panel_locations:
            wanted = set(query.panel_locations)
            normalized_location = shaped["panel_location"].where(
                ~shaped["panel_location"].fillna("").str.startswith("BOTTOM"),
                "BOTTOM",
            )
            shaped = shaped[normalized_location.isin(wanted)]

        shaped = shaped.sort_values(
            ["productcode", "print_time", "rs_code", "glass_id", "printer"],
            kind="stable",
            na_position="last",
        )
        return shaped.reindex(columns=DETAIL_COLUMNS).reset_index(drop=True)


def _format_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")
