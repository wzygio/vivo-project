"""PostgreSQL adapter for the prepared Q-Time report dataset."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from src.qtime_domain.application.dtos import QTimeQuery, Shop
from src.qtime_domain.application.errors import QTimeDataAccessError

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


DETAIL_COLUMNS = [
    "step_desc",
    "lot_id",
    "prod_qty",
    "sub_prod_type",
    "f_step",
    "t_step",
    "q_spec",
    "wait_time",
    "timekey",
    "shop",
    "prodcode",
]
SAFE_DATA_ERROR = "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
logger = logging.getLogger(__name__)


class QTimeRepository:
    """Read Q-Time options and details from the report-owned database tables."""

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self._engine = db_manager.engine

    def list_products(self) -> tuple[str, ...]:
        frame = self._read_frame(
            text(
                "SELECT DISTINCT productspecname "
                "FROM eda.imp_qtime_tzbjx ORDER BY productspecname"
            ),
        )
        if frame.empty or "productspecname" not in frame:
            return ()
        products = frame["productspecname"].dropna().astype(str).str.strip()
        return tuple(sorted(product for product in products.unique() if product))

    def list_step_descriptions(self, shop: Shop) -> tuple[str, ...]:
        frame = self._read_frame(
            text(
                "SELECT DISTINCT step_desc FROM ("
                "SELECT CASE WHEN f_step LIKE '1%' THEN 'ARRAY' "
                "WHEN f_step LIKE '2%' THEN 'OLED' ELSE 'TP' END AS shop, "
                "step_desc FROM mdw.qtime_tzbjx"
                ") AS qtime_steps WHERE shop = :shop ORDER BY step_desc"
            ),
            params={"shop": shop},
        )
        if frame.empty or "step_desc" not in frame:
            return ()
        descriptions = frame["step_desc"].dropna().astype(str).str.strip()
        return tuple(sorted(value for value in descriptions.unique() if value))

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        sql = """
            SELECT
                step_desc,
                lot_id,
                prod_qty,
                sub_prod_type,
                f_step,
                t_step,
                q_spec,
                wait_time,
                timekey,
                CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                END AS shop,
                prodcode
            FROM mdw.qtime_tzbjx
            WHERE timekey >= :start_time
              AND timekey < :end_time
              AND step_desc = :step_desc
              AND CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                  END = :shop
        """
        params: dict[str, object] = {
            "start_time": query.start_time.strftime("%Y%m%d%H%M%S"),
            "end_time": query.end_time.strftime("%Y%m%d%H%M%S"),
            "shop": query.shop,
            "step_desc": query.step_desc,
        }
        if query.products:
            sql = f"{sql} AND prodcode IN :products"
            params["products"] = query.products
        statement = text(f"{sql} ORDER BY step_desc, lot_id, timekey")
        if query.products:
            statement = statement.bindparams(bindparam("products", expanding=True))

        frame = self._read_frame(statement, params=params)
        return self._normalize_details(frame)

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
            logger.error("Q-Time database read failed: %s", type(exc).__name__)
            raise QTimeDataAccessError(SAFE_DATA_ERROR) from exc

    @staticmethod
    def _normalize_details(frame: pd.DataFrame) -> pd.DataFrame:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        normalized = normalized.reindex(columns=DETAIL_COLUMNS)
        for column in ("prod_qty", "q_spec", "wait_time"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        return normalized
