"""PostgreSQL adapter for the prepared Q-Time report dataset."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from src.indicator_domain.application.qtime.dtos import QTimeQuery, QTimeStepOption, Shop
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.shared_kernel.config import ConfigLoader

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
        self._data_forward_policy = ConfigLoader.get_data_forward_policy()

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

    def list_step_options(self, shop: Shop) -> tuple[QTimeStepOption, ...]:
        frame = self._read_frame(
            text(
                "SELECT DISTINCT step_desc, f_step, t_step FROM ("
                "SELECT CASE WHEN f_step LIKE '1%' THEN 'ARRAY' "
                "WHEN f_step LIKE '2%' THEN 'OLED' ELSE 'TP' END AS shop, "
                "step_desc, f_step, t_step FROM mdw.qtime_tzbjx"
                ") AS qtime_steps WHERE shop = :shop "
                "ORDER BY step_desc, f_step, t_step"
            ),
            params={"shop": shop},
        )
        required_columns = {"step_desc", "f_step", "t_step"}
        if frame.empty or not required_columns.issubset(frame.columns):
            return ()
        normalized = frame.loc[:, ["step_desc", "f_step", "t_step"]].dropna().copy()
        for column in ("step_desc", "f_step", "t_step"):
            normalized[column] = normalized[column].astype(str).str.strip()
        normalized = normalized.loc[(normalized != "").all(axis=1)].drop_duplicates()
        normalized = normalized.sort_values(["step_desc", "f_step", "t_step"])
        return tuple(
            QTimeStepOption(
                step_desc=row.step_desc,
                f_step=row.f_step,
                t_step=row.t_step,
            )
            for row in normalized.itertuples(index=False)
        )

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
              AND step_desc IN :step_descriptions
              AND CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                  END = :shop
        """
        source_start, source_end = self._data_forward_policy.to_source_window(
            pd.Timestamp(query.start_time),
            pd.Timestamp(query.end_time),
        )
        params: dict[str, object] = {
            "start_time": source_start.strftime("%Y%m%d%H%M%S"),
            "end_time": source_end.strftime("%Y%m%d%H%M%S"),
            "shop": query.shop,
            "step_descriptions": query.step_descriptions,
        }
        if query.products:
            sql = f"{sql} AND prodcode IN :products"
            params["products"] = query.products
        statement = text(f"{sql} ORDER BY step_desc, lot_id, timekey").bindparams(
            bindparam("step_descriptions", expanding=True)
        )
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

    def _normalize_details(self, frame: pd.DataFrame) -> pd.DataFrame:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        normalized = normalized.reindex(columns=DETAIL_COLUMNS)
        for column in ("prod_qty", "q_spec", "wait_time"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        timekey = pd.to_datetime(
            normalized["timekey"],
            format="%Y%m%d%H%M%S",
            errors="coerce",
        ) + pd.Timedelta(days=self._data_forward_policy.effective_days)
        normalized["timekey"] = timekey.dt.strftime("%Y%m%d%H%M%S")
        return normalized
