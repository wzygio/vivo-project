"""Shared data access for TP defect-detail facts."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

from src.inline_domain.infrastructure.shared.array_defect_data_loader import (
    ARRAY_PARTICLE_COUNT_COLUMNS,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


def load_tp_particle_size_counts(
    db_manager: "DatabaseManager",
    *,
    prod_code: str,
    start_time: datetime,
    end_time: datetime,
    step_id: str | None = None,
) -> pd.DataFrame:
    """读取 TP 的 S/M/L/H 缺陷行数；Particle Size 字段为 item2。"""
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized.")

    step_filter = "AND tdt.step_id = :step_id" if step_id else ""
    statement = text(
        f"""
        WITH sheet_products AS (
            SELECT DISTINCT
                sta.glass_id AS sheet_id,
                dmp.productcode
            FROM eda.spc_tzbjx_tsp AS sta
            JOIN mdw.dwr_mes_productspec AS dmp
              ON dmp.productspecname = sta.product_spec
            WHERE dmp.productcode = :prod_code
              AND sta.glass_start_time >= :start_time
              AND sta.glass_start_time < :end_time
        )
        SELECT
            'TP' AS factory,
            sheet_products.productcode AS prod_code,
            MIN(tdt.cut_start_time) AS start_time,
            tdt.cut_id AS sheet_id,
            tdt.step_id,
            UPPER(TRIM(tdt.item2)) AS particle_size,
            COUNT(*) AS particle_qty
        FROM eda.TSP_DEFECT_T AS tdt
        JOIN sheet_products
          ON sheet_products.sheet_id = tdt.cut_id
        WHERE UPPER(TRIM(tdt.item2)) IN ('S', 'M', 'L', 'H')
          AND tdt.cut_start_time >= :start_time
          AND tdt.cut_start_time < :end_time
          {step_filter}
        GROUP BY
            sheet_products.productcode,
            tdt.cut_id,
            tdt.step_id,
            UPPER(TRIM(tdt.item2))
        ORDER BY
            tdt.cut_id,
            tdt.step_id,
            particle_size
        """
    )
    params: dict[str, object] = {
        "prod_code": prod_code,
        "start_time": start_time,
        "end_time": end_time,
    }
    if step_id:
        params["step_id"] = step_id

    result = pd.read_sql(statement, db_manager.engine, params=params)
    result.columns = result.columns.str.lower()
    return result.reindex(columns=ARRAY_PARTICLE_COUNT_COLUMNS)
