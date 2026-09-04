"""Shared data access for ARRAY defect-detail facts."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


ARRAY_PARTICLE_COUNT_COLUMNS = [
    "factory",
    "prod_code",
    "start_time",
    "sheet_id",
    "step_id",
    "particle_size",
    "particle_qty",
]


def load_array_aoi_particle_size_counts(
    db_manager: "DatabaseManager",
    *,
    prod_code: str,
    start_time: datetime,
    end_time: datetime,
    step_id: str | None = None,
) -> pd.DataFrame:
    """读取 ARRAY AOI 的 O/L 缺陷行数，SPC 仅用于唯一 Sheet-产品映射。"""
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized.")

    step_filter = "AND adt.step_id = :step_id" if step_id else ""
    statement = text(
        f"""
        WITH sheet_products AS (
            SELECT DISTINCT
                sta.sheet_id,
                dmp.productcode
            FROM eda.spc_tzbjx_array AS sta
            JOIN mdw.dwr_mes_productspec AS dmp
              ON dmp.productspecname = sta.product_spec
            WHERE dmp.productcode = :prod_code
              AND sta.sheet_start_time >= :start_time
              AND sta.sheet_start_time < :end_time
        )
        SELECT
            'ARRAY' AS factory,
            sheet_products.productcode AS prod_code,
            MIN(adt.glass_start_time) AS start_time,
            adt.glass_id AS sheet_id,
            adt.step_id,
            UPPER(TRIM(adt.item119)) AS particle_size,
            COUNT(*) AS particle_qty
        FROM eda.ARRAY_DEFECT_T AS adt
        JOIN sheet_products
          ON sheet_products.sheet_id = adt.glass_id
        WHERE UPPER(TRIM(adt.item51)) = 'AOI'
          AND UPPER(TRIM(adt.item119)) IN ('O', 'L')
          AND adt.glass_start_time >= :start_time
          AND adt.glass_start_time < :end_time
          {step_filter}
        GROUP BY
            sheet_products.productcode,
            adt.glass_id,
            adt.step_id,
            UPPER(TRIM(adt.item119))
        ORDER BY
            adt.glass_id,
            adt.step_id,
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
