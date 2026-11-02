"""Reusable database adapter for the shared Inline measurement fact set."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)

MQC_PRODUCT_STEP_ID = "2120A"

FACTORY_TABLES = {
    "ARRAY": ("spc_tzbjx_array", "sheet_id", "sheet_start_time"),
    "OLED": ("spc_tzbjx_oled", "glass_id", "glass_start_time"),
    "TP": ("spc_tzbjx_tsp", "glass_id", "glass_start_time"),
}

RAW_MEASUREMENT_COLUMNS = [
    "factory",
    "prod_code",
    "start_time",
    "sheet_id",
    "lot_id",
    "step_id",
    "param_name",
    "site_name",
    "unit_id",
    "param_value",
]


def load_raw_measurements(
    db_manager: "DatabaseManager",
    start_date: str,
    end_date: str,
    prod_code: str,
) -> pd.DataFrame:
    """Load and normalize one product's measurements from all three factories."""
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized.")

    selects = []
    for factory, (table_name, id_column, time_column) in FACTORY_TABLES.items():
        normal_station_filter = (
            "AND (T.step_id <> :mqc_step_id OR T.step_id IS NULL)"
            if factory == "OLED" else ""
        )
        product_routes = [(
            "P.productcode",
            "JOIN mdw.dwr_mes_productspec P ON T.product_spec = P.productspecname",
            normal_station_filter,
        )]
        if factory == "OLED":
            # 2120A belongs to the PPID-mapped product even when its spec says MQC.
            product_routes.append((
                "M.prodcode",
                """JOIN (
                    SELECT DISTINCT ppid, prodcode
                    FROM eda.IMP_SPC_TZBJX_MQC
                    WHERE prodcode = :prod_code
                ) M ON T.ppid = M.ppid""",
                "AND T.step_id = :mqc_step_id",
            ))
        for product_column, product_join, station_filter in product_routes:
            selects.append(
                f"""
            SELECT
                '{factory}' AS factory,
                {product_column} AS prod_code,
                T.{time_column} AS start_time,
                T.{id_column} AS sheet_id,
                T.lot_id,
                T.step_id,
                T.param_name,
                T.site_name,
                T.unit_id,
                T.param_value
            FROM eda.{table_name} T
            {product_join}
            WHERE T.{time_column} >= :start_time
              AND T.{time_column} <= :end_time
              AND {product_column} = :prod_code
              {station_filter}
            """
            )

    params = {
        "start_time": f"{start_date} 00:00:00",
        "end_time": f"{end_date} 23:59:59",
        "prod_code": prod_code,
        "mqc_step_id": MQC_PRODUCT_STEP_ID,
    }
    try:
        result = pd.read_sql(text(" UNION ALL ".join(selects)), db_manager.engine, params=params)
    except Exception:
        logger.exception("Failed to load shared Inline measurements for %s", prod_code)
        raise

    result.columns = result.columns.str.lower()
    if result.empty:
        return pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)

    normalized = result.reindex(columns=RAW_MEASUREMENT_COLUMNS).copy()
    normalized["start_time"] = pd.to_datetime(normalized["start_time"], errors="coerce")
    normalized["param_value"] = pd.to_numeric(normalized["param_value"], errors="coerce")
    return normalized.dropna(subset=["start_time"]).reset_index(drop=True)
