"""Reusable database adapter for Inline parameter catalog and specification facts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

SPEC_COLUMNS = [
    "prod_code",
    "step_id",
    "param_name",
    "param_type",
    "usl",
    "lsl",
    "ucl",
    "lcl",
    "main_step_id",
    "main_eqp_type",
]
CATALOG_COLUMNS = ["ref_param_name", "data_type"]


def _read_sql(
    db_manager: "DatabaseManager",
    sql: str,
    prod_code: str,
) -> pd.DataFrame:
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized.")
    result = pd.read_sql(text(sql), db_manager.engine, params={"prod_code": prod_code})
    result.columns = result.columns.str.lower()
    return result


def load_parameter_catalog(
    db_manager: "DatabaseManager",
    prod_code: str,
) -> pd.DataFrame:
    result = _read_sql(
        db_manager,
        """
        SELECT DISTINCT
            T1.parmtername AS ref_param_name,
            T1.data_type
        FROM eda.IMP_SPC_TZBJX T1
        JOIN mdw.dwr_mes_productspec T2
          ON T1.productspecname = T2.productspecname
        WHERE T2.productcode = :prod_code
        """,
        prod_code,
    )
    if result.empty:
        return pd.DataFrame(columns=CATALOG_COLUMNS)
    normalized = result.reindex(columns=CATALOG_COLUMNS).copy()
    normalized["ref_param_name"] = normalized["ref_param_name"].astype(str).str.strip().str.upper()
    return normalized


def load_parameter_specs(
    db_manager: "DatabaseManager",
    prod_code: str,
) -> pd.DataFrame:
    result = _read_sql(
        db_manager,
        """
        SELECT
            prod_code,
            step_id,
            param_name,
            param_type,
            usl,
            lsl,
            ucl,
            lcl,
            main_step_id,
            main_eqp_type
        FROM mdw.dwd_imp_dv_param_spec
        WHERE prod_code = :prod_code
        """,
        prod_code,
    )
    if result.empty:
        return pd.DataFrame(columns=SPEC_COLUMNS)

    specs = result.reindex(columns=SPEC_COLUMNS).copy()
    for column in ("usl", "lsl", "ucl", "lcl"):
        specs[column] = pd.to_numeric(specs[column], errors="coerce")
    main_steps = specs["main_step_id"].astype("string").str.strip()
    specs["main_step_id"] = main_steps.mask(main_steps.eq("")).fillna(
        specs["step_id"].astype(str)
    )
    route_types = specs["main_eqp_type"].astype("string").str.strip().str.upper()
    specs["main_eqp_type"] = route_types.where(route_types.isin({"EQP", "CHAMBER"}), "EQP")
    return specs


class InlineMeasurementMetadataRepository:
    """Expose measurement metadata through the application-owned port."""

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self.db_manager = db_manager

    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame:
        return load_parameter_catalog(self.db_manager, prod_code)

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame:
        return load_parameter_specs(self.db_manager, prod_code)
