"""Reusable database adapter for main-process OUT histories."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MAIN_PROCESS_EVENT_TIME_COLUMN = "main_process_event_time"
HISTORY_OUTPUT_COLUMNS = [
    "factory",
    "main_eqp_type",
    "sheet_id",
    "main_step_id",
    "main_process_unit_id",
    MAIN_PROCESS_EVENT_TIME_COLUMN,
    "main_process_trace_source",
    "source_rank",
]

ARRAY_EQP_HISTORY_SQL = """
SELECT 'ARRAY' AS factory, 'EQP' AS main_eqp_type, sheet_id,
       oper_code AS main_step_id, eqp_id AS main_process_unit_id,
       event_timekey, 'array_sht' AS main_process_trace_source, 1 AS source_rank
FROM mdw.dwt_inout_sht
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND sheet_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
"""

ARRAY_CHAMBER_HISTORY_SQL = """
SELECT 'ARRAY' AS factory, 'CHAMBER' AS main_eqp_type, sheet_id,
       oper_code AS main_step_id, sub_unit_id AS main_process_unit_id,
       event_timekey, 'array_sub_unit_sht' AS main_process_trace_source,
       1 AS source_rank
FROM mdw.dwt_inout_sub_unit_sht
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND sheet_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
  AND split_part(sub_unit_id, '-', 2) IN ('CVD', 'SPU', 'DRE', 'OVE')
  AND substr(sub_unit_id, 8, 6) IN ('DRE-PC', 'CVD-CH', 'OVE-CH', 'SPU-PM')
UNION ALL
SELECT 'ARRAY' AS factory, 'CHAMBER' AS main_eqp_type, sheet_id,
       oper_code AS main_step_id, unit_id AS main_process_unit_id,
       event_timekey, 'array_unit_sht' AS main_process_trace_source,
       2 AS source_rank
FROM mdw.dwt_inout_unit_sht
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND sheet_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
  AND substr(unit_id, 8, 2) = 'CH'
"""


def _glass_eqp_history_sql(factory: str) -> str:
    trace_source = f"{factory.lower()}_gls"
    factory_predicate = "factory LIKE 'OLED%'" if factory == "OLED" else "factory = 'TP'"
    return f"""
SELECT '{factory}' AS factory, 'EQP' AS main_eqp_type,
       glass_id AS sheet_id, oper_code AS main_step_id,
       eqp_id AS main_process_unit_id, event_timekey,
       '{trace_source}' AS main_process_trace_source, 1 AS source_rank
FROM mdw.dwt_inout_gls
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND {factory_predicate}
  AND glass_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
"""


TP_CHAMBER_HISTORY_SQL = """
SELECT 'TP' AS factory, 'CHAMBER' AS main_eqp_type,
       glass_id AS sheet_id, oper_code AS main_step_id,
       sub_unit_id AS main_process_unit_id, event_timekey,
       'tp_sub_unit_gls' AS main_process_trace_source, 1 AS source_rank
FROM mdw.dwt_inout_sub_unit_gls
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND factory = 'TP'
  AND glass_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
  AND split_part(sub_unit_id, '-', 2) IN ('CVD', 'SPU', 'DRE', 'OVE')
  AND substr(sub_unit_id, 8, 6) IN ('DRE-PC', 'CVD-CH', 'OVE-CH', 'SPU-PM')
"""

OLED_CHAMBER_HISTORY_SQL = """
WITH oled_history AS (
    SELECT 'OLED' AS factory, 'CHAMBER' AS main_eqp_type,
           history.glass_id AS sheet_id,
           CASE
               WHEN route.new_oper IN
                    ('21200-CVD1', '21200-CVD2', '21200-CVD3', '21200-CVD4')
                AND substring(route.sub_unit_id, 12, 2) = 'CH'
               THEN '21200-CVD'
               ELSE route.new_oper
           END AS main_step_id,
           history.sub_unit_id AS main_process_unit_id,
           history.event_timekey,
           'oled_sub_unit_gls' AS main_process_trace_source,
           1 AS source_rank
    FROM mdw.dwt_inout_sub_unit_gls AS history
    JOIN mdw.dwd_mes_oled_oper_layer_v AS route
      ON history.oper_code = route.oper_code
     AND history.sub_unit_id = route.sub_unit_id
     AND route.new_oper IS NOT NULL
    WHERE history.date_timekey BETWEEN :history_start AND :history_end
      AND history.inout_type = 'OUT'
      AND history.factory LIKE 'OLED%'
      AND history.glass_id = ANY(:material_ids)
)
SELECT * FROM oled_history WHERE main_step_id = ANY(:main_step_ids)
"""

HISTORY_ROUTE_SQL = {
    ("ARRAY", "EQP"): ARRAY_EQP_HISTORY_SQL,
    ("ARRAY", "CHAMBER"): ARRAY_CHAMBER_HISTORY_SQL,
    ("OLED", "EQP"): _glass_eqp_history_sql("OLED"),
    ("TP", "EQP"): _glass_eqp_history_sql("TP"),
    ("TP", "CHAMBER"): TP_CHAMBER_HISTORY_SQL,
    ("OLED", "CHAMBER"): OLED_CHAMBER_HISTORY_SQL,
}


def _format_history_date(value: object) -> str:
    return pd.to_datetime(value, errors="raise").strftime("%Y%m%d")


class InlineMainProcessHistoryRepository:
    """Load only histories referenced by the routed measurement fact set."""

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self.db_manager = db_manager
        self.data_forward_policy = ConfigLoader.get_data_forward_policy()

    def get_main_process_history(
        self,
        routed_measurements: pd.DataFrame,
        history_start: object,
        history_end: object,
    ) -> pd.DataFrame:
        if routed_measurements.empty:
            return pd.DataFrame(columns=HISTORY_OUTPUT_COLUMNS)
        if self.db_manager.engine is None:
            raise ValueError("Database engine is not initialized")

        frames: list[pd.DataFrame] = []
        for (factory, route_type), sql_query in HISTORY_ROUTE_SQL.items():
            target = routed_measurements[
                routed_measurements["factory"].eq(factory)
                & routed_measurements["main_eqp_type"].eq(route_type)
            ]
            if target.empty:
                continue

            source_start, source_end = self.data_forward_policy.to_source_window(
                pd.Timestamp(history_start),
                pd.Timestamp(history_end),
            )
            params = {
                "history_start": _format_history_date(source_start),
                "history_end": _format_history_date(source_end),
                "material_ids": target["sheet_id"].dropna().astype(str).drop_duplicates().tolist(),
                "main_step_ids": target["main_step_id"].dropna().astype(str).drop_duplicates().tolist(),
            }
            frame = pd.read_sql(text(sql_query), self.db_manager.engine, params=params)
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=HISTORY_OUTPUT_COLUMNS)

        history = pd.concat(frames, ignore_index=True)
        history.columns = history.columns.str.lower()
        history[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.to_datetime(
            history.pop("event_timekey").astype("string").str[:14],
            format="%Y%m%d%H%M%S",
            errors="coerce",
        )
        return self.data_forward_policy.shift_frame(
            history[HISTORY_OUTPUT_COLUMNS],
            (MAIN_PROCESS_EVENT_TIME_COLUMN,),
        )
