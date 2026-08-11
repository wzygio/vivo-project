from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MAIN_PROCESS_UNIT_COLUMN = "main_process_unit_id"
MAIN_PROCESS_EVENT_TIME_COLUMN = "main_process_event_time"
MAIN_PROCESS_TRACE_SOURCE_COLUMN = "main_process_trace_source"

_TRACE_TARGET_COLUMNS = [
    "factory",
    "sheet_id",
    "main_step_id",
    "main_eqp_type",
    "sheet_start_time",
    "unit_id",
]
_HISTORY_MATCH_COLUMNS = ["factory", "main_eqp_type", "sheet_id", "main_step_id"]
_SPEC_KEY_COLUMNS = ["prod_code", "step_id", "param_name"]
_HISTORY_OUTPUT_COLUMNS = [
    "factory",
    "main_eqp_type",
    "sheet_id",
    "main_step_id",
    MAIN_PROCESS_UNIT_COLUMN,
    MAIN_PROCESS_EVENT_TIME_COLUMN,
    MAIN_PROCESS_TRACE_SOURCE_COLUMN,
    "source_rank",
]

_ARRAY_EQP_HISTORY_SQL = """
SELECT
    'ARRAY' AS factory,
    'EQP' AS main_eqp_type,
    sheet_id,
    oper_code AS main_step_id,
    eqp_id AS main_process_unit_id,
    event_timekey,
    'array_sht' AS main_process_trace_source,
    1 AS source_rank
FROM mdw.dwt_inout_sht
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND sheet_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
"""

_ARRAY_CHAMBER_HISTORY_SQL = """
SELECT
    'ARRAY' AS factory,
    'CHAMBER' AS main_eqp_type,
    sheet_id,
    oper_code AS main_step_id,
    sub_unit_id AS main_process_unit_id,
    event_timekey,
    'array_sub_unit_sht' AS main_process_trace_source,
    1 AS source_rank
FROM mdw.dwt_inout_sub_unit_sht
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND sheet_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
  AND split_part(sub_unit_id, '-', 2) IN ('CVD', 'SPU', 'DRE', 'OVE')
  AND substr(sub_unit_id, 8, 6) IN ('DRE-PC', 'CVD-CH', 'OVE-CH', 'SPU-PM')
UNION ALL
SELECT
    'ARRAY' AS factory,
    'CHAMBER' AS main_eqp_type,
    sheet_id,
    oper_code AS main_step_id,
    unit_id AS main_process_unit_id,
    event_timekey,
    'array_unit_sht' AS main_process_trace_source,
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
SELECT
    '{factory}' AS factory,
    'EQP' AS main_eqp_type,
    glass_id AS sheet_id,
    oper_code AS main_step_id,
    eqp_id AS main_process_unit_id,
    event_timekey,
    '{trace_source}' AS main_process_trace_source,
    1 AS source_rank
FROM mdw.dwt_inout_gls
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND {factory_predicate}
  AND glass_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
"""


_TP_CHAMBER_HISTORY_SQL = """
SELECT
    'TP' AS factory,
    'CHAMBER' AS main_eqp_type,
    glass_id AS sheet_id,
    oper_code AS main_step_id,
    sub_unit_id AS main_process_unit_id,
    event_timekey,
    'tp_sub_unit_gls' AS main_process_trace_source,
    1 AS source_rank
FROM mdw.dwt_inout_sub_unit_gls
WHERE date_timekey BETWEEN :history_start AND :history_end
  AND inout_type = 'OUT'
  AND factory = 'TP'
  AND glass_id = ANY(:material_ids)
  AND oper_code = ANY(:main_step_ids)
  AND split_part(sub_unit_id, '-', 2) IN ('CVD', 'SPU', 'DRE', 'OVE')
  AND substr(sub_unit_id, 8, 6) IN ('DRE-PC', 'CVD-CH', 'OVE-CH', 'SPU-PM')
"""

_OLED_CHAMBER_HISTORY_SQL = """
WITH oled_history AS (
    SELECT
        'OLED' AS factory,
        'CHAMBER' AS main_eqp_type,
        history.glass_id AS sheet_id,
        CASE
            WHEN route.new_oper IN ('21200-CVD1', '21200-CVD2', '21200-CVD3', '21200-CVD4')
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
SELECT *
FROM oled_history
WHERE main_step_id = ANY(:main_step_ids)
"""

_HISTORY_ROUTE_SQL = {
    ("ARRAY", "EQP"): _ARRAY_EQP_HISTORY_SQL,
    ("ARRAY", "CHAMBER"): _ARRAY_CHAMBER_HISTORY_SQL,
    ("OLED", "EQP"): _glass_eqp_history_sql("OLED"),
    ("TP", "EQP"): _glass_eqp_history_sql("TP"),
    ("TP", "CHAMBER"): _TP_CHAMBER_HISTORY_SQL,
    ("OLED", "CHAMBER"): _OLED_CHAMBER_HISTORY_SQL,
}


def _format_history_date(value: object) -> str:
    timestamp = pd.to_datetime(value, errors="raise")
    return timestamp.strftime("%Y%m%d")


def load_main_process_history(
    db_manager: "DatabaseManager",
    routed_measurements_df: pd.DataFrame,
    history_start: object,
    history_end: object,
) -> pd.DataFrame:
    """Load only the main-process OUT histories needed by the routed measurements."""
    if routed_measurements_df.empty:
        return pd.DataFrame(columns=_HISTORY_OUTPUT_COLUMNS)
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized")

    frames: list[pd.DataFrame] = []
    for (factory, route_type), sql_query in _HISTORY_ROUTE_SQL.items():
        target = routed_measurements_df[
            (routed_measurements_df["factory"] == factory)
            & (routed_measurements_df["main_eqp_type"] == route_type)
        ]
        if target.empty:
            continue

        params = {
            "history_start": _format_history_date(history_start),
            "history_end": _format_history_date(history_end),
            "material_ids": target["sheet_id"].dropna().astype(str).drop_duplicates().tolist(),
            "main_step_ids": target["main_step_id"].dropna().astype(str).drop_duplicates().tolist(),
        }
        frame = pd.read_sql(text(sql_query), db_manager.engine, params=params)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=_HISTORY_OUTPUT_COLUMNS)

    history = pd.concat(frames, ignore_index=True)
    history.columns = history.columns.str.lower()
    history[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.to_datetime(
        history.pop("event_timekey").astype("string").str[:14],
        format="%Y%m%d%H%M%S",
        errors="coerce",
    )
    return history[_HISTORY_OUTPUT_COLUMNS]


def enrich_measurements_with_main_process_trace(
    db_manager: "DatabaseManager",
    measurements_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    history_start: object,
    history_end: object,
) -> pd.DataFrame:
    """Attach specification routing and resolved main-process history to point measurements."""
    routed_measurements = attach_main_process_spec(measurements_df, spec_df)
    history = load_main_process_history(
        db_manager,
        routed_measurements,
        history_start=history_start,
        history_end=history_end,
    )
    return apply_main_process_history(routed_measurements, history)


def attach_main_process_spec(
    measurements_df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the unique main-process route for each product, station, and parameter."""
    result = measurements_df.drop(
        columns=["main_step_id", "main_eqp_type"], errors="ignore"
    ).copy()
    if spec_df.empty or not set(_SPEC_KEY_COLUMNS).issubset(spec_df.columns):
        result["main_step_id"] = result["step_id"].astype(str)
        result["main_eqp_type"] = "EQP"
        return result

    required_spec_columns = _SPEC_KEY_COLUMNS + ["main_step_id", "main_eqp_type"]
    missing_spec_columns = set(required_spec_columns) - set(spec_df.columns)
    if missing_spec_columns:
        raise ValueError(f"Specifications missing main-process columns: {sorted(missing_spec_columns)}")

    routes = spec_df[required_spec_columns].copy()
    if routes.duplicated(_SPEC_KEY_COLUMNS).any():
        raise ValueError("Main-process specifications must be unique by product, step, and parameter")

    result = result.merge(
        routes,
        on=_SPEC_KEY_COLUMNS,
        how="left",
        validate="many_to_one",
    )
    normalized_steps = result["main_step_id"].astype("string").str.strip()
    result["main_step_id"] = normalized_steps.mask(normalized_steps.eq("")).fillna(
        result["step_id"].astype(str)
    )
    normalized_route_types = result["main_eqp_type"].astype("string").str.strip().str.upper()
    result["main_eqp_type"] = normalized_route_types.where(
        normalized_route_types.isin({"EQP", "CHAMBER"}),
        "EQP",
    )
    return result


def apply_main_process_history(
    measurements_df: pd.DataFrame,
    history_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the nearest preceding main-process history without expanding point rows."""
    missing_columns = set(_TRACE_TARGET_COLUMNS) - set(measurements_df.columns)
    if missing_columns:
        raise ValueError(f"Measurements missing main-process trace columns: {sorted(missing_columns)}")

    result = measurements_df.drop(
        columns=[
            MAIN_PROCESS_UNIT_COLUMN,
            MAIN_PROCESS_EVENT_TIME_COLUMN,
            MAIN_PROCESS_TRACE_SOURCE_COLUMN,
        ],
        errors="ignore",
    ).copy()
    result["sheet_start_time"] = pd.to_datetime(result["sheet_start_time"], errors="coerce")
    result["_trace_target_id"] = pd.factorize(
        pd.MultiIndex.from_frame(result[_TRACE_TARGET_COLUMNS])
    )[0]

    targets = result.drop_duplicates("_trace_target_id")[_TRACE_TARGET_COLUMNS + ["_trace_target_id"]].copy()
    is_eqp_route = targets["main_eqp_type"].eq("EQP")
    targets[MAIN_PROCESS_UNIT_COLUMN] = targets["unit_id"].where(is_eqp_route, "UNKNOWN")
    targets[MAIN_PROCESS_TRACE_SOURCE_COLUMN] = is_eqp_route.map(
        {True: "measurement_unit_fallback", False: "unmatched_chamber"}
    )
    targets[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.NaT

    if not history_df.empty:
        history = history_df.copy()
        history[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.to_datetime(
            history[MAIN_PROCESS_EVENT_TIME_COLUMN], errors="coerce"
        )
        candidates = targets[_TRACE_TARGET_COLUMNS + ["_trace_target_id"]].merge(
            history,
            on=_HISTORY_MATCH_COLUMNS,
            how="inner",
            validate="many_to_many",
        )
        candidates = candidates[
            candidates[MAIN_PROCESS_EVENT_TIME_COLUMN] <= candidates["sheet_start_time"]
        ].copy()
        if not candidates.empty:
            selected = (
                candidates.sort_values(
                    ["_trace_target_id", "source_rank", MAIN_PROCESS_EVENT_TIME_COLUMN],
                    ascending=[True, True, False],
                )
                .drop_duplicates("_trace_target_id", keep="first")
                [[
                    "_trace_target_id",
                    MAIN_PROCESS_UNIT_COLUMN,
                    MAIN_PROCESS_EVENT_TIME_COLUMN,
                    MAIN_PROCESS_TRACE_SOURCE_COLUMN,
                ]]
            )
            targets = targets.drop(
                columns=[
                    MAIN_PROCESS_UNIT_COLUMN,
                    MAIN_PROCESS_EVENT_TIME_COLUMN,
                    MAIN_PROCESS_TRACE_SOURCE_COLUMN,
                ]
            ).merge(selected, on="_trace_target_id", how="left", validate="one_to_one")

            missing_trace = targets[MAIN_PROCESS_UNIT_COLUMN].isna()
            route_is_eqp = targets["main_eqp_type"].eq("EQP")
            targets.loc[missing_trace, MAIN_PROCESS_UNIT_COLUMN] = targets.loc[
                missing_trace, "unit_id"
            ].where(route_is_eqp[missing_trace], "UNKNOWN")
            targets.loc[missing_trace, MAIN_PROCESS_TRACE_SOURCE_COLUMN] = route_is_eqp[
                missing_trace
            ].map({True: "measurement_unit_fallback", False: "unmatched_chamber"})

    trace_columns = [
        "_trace_target_id",
        MAIN_PROCESS_UNIT_COLUMN,
        MAIN_PROCESS_EVENT_TIME_COLUMN,
        MAIN_PROCESS_TRACE_SOURCE_COLUMN,
    ]
    return (
        result.merge(targets[trace_columns], on="_trace_target_id", how="left", validate="many_to_one")
        .drop(columns="_trace_target_id")
    )
