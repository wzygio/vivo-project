"""Pure preprocessing for main-process routing and history matching."""

from __future__ import annotations

import pandas as pd

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
        raise ValueError(
            f"Specifications missing main-process columns: {sorted(missing_spec_columns)}"
        )

    routes = spec_df[required_spec_columns].copy()
    if routes.duplicated(_SPEC_KEY_COLUMNS).any():
        raise ValueError(
            "Main-process specifications must be unique by product, step, and parameter"
        )

    result = result.merge(routes, on=_SPEC_KEY_COLUMNS, how="left", validate="many_to_one")
    normalized_steps = result["main_step_id"].astype("string").str.strip()
    result["main_step_id"] = normalized_steps.mask(normalized_steps.eq("")).fillna(
        result["step_id"].astype(str)
    )
    normalized_route_types = (
        result["main_eqp_type"].astype("string").str.strip().str.upper()
    )
    result["main_eqp_type"] = normalized_route_types.where(
        normalized_route_types.isin({"EQP", "CHAMBER"}), "EQP"
    )
    return result


def apply_main_process_history(
    measurements_df: pd.DataFrame,
    history_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the nearest preceding main-process history without expanding point rows."""
    missing_columns = set(_TRACE_TARGET_COLUMNS) - set(measurements_df.columns)
    if missing_columns:
        raise ValueError(
            f"Measurements missing main-process trace columns: {sorted(missing_columns)}"
        )

    result = measurements_df.drop(
        columns=[
            MAIN_PROCESS_UNIT_COLUMN,
            MAIN_PROCESS_EVENT_TIME_COLUMN,
            MAIN_PROCESS_TRACE_SOURCE_COLUMN,
        ],
        errors="ignore",
    ).copy()
    result["sheet_start_time"] = pd.to_datetime(
        result["sheet_start_time"], errors="coerce"
    )
    result["_trace_target_id"] = pd.factorize(
        pd.MultiIndex.from_frame(result[_TRACE_TARGET_COLUMNS])
    )[0]

    targets = result.drop_duplicates("_trace_target_id")[
        _TRACE_TARGET_COLUMNS + ["_trace_target_id"]
    ].copy()
    is_eqp_route = targets["main_eqp_type"].eq("EQP")
    targets[MAIN_PROCESS_UNIT_COLUMN] = targets["unit_id"].where(
        is_eqp_route, "UNKNOWN"
    )
    targets[MAIN_PROCESS_TRACE_SOURCE_COLUMN] = is_eqp_route.map(
        {True: "measurement_unit_fallback", False: "unmatched_chamber"}
    )
    targets[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.NaT

    if not history_df.empty:
        history = history_df.copy()
        history[MAIN_PROCESS_EVENT_TIME_COLUMN] = pd.to_datetime(
            history[MAIN_PROCESS_EVENT_TIME_COLUMN], errors="coerce"
        )
        candidates = targets[
            _TRACE_TARGET_COLUMNS + ["_trace_target_id"]
        ].merge(
            history,
            on=_HISTORY_MATCH_COLUMNS,
            how="inner",
            validate="many_to_many",
        )
        candidates = candidates[
            candidates[MAIN_PROCESS_EVENT_TIME_COLUMN]
            <= candidates["sheet_start_time"]
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
            targets.loc[
                missing_trace, MAIN_PROCESS_TRACE_SOURCE_COLUMN
            ] = route_is_eqp[missing_trace].map(
                {True: "measurement_unit_fallback", False: "unmatched_chamber"}
            )

    trace_columns = [
        "_trace_target_id",
        MAIN_PROCESS_UNIT_COLUMN,
        MAIN_PROCESS_EVENT_TIME_COLUMN,
        MAIN_PROCESS_TRACE_SOURCE_COLUMN,
    ]
    return result.merge(
        targets[trace_columns],
        on="_trace_target_id",
        how="left",
        validate="many_to_one",
    ).drop(columns="_trace_target_id")
