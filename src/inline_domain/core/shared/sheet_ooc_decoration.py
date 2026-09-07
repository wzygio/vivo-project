"""Pure builders for real Inline out-of-control (OOC) facts.

OOC is lower priority than OOS: a row outside a specification limit is not
also emitted as OOC.  Missing control limits never create an alert.
"""

from __future__ import annotations

import pandas as pd

OOC_KEY_COLUMNS = ["prod_code", "step_id", "param_name", "sheet_id"]
OOC_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "param_name",
    "sheet_id",
    "sheet_start_time",
    "sheet_max",
    "sheet_min",
    "sheet_mean",
    "ucl",
    "lcl",
    "ooc_type",
]

AOI_TT_OOC_KEY_COLUMNS = ["prod_code", "step_id", "tt_name", "sheet_id"]
AOI_TT_OOC_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "tt_name",
    "sheet_id",
    "lot_id",
    "start_time",
    "tt_qty",
    "ucl",
    "ooc_type",
]

SCOPE_OOC_DECORATION_FILE_NAME = {
    "spc": "spc_sheet_ooc_decoration.xlsx",
    "ctq": "ctq_sheet_ooc_decoration.xlsx",
    "aoi_tt": "aoi_tt_sheet_ooc_decoration.xlsx",
    "aoi_rs": "aoi_rs_sheet_ooc_decoration.xlsx",
}


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def build_sheet_ooc_detail(sheet_features_df: pd.DataFrame) -> pd.DataFrame:
    """Build SPC/CTQ OOC facts from Sheet Mean and real UCL/LCL values."""
    required = {
        "factory", *OOC_KEY_COLUMNS, "sheet_start_time", "sheet_max",
        "sheet_min", "sheet_mean", "usl", "lsl", "ucl", "lcl",
    }
    if sheet_features_df.empty or not required.issubset(sheet_features_df.columns):
        return _empty(OOC_DETAIL_COLUMNS)

    frame = sheet_features_df.copy()
    numeric = ["sheet_max", "sheet_min", "sheet_mean", "usl", "lsl", "ucl", "lcl"]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    upper_ooc = frame["ucl"].notna() & (frame["sheet_mean"] > frame["ucl"])
    lower_ooc = frame["lcl"].notna() & (frame["sheet_mean"] < frame["lcl"])
    upper_oos = frame["usl"].notna() & (frame["sheet_max"] > frame["usl"])
    lower_oos = frame["lsl"].notna() & (frame["sheet_min"] < frame["lsl"])
    selected = frame.loc[(upper_ooc | lower_ooc) & ~(upper_oos | lower_oos)].copy()
    if selected.empty:
        return _empty(OOC_DETAIL_COLUMNS)
    selected["ooc_type"] = "UCL"
    selected.loc[lower_ooc.loc[selected.index], "ooc_type"] = "LCL"
    both = upper_ooc.loc[selected.index] & lower_ooc.loc[selected.index]
    selected.loc[both, "ooc_type"] = "UCL/LCL"
    for column in OOC_KEY_COLUMNS:
        selected[column] = selected[column].fillna("").astype(str)
    return (
        selected.reindex(columns=OOC_DETAIL_COLUMNS)
        .sort_values(["factory", "step_id", "param_name", "sheet_start_time", "sheet_id"], kind="stable")
        .reset_index(drop=True)
    )


def build_aoi_tt_ooc_detail(
    tt_details_df: pd.DataFrame, spec_df: pd.DataFrame
) -> pd.DataFrame:
    """Build AOI_TT OOC facts from UCL while excluding rows over USL."""
    required_details = {"factory", *AOI_TT_OOC_KEY_COLUMNS, "lot_id", "start_time", "tt_qty"}
    required_specs = {"step_id", "tt_name", "ucl", "usl"}
    if (
        tt_details_df.empty
        or spec_df.empty
        or not required_details.issubset(tt_details_df.columns)
        or not required_specs.issubset(spec_df.columns)
    ):
        return _empty(AOI_TT_OOC_DETAIL_COLUMNS)
    specs = spec_df[["step_id", "tt_name", "ucl", "usl"]].copy()
    specs["ucl"] = pd.to_numeric(specs["ucl"], errors="coerce")
    specs["usl"] = pd.to_numeric(specs["usl"], errors="coerce")
    specs = specs.drop_duplicates(["step_id", "tt_name"], keep="first")
    merged = tt_details_df.merge(specs, on=["step_id", "tt_name"], how="inner")
    values = pd.to_numeric(merged["tt_qty"], errors="coerce")
    mask = merged["ucl"].notna() & (values > merged["ucl"])
    mask &= merged["usl"].isna() | (values <= merged["usl"])
    selected = merged.loc[mask].copy()
    if selected.empty:
        return _empty(AOI_TT_OOC_DETAIL_COLUMNS)
    selected["ooc_type"] = "UCL"
    for column in AOI_TT_OOC_KEY_COLUMNS:
        selected[column] = selected[column].fillna("").astype(str)
    return (
        selected.reindex(columns=AOI_TT_OOC_DETAIL_COLUMNS)
        .sort_values(["factory", "step_id", "tt_name", "start_time", "sheet_id"], kind="stable")
        .reset_index(drop=True)
    )


def build_aoi_rs_ooc_detail() -> pd.DataFrame:
    """Return no facts until AOI_RS exposes a genuine control-limit source."""
    return pd.DataFrame(
        columns=[
            "factory", "prod_code", "step_id", "rs_code", "point_id",
            "sheet_start_time", "value", "ucl", "lcl", "ooc_type",
        ]
    )
