"""Explicit Total Defect parameter names shared by TT reports and alerts."""

from __future__ import annotations


AOI_TT_PARAMETER_NAMES = frozenset({"TDSUM", "DSUM_L", "DSUM_O", "TOTAL_O_L"})


def is_aoi_tt_parameter(param_name: object) -> bool:
    """A missing specification type alone does not make a measurement TT."""
    return str(param_name).strip().upper() in AOI_TT_PARAMETER_NAMES
