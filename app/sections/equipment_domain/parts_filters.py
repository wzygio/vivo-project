"""关键备件页面的筛选与行选择交互。"""

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd
import streamlit as st


def _sorted_values(dataframe: pd.DataFrame, column: str) -> list[str]:
    if column not in dataframe.columns:
        return []
    values = dataframe[column].dropna().astype(str).str.strip()
    return sorted(values[values.ne("")].unique())


def build_cascading_filter_options(
    report_df: pd.DataFrame,
    *,
    selected_factories: Sequence[str],
    selected_equipment_types: Sequence[str],
) -> tuple[list[str], list[str], list[str]]:
    """按厂别、设备类型的管控顺序生成三级候选项。"""
    available_factories = _sorted_values(report_df, "厂别")
    equipment_scope = report_df.copy()
    if selected_factories:
        equipment_scope = equipment_scope[
            equipment_scope["厂别"].isin(selected_factories)
        ]
    available_equipment_types = _sorted_values(equipment_scope, "设备类型")

    part_scope = equipment_scope
    if selected_equipment_types:
        part_scope = part_scope[
            part_scope["设备类型"].isin(selected_equipment_types)
        ]
    available_part_types = _sorted_values(part_scope, "备件类型")
    return available_factories, available_equipment_types, available_part_types


def render_parts_filters(
    report_df: pd.DataFrame,
) -> tuple[list[str], list[str], list[str]]:
    """渲染厂别→设备类型→备件类型的多选级联筛选器。"""
    factory_column, equipment_column, part_column = st.columns(
        3,
        vertical_alignment="bottom",
    )
    available_factories = _sorted_values(report_df, "厂别")
    with factory_column:
        selected_factories = st.multiselect(
            "厂别",
            options=available_factories,
            default=[],
            placeholder="全部厂别",
            key="parts_filter_factories",
        )
    _, available_equipment_types, _ = build_cascading_filter_options(
        report_df,
        selected_factories=selected_factories,
        selected_equipment_types=[],
    )
    with equipment_column:
        selected_equipment_types = st.multiselect(
            "设备类型",
            options=available_equipment_types,
            default=[],
            placeholder="全部设备类型",
            key="parts_filter_equipment_types",
        )
    _, _, available_part_types = build_cascading_filter_options(
        report_df,
        selected_factories=selected_factories,
        selected_equipment_types=selected_equipment_types,
    )
    with part_column:
        selected_part_types = st.multiselect(
            "备件类型",
            options=available_part_types,
            default=[],
            placeholder="全部备件类型",
            key="parts_filter_part_types",
        )
    return selected_factories, selected_equipment_types, selected_part_types


def apply_parts_filters(
    report_df: pd.DataFrame,
    *,
    selected_factories: Sequence[str],
    selected_equipment_types: Sequence[str],
    selected_part_types: Sequence[str],
) -> pd.DataFrame:
    """按已选择维度过滤报表；每个空选择维度均视为全选。"""
    filtered_df = report_df.copy()
    filters = (
        ("厂别", selected_factories),
        ("设备类型", selected_equipment_types),
        ("备件类型", selected_part_types),
    )
    for column, selected_values in filters:
        if selected_values and column in filtered_df.columns:
            filtered_df = filtered_df[filtered_df[column].isin(selected_values)].copy()
    return filtered_df


def get_selected_parts_row(
    report_df: pd.DataFrame,
    selection_event: Any,
) -> pd.Series | None:
    """仅在用户显式选择一行时返回对应备件。"""
    if report_df.empty:
        return None
    selection = (
        selection_event.get("selection", {})
        if isinstance(selection_event, Mapping)
        else getattr(selection_event, "selection", None)
    )
    rows = (
        selection.get("rows", [])
        if isinstance(selection, Mapping)
        else getattr(selection, "rows", [])
    )
    if not rows:
        return None
    row_index = rows[0]
    if not isinstance(row_index, int) or not 0 <= row_index < len(report_df):
        return None
    return report_df.iloc[row_index]
