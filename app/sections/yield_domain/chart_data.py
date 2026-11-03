"""Bounded native projections shared by automatic and manual Yield charts."""
import pandas as pd
import streamlit as st
import hashlib

from src.shared_kernel.config import ConfigLoader


def build_chart_data_signature(*values) -> str:
    """Hash actual small report outputs, including full periods, rather than Panel rows."""
    digest = hashlib.sha256()

    def update(value):
        if isinstance(value, pd.DataFrame):
            digest.update(repr((list(value.columns), list(map(str, value.dtypes)))).encode())
            digest.update(pd.util.hash_pandas_object(value, index=True).values.tobytes())
        elif isinstance(value, dict):
            for key in sorted(value):
                digest.update(str(key).encode())
                update(value[key])
        elif isinstance(value, (list, tuple)):
            for item in value:
                update(item)
        else:
            digest.update(repr(value).encode())

    for value in values:
        update(value)
    return digest.hexdigest()


@st.cache_data(show_spinner=False, max_entries=32, ttl=ConfigLoader.get_cache_ttl_seconds())
def prepare_lot_chart_index(lot_data: dict, data_signature: str) -> dict[str, pd.DataFrame]:
    from app.sections.yield_domain.yield_dashboard import normalize_lot_chart_rows

    frames = [frame for frame in (lot_data or {}).get('code_level_details', {}).values()
              if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not frames:
        return {}
    combined = pd.concat(frames, ignore_index=True)
    if 'defect_desc' not in combined:
        return {}
    normalized = normalize_lot_chart_rows(combined)
    return {code: frame.sort_values('warehousing_time')
            for code, frame in normalized.groupby('defect_desc', sort=False)}


@st.cache_data(show_spinner=False, max_entries=64, ttl=ConfigLoader.get_cache_ttl_seconds())
def prepare_mapping_chart_matrices(
    mapping_data: pd.DataFrame, curr_group: str, curr_code: str, hotspot_scripts: list,
    product_code: str | None, mapping_layout: dict, data_signature: str,
) -> tuple:
    from app.sections.yield_domain.yield_dashboard import _prepare_mapping_matrices

    return _prepare_mapping_matrices(mapping_data, curr_group, curr_code, hotspot_scripts,
                                     product_code, mapping_layout)
