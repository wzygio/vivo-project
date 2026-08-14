"""Shared stateless decorated-feature pipeline for the SPC/CTQ/monitor services.

This module is the single shared computation point of the inline domain:
each module keeps its own repository<->service pair, but all of them route
Sheet OOS decoration + Sheet feature computation through the cached
``fetch_decorated_features`` function below so identical (product, scope,
window) requests hit the same cache entry across modules.

Cache-boundary rules follow ADR-0001: the returned dict only contains
native DataFrames / dicts / strings / booleans.
"""

from __future__ import annotations

import logging

import pandas as pd
import streamlit as st

from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.spc.ports import SpcDataPort
from src.inline_domain.application.spc.spc_data_decoration import (
    _preprocess_sheet_features_by_type,
    prepare_decorated_spc_data,
)

logger = logging.getLogger(__name__)

SCOPE_SPC = "spc"
SCOPE_CTQ = "ctq"
SCOPE_NONE = "none"

# scope -> data_type filter used when pulling prepared measurements:
# spc/ctq scopes fetch only their own parameter type (the existing contract
# of the two report services); "none" performs no filtering (the caller,
# e.g. the monitor AOI group, decides which rows to feed in).
_DATA_TYPE_FILTER_BY_SCOPE = {
    SCOPE_SPC: "SPC",
    SCOPE_CTQ: "CTQ",
    SCOPE_NONE: "ALL",
}


class InMemoryFeaturesSource:
    """SpcDataPort-shaped adapter over pre-fetched prepared data.

    The monitor already pulls ALL prepared measurements per product and then
    groups them by ``data_type``; this adapter serves a single group (plus the
    product spec limits) to ``fetch_decorated_features`` without re-fetching.
    The window filter inside the cached function is idempotent on
    already-windowed data.
    """

    def __init__(self, measurements_df: pd.DataFrame, spec_df: pd.DataFrame) -> None:
        self._measurements_df = measurements_df
        self._spec_df = spec_df

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        return self._measurements_df.copy()

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return self._spec_df.copy()


def _empty_features_payload(spec_empty: bool = True) -> dict[str, object]:
    return {
        "sheet_features_df": pd.DataFrame(),
        "original_sheet_features_df": pd.DataFrame(),
        "raw_measurements_df": pd.DataFrame(),
        "original_raw_measurements_df": pd.DataFrame(),
        "spec_empty": spec_empty,
        "sheet_oos_decoration": None,
    }


@st.cache_data(show_spinner=False, max_entries=12, ttl=4 * 60 * 60)
def fetch_decorated_features(
    _features_source: SpcDataPort,
    prod_code: str,
    scope: str,
    start_date: str,
    end_date: str,
    snapshot_signature: str = "",
) -> dict[str, object]:
    """Fetch prepared measurements, apply the scope's decoration calibre, compute features.

    Cache key = (prod_code, scope, start_date, end_date, snapshot_signature);
    ``_features_source`` is underscore-prefixed and therefore excluded from
    hashing (same pattern as the existing ``_db_manager``/``_data_port``
    arguments). Identical windows share one cache entry across modules;
    different windows cache separately (correctness first).

    ``scope`` selects the decoration calibre:
    - ``"spc"``: ``resources/spc_sheet_oos_decoration.xlsx`` (sheet = product);
    - ``"ctq"``: ``resources/ctq_sheet_oos_decoration.xlsx`` (sheet = product,
      missing sheet = empty decoration semantics, handled by the engine);
    - ``"none"``: decoration skipped entirely, only preprocess feature
      computation (same exemption as aoi_tt).

    Audit-file persistence: the underlying ``prepare_decorated_*`` wrappers
    run with ``persist_files=True`` so the user-maintained decoration
    workbook is (re)written once per cache miss; cache hits return the
    computed payload without rewriting the workbook.

    Returns a native-payload dict (ADR-0001): decorated/original
    sheet_features_df, decorated/original raw_measurements_df, ``spec_empty``
    flag, and the decoration payload (decoration_df / decoration_path /
    decoration_sheet) or None.
    """
    normalized_scope = (scope or "").strip().lower()
    if normalized_scope not in _DATA_TYPE_FILTER_BY_SCOPE:
        raise ValueError(f"unknown decoration scope: {scope!r}")

    fetch_config = SpcQueryConfig(
        prod_code=prod_code,
        start_date=start_date,
        end_date=end_date,
        data_type_filter=_DATA_TYPE_FILTER_BY_SCOPE[normalized_scope],
    )
    measurements_df = _features_source.get_spc_measurements(fetch_config)
    spec_df = _features_source.get_spc_spec_limits(prod_code)
    if measurements_df.empty:
        return _empty_features_payload(spec_empty=spec_df.empty)

    if "sheet_start_time" in measurements_df.columns:
        measurements_df = measurements_df.copy()
        measurements_df["sheet_start_time"] = pd.to_datetime(
            measurements_df["sheet_start_time"], errors="coerce"
        )
        start_dt = pd.to_datetime(start_date, errors="coerce")
        end_dt = pd.to_datetime(end_date, errors="coerce") + pd.Timedelta(days=1)
        measurements_df = measurements_df[
            (measurements_df["sheet_start_time"] >= start_dt)
            & (measurements_df["sheet_start_time"] < end_dt)
        ].copy()
        if measurements_df.empty:
            return _empty_features_payload(spec_empty=spec_df.empty)

    if normalized_scope == SCOPE_NONE:
        # 免修饰口径：只做 preprocess 特征计算（与 aoi_tt 一致）。
        features_df = _preprocess_sheet_features_by_type(measurements_df, spec_df)
        return {
            "sheet_features_df": features_df,
            "original_sheet_features_df": features_df,
            "raw_measurements_df": measurements_df,
            "original_raw_measurements_df": measurements_df.copy(),
            "spec_empty": spec_df.empty,
            "sheet_oos_decoration": None,
        }

    original_raw_measurements_df = measurements_df.copy()
    if normalized_scope == SCOPE_SPC:
        decorated_data = prepare_decorated_spc_data(
            raw_measurements_df=measurements_df,
            spec_df=spec_df,
            prod_code=prod_code,
            persist_files=True,
        )
        original_features_df = decorated_data.original_sheet_features_df
    else:
        # 延迟导入：ctq 包 __init__ 依赖 ctq_service，而 ctq_service 依赖本模块，
        # 顶层导入会形成循环。
        from src.inline_domain.application.ctq.ctq_data_decoration import (
            prepare_decorated_ctq_data,
        )

        decorated_data = prepare_decorated_ctq_data(
            raw_measurements_df=measurements_df,
            spec_df=spec_df,
            prod_code=prod_code,
            persist_decoration=True,
        )
        original_features_df = _preprocess_sheet_features_by_type(
            original_raw_measurements_df, spec_df
        )

    decoration_result = decorated_data.sheet_oos_decoration_result
    logger.info(
        "[shared] decorated features prepared: prod=%s scope=%s features=%s",
        prod_code,
        normalized_scope,
        len(decorated_data.sheet_features_df),
    )
    return {
        "sheet_features_df": decorated_data.sheet_features_df,
        "original_sheet_features_df": original_features_df,
        "raw_measurements_df": decorated_data.raw_measurements_df,
        "original_raw_measurements_df": original_raw_measurements_df,
        "spec_empty": spec_df.empty,
        "sheet_oos_decoration": {
            "decoration_df": decoration_result.decoration_df,
            "decoration_path": str(decoration_result.decoration_path),
            "decoration_sheet": decoration_result.decoration_sheet,
        },
    }
