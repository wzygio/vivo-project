"""Keep dashboard Mapping configuration stable between explicit refreshes."""

from copy import deepcopy
import math

import streamlit as st

from src.shared_kernel.config_model import AppConfig
from src.shared_kernel.config import ConfigLoader
from app.components.code_selector import DEFAULT_MONTHLY_RATE_THRESHOLD
from yield_domain.application.excel_service import ExcelService


def get_code_monthly_rate_threshold() -> float:
    """Load the shared image visibility threshold as a fractional monthly rate."""
    dashboard = ConfigLoader.load_domain_config("yield_domain").get("dashboard", {})
    if not isinstance(dashboard, dict):
        raise ValueError("Yield dashboard settings must be a mapping")
    value = dashboard.get("code_monthly_rate_threshold", DEFAULT_MONTHLY_RATE_THRESHOLD)
    if isinstance(value, bool):
        raise ValueError("Code monthly rate threshold must be a number in [0, 1]")
    try:
        threshold = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Code monthly rate threshold must be a number in [0, 1]") from exc
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Code monthly rate threshold must be a number in [0, 1]")
    return threshold


def get_dashboard_resource_config(
    config: AppConfig, resource_revision: str
) -> AppConfig:
    """Read Mapping once per product/revision without mutating the session config."""
    state_key = f"yield_dashboard_mapping_config_{config.data_source.product_code}"
    snapshot = st.session_state.get(state_key)
    result = config.model_copy(deep=True)
    if snapshot is None or snapshot["revision"] != resource_revision:
        ExcelService.inject_mapping_config_to_config(result)
        snapshot = {
            "revision": resource_revision,
            "scripts": deepcopy(result.processing.get("mapping_hotspot_script", [])),
        }
        st.session_state[state_key] = snapshot
    result.processing["mapping_hotspot_script"] = deepcopy(snapshot["scripts"])
    return result
