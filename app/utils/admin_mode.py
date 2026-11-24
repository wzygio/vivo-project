"""Recognize the portal's administrator display-mode URLs."""

import streamlit as st


def is_admin_mode(params=None) -> bool:
    params = st.query_params if params is None else params
    return params.get("admin") in ("true", "ture")
