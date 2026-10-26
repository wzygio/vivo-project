"""Automatic alert publication from the already loaded full parts report."""

import logging
from collections.abc import Callable

import pandas as pd
import streamlit as st

from app.sections.equipment_domain.parts_dashboard import render_parts_alert_center
from src.equipment_domain.application.parts_alert_service import publish_parts_alerts
from src.equipment_domain.application.ports import PartsAlertStore

logger = logging.getLogger(__name__)


def render_parts_alerts(
    report_df: pd.DataFrame, *, store_factory: Callable[[], PartsAlertStore],
    as_of_date: str, last_update: str,
) -> None:
    try:
        alerts = publish_parts_alerts(
            report_df, store=store_factory(), as_of_date=as_of_date, last_update=last_update,
        )
    except Exception:
        logger.exception('Critical-parts automatic alert publication failed')
        with st.expander('超规备件预警中心', expanded=True):
            st.error('备件预警数据暂时无法更新，请稍后重试。')
        return
    render_parts_alert_center(alerts, has_source_data=not report_df.empty)
