"""Automatic alert publication from the already loaded full parts report."""

import logging
from datetime import date
from collections.abc import Callable

import pandas as pd
import streamlit as st

from app.sections.equipment_domain.parts_dashboard import render_parts_alert_center
from src.equipment_domain.application.parts_alert_service import publish_parts_alerts
from src.equipment_domain.application.ports import PartsAlertStore
from app.sections.shared.alert_results import read_alert_result

logger = logging.getLogger(__name__)


def render_parts_alerts(
    report_df: pd.DataFrame, *, store_factory: Callable[[], PartsAlertStore],
    as_of_date: str, last_update: str,
) -> None:
    try:
        publish_parts_alerts(
            report_df, store=store_factory(), as_of_date=as_of_date, last_update=last_update,
        )
    except Exception:
        logger.exception('Critical-parts automatic alert publication failed')
        st.warning('备件预警数据暂时无法更新；以下状态以已保存结果为准。')
    render_saved_parts_alerts(date.fromisoformat(as_of_date))


def render_saved_parts_alerts(reference_date: date | None = None) -> None:
    """Display only committed alert rows, including when the report cannot load."""
    result = read_alert_result('parts_oos', '', reference_date or date.today())
    if result['state'] == 'error':
        with st.expander('超规备件预警中心', expanded=True):
            st.error(result['message'])
        return
    render_parts_alert_center(result['alerts'], has_source_data=result['state'] != 'no_data')
