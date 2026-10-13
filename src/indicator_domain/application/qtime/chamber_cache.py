"""Cache native date-window payloads; product/line/chamber filters reuse results."""

from datetime import date

import streamlit as st

from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.shared_kernel.config import ConfigLoader


def cached_chamber_report(
    service: ChamberQTimeService, *, as_of: date,
    start_date: date | None = None, end_date: date | None = None,
) -> dict:
    return _cached_chamber_payload(service, signature=service.cache_signature(), as_of=as_of,
                                  start_date=start_date, end_date=end_date)


@st.cache_data(show_spinner=False, max_entries=8, ttl=ConfigLoader.get_cache_ttl_seconds())
def _cached_chamber_payload(
    _service: ChamberQTimeService, *, signature: tuple, as_of: date, version: int = 2,
    start_date: date | None = None, end_date: date | None = None,
) -> dict:
    return _service.load(as_of=as_of, start_date=start_date, end_date=end_date)


def clear_chamber_cache() -> None:
    _cached_chamber_payload.clear()


def get_chamber_cached_funcs() -> list:
    return [_cached_chamber_payload]
