"""Cache native report payloads; user filters do not trigger workbook/SQL reads."""

from datetime import date

import streamlit as st

from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.shared_kernel.config import ConfigLoader


def cached_chamber_report(
    service: ChamberQTimeService, *, as_of: date,
) -> dict:
    return _cached_chamber_payload(service, signature=service.cache_signature(), as_of=as_of)


@st.cache_data(show_spinner=False, max_entries=8, ttl=ConfigLoader.get_cache_ttl_seconds())
def _cached_chamber_payload(
    _service: ChamberQTimeService, *, signature: tuple, as_of: date, version: int = 1,
) -> dict:
    return _service.load(as_of=as_of)


def clear_chamber_cache() -> None:
    _cached_chamber_payload.clear()


def get_chamber_cached_funcs() -> list:
    return [_cached_chamber_payload]
