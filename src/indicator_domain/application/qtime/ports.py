"""Outbound data contract owned by the Q-Time application layer."""

from pathlib import Path
from typing import Protocol

import pandas as pd

from src.indicator_domain.application.qtime.dtos import (
    QTimeQuery,
    QTimeStepOption,
    Shop,
)


class QTimeDataPort(Protocol):
    def list_step_options(self, shop: Shop) -> tuple[QTimeStepOption, ...]: ...

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame: ...


class QTimeDecorationPort(Protocol):
    @property
    def decoration_path(self) -> Path: ...

    def load_decisions(self) -> pd.DataFrame: ...

    def save_decisions(self, decisions: pd.DataFrame) -> None: ...
