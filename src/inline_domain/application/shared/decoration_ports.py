"""Consumer-owned contracts for mutable Inline decoration resources.

Adapters preserve existing ledger/refresh semantics. Results cross only the
uncached use-case boundary; cached report payloads remain native containers.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import OOS_DECORATION_FILE_NAME, RefreshDecision
from src.inline_domain.core.spc.cpk_decoration import CAPABILITY_METRIC_CPK


class SheetOosDecorationReadError(RuntimeError):
    """Existing user decisions could not be read safely; never infer empty."""


class SheetOosDecorationWriteError(RuntimeError):
    """The decoration result could not be persisted."""


class CapabilityDecorationReadError(RuntimeError):
    """Existing capability decisions could not be read safely."""


@dataclass(frozen=True)
class SheetOosPersistOutcome:
    decoration_df: pd.DataFrame
    decisions_df: pd.DataFrame
    refresh_decision: RefreshDecision


class SheetDecorationPort(Protocol):
    def load_sheet_oos_decisions(
        self, product_dir: Path, file_name: str = OOS_DECORATION_FILE_NAME,
        sheet_name: str | None = None, key_columns: Iterable[str] | None = None,
    ) -> pd.DataFrame: ...

    def persist_sheet_oos_decoration_outcome(
        self, product_dir: Path, detail_df: pd.DataFrame,
        file_name: str = OOS_DECORATION_FILE_NAME, sheet_name: str | None = None,
        key_columns: Iterable[str] | None = None, *, scope: str | None = None,
        prod_code: str | None = None, product_revision: str | None = None,
        decision_signature: str | None = None, now: datetime | None = None,
        force: bool = False,
    ) -> SheetOosPersistOutcome: ...


class CapabilityDecorationPort(Protocol):
    def load_capability_decoration(
        self, product_dir: Path, sheet_name: str | None = None,
        metric: str = CAPABILITY_METRIC_CPK, *, raise_on_error: bool = False,
    ) -> pd.DataFrame: ...

    def persist_capability_decoration(
        self, product_dir: Path, detail_df: pd.DataFrame,
        sheet_name: str | None = None, metric: str = CAPABILITY_METRIC_CPK,
    ) -> pd.DataFrame: ...

    def get_capability_decoration_signature(self, product_dir: Path) -> tuple[int, int]: ...


class DecisionSignaturePort(Protocol):
    def get_decision_file_stat(self, workbook_path: Path) -> tuple[int, int] | None: ...

    def load_sheet_oos_decisions(
        self, product_dir: Path, file_name: str = OOS_DECORATION_FILE_NAME,
        sheet_name: str | None = None, key_columns: Iterable[str] | None = None,
    ) -> pd.DataFrame: ...


class DecorationResourcePort(Protocol):
    def scope_resource_dir(self, scope: str, alarm_type: str = "oos") -> Path: ...


class DecorationPort(
    SheetDecorationPort, CapabilityDecorationPort, DecisionSignaturePort,
    DecorationResourcePort, Protocol,
):
    """Composite used only for the production compatibility default."""
