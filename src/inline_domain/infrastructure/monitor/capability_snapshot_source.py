"""Read-only weekly capability denominators from verified measurement snapshots.

Metadata is supplied by the caller. This adapter cannot refresh snapshots, query
a database, or publish decisions. Equipment history only decorates trace columns;
it is unnecessary for the product/factory/station/parameter capability identity.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import hashlib
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.spc.spc_service import exclude_cpm_cpk_parameters
from src.inline_domain.core.spc.spc_calculator import build_period_capability_report
from src.inline_domain.infrastructure.shared.date_exclusion import apply_inline_date_exclusion
from src.inline_domain.infrastructure.shared.main_process_history_repository import HISTORY_OUTPUT_COLUMNS
from src.inline_domain.infrastructure.shared.measurement_preparation import InlineMeasurementPreparationRepository
from src.inline_domain.infrastructure.shared.measurement_preprocessor import (
    filter_excluded_steps, filter_invalid_measurement_values, MEASUREMENT_VALIDITY_SIGNATURE,
)
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
from src.inline_domain.infrastructure.shared.outlier_filter_rules import apply_outlier_filter_rules
from src.inline_domain.infrastructure.shared.rolling_snapshot import read_metadata, snapshot_process_lock
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_paths import snapshot_component

TOTAL_COLUMNS = ["prod_code", "period_label", "total_projects"]
MetadataProvider = Callable[[str], Mapping[str, pd.DataFrame]]


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=64, show_spinner=False)
def _cached_totals(signature: tuple, product: str, weeks: tuple[str, ...], metric: str,
                   as_of: str, *, _source) -> pd.DataFrame:
    return _source._read_weekly_totals(product, weeks, metric=metric, as_of=as_of)


class SnapshotCapabilityInputError(RuntimeError):
    """The requested complete-week population cannot be established safely."""


class _StaticMetadata:
    def __init__(self, frames: Mapping[str, pd.DataFrame]) -> None:
        self.frames = frames

    def get_parameter_catalog(self, product: str) -> pd.DataFrame:
        return self.frames["catalog"].copy()

    def get_parameter_specs(self, product: str) -> pd.DataFrame:
        return self.frames["specs"].copy()


class _SnapshotFacts:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def get_measurements(self, product: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
        if force_refresh:
            raise SnapshotCapabilityInputError("Offline capability inputs cannot refresh snapshots")
        return self.frame.copy()


class _NoTraceHistory:
    def get_main_process_history(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(columns=HISTORY_OUTPUT_COLUMNS)


class _Preparation(InlineMeasurementPreparationRepository):
    def __init__(self, *args, outlier_rules: pd.DataFrame | None = None) -> None:
        super().__init__(*args)
        self.outlier_rules = outlier_rules

    def _apply_outlier_filters(self, frame: pd.DataFrame, product: str) -> pd.DataFrame:
        if self.outlier_rules is None:
            return super()._apply_outlier_filters(frame, product)
        return apply_outlier_filter_rules(frame, product, self.outlier_rules)


def _week_start(label: str) -> pd.Timestamp:
    try:
        year, week = str(label).split("-W")
        start = pd.Timestamp.fromisocalendar(int(year), int(week), 1)
        if label != f"{start.isocalendar().year}-W{start.isocalendar().week:02d}":
            raise ValueError("Noncanonical ISO week")
        return start
    except (TypeError, ValueError) as exc:
        raise SnapshotCapabilityInputError(f"Invalid complete-week label: {label}") from exc


class SnapshotCapabilitySource:
    """Reuse the final SPC point pipeline without its source/publishing effects."""

    def __init__(
        self, snapshot_dir: Path | str, metadata_provider: MetadataProvider, *,
        decoration_port=None, outlier_rules: pd.DataFrame | None = None,
    ) -> None:
        self.snapshot_dir = Path(snapshot_dir)
        self.metadata_provider = metadata_provider
        self.decoration_port = decoration_port
        self.outlier_rules = outlier_rules

    def read_weekly_totals(
        self, product: str, weeks: Iterable[str], *, metric: str, as_of: object,
    ) -> pd.DataFrame:
        labels = tuple(dict.fromkeys(weeks))
        provider_signature = getattr(self.metadata_provider, "source_signature", None)
        if provider_signature is None or self.decoration_port is not None or self.outlier_rules is not None:
            return self._read_weekly_totals(product, labels, metric=metric, as_of=as_of)
        signature = self._product_signature(product)
        return _cached_totals(signature, product, labels, metric,
                              pd.Timestamp(as_of).date().isoformat(), _source=self)

    def source_signature(self, products: Iterable[str]) -> str:
        """Bind historical results to verified source/policy generations."""
        if self.decoration_port is not None or self.outlier_rules is not None:
            raise SnapshotCapabilityInputError("Injected inputs need a caller-owned source identity")
        signature = [self._product_signature(product) for product in sorted(set(products))]
        return hashlib.sha256(json.dumps(signature, ensure_ascii=True).encode()).hexdigest()

    def _product_signature(self, product: str) -> tuple:
        provider_signature = getattr(self.metadata_provider, "source_signature", None)
        if provider_signature is None:
            raise SnapshotCapabilityInputError("Verified metadata source identity unavailable")
        root = ConfigLoader.get_project_root()
        snapshot = self.snapshot_dir / f"inline_measurements_{product}.parquet"
        paths = [snapshot, snapshot.with_suffix(".snapshot.json"), snapshot.with_suffix(".policy"),
                 root / "config/global.yaml", root / "config/domain/inline_domain.yaml",
                 root / "config/products" / f"{product}.yaml"]
        for key, default in (("spc_sheet_oos_decoration", "spc_sheet_oos_decoration.xlsx"),
                             ("spc_outlier_filters", "spc_outlier_filters.csv")):
            paths.append(ConfigLoader.get_domain_resource_path("inline_domain", key, default))
        signatures = []
        for path in paths:
            try:
                stat = path.stat()
                signatures.append((str(path.resolve()), stat.st_mtime_ns, stat.st_size))
            except FileNotFoundError:
                signatures.append((str(path.resolve()), "missing"))
        if product == "Z622":
            from src.inline_domain.infrastructure.spc.spc_repository import spc_input_signature

            signatures.append(spc_input_signature(root, product))
        return (MEASUREMENT_VALIDITY_SIGNATURE, provider_signature([product]), *signatures)

    def _read_weekly_totals(
        self, product: str, weeks: Iterable[str], *, metric: str, as_of: object,
    ) -> pd.DataFrame:
        """Count computable projects; the 48-hour rule gates alerts, not totals."""
        if metric not in {"cpk", "cpm"}:
            raise ValueError(f"Unsupported capability metric: {metric}")
        labels = list(dict.fromkeys(weeks))
        if not labels:
            return pd.DataFrame(columns=TOTAL_COLUMNS)
        today = pd.Timestamp(as_of).normalize()
        starts = [_week_start(label) for label in labels]
        current_monday = today - pd.Timedelta(days=today.weekday())
        if any(start >= current_monday for start in starts):
            raise SnapshotCapabilityInputError("Only completed ISO weeks may be recomputed")
        points = self._snapshot_points(product, min(starts), max(starts) + pd.Timedelta(days=6), today)
        if points.empty:
            return pd.DataFrame([[product, label, 0] for label in labels], columns=TOTAL_COLUMNS)
        metadata = self.metadata_provider(product)
        if (not {"catalog", "specs"}.issubset(metadata)
                or any(not isinstance(metadata[name], pd.DataFrame) or metadata[name].empty
                       for name in ("catalog", "specs"))):
            raise SnapshotCapabilityInputError("Capability metadata requires catalog and specs")
        preparation = _Preparation(
            _SnapshotFacts(points), _StaticMetadata(metadata), _NoTraceHistory(),
            outlier_rules=self.outlier_rules,
        )
        query = SpcQueryConfig(prod_code=product, start_date=min(starts).date().isoformat(),
                               end_date=today.date().isoformat(), data_type_filter="SPC")
        prepared = preparation.get_prepared_measurements(query)
        specs = preparation.get_spec_limits(product)
        if not prepared.empty and specs.empty:
            raise SnapshotCapabilityInputError(f"SPC specifications unavailable for {product}")
        final = prepare_decorated_data(
            prepared, specs, product, "spc", persist=False,
            decoration_port=self.decoration_port,
            spc_point_policy=ConfigLoader.get_spc_point_decoration_policy(),
        )
        exemptions = ConfigLoader.get_spc_capability_param_exemptions()
        features = exclude_cpm_cpk_parameters(final.sheet_features_df, exemptions)
        measurements = exclude_cpm_cpk_parameters(final.raw_measurements_df, exemptions)
        records = []
        for label, start in zip(labels, starts):
            capability = build_period_capability_report(
                features, end_date=(start + pd.Timedelta(days=7)).date(),
                raw_measurements=measurements,
                sigma_source=ConfigLoader.get_spc_period_sigma_source(), previous_week_only=True,
            )
            count = 0 if capability.empty else int(pd.to_numeric(capability[metric], errors="coerce").notna().sum())
            records.append([product, label, count])
        return pd.DataFrame(records, columns=TOTAL_COLUMNS)

    def _snapshot_points(
        self, product: str, first: pd.Timestamp, last: pd.Timestamp, as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        path = self.snapshot_dir / f"inline_measurements_{snapshot_component(product)}.parquet"
        policy = ConfigLoader.get_data_forward_policy()
        source_first, source_last = policy.to_source_window(first, last)
        source_cutoff = ConfigLoader.get_report_cutoff_policy().boundary(as_of) - pd.Timedelta(days=policy.effective_days)
        with snapshot_process_lock(path):
            metadata = read_metadata(path, InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION, policy_file=True)
            if (metadata is None or pd.Timestamp(metadata["covered_from"]) > source_first
                    or pd.Timestamp(metadata["covered_through"]) < source_last):
                raise SnapshotCapabilityInputError(f"Verified raw snapshot does not cover the requested weeks for {product}")
            frame = pd.read_parquet(path, filters=[("start_time", ">=", source_first), ("start_time", "<=", source_cutoff)])
            if read_metadata(path, InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION, policy_file=True) != metadata:
                raise SnapshotCapabilityInputError("Measurement snapshot generation changed during read")
        frame = filter_invalid_measurement_values(filter_excluded_steps(frame))
        if not frame.empty and not frame["prod_code"].eq(product).all():
            raise SnapshotCapabilityInputError("Measurement snapshot contains another product")
        shifted = policy.shift_frame(frame, ("start_time",))
        cutoff = ConfigLoader.get_report_cutoff_policy().filter_frame(shifted, "start_time", now=as_of)
        return apply_inline_date_exclusion(cutoff)
