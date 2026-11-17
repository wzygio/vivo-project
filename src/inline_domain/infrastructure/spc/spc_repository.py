"""SPC projection and the Z622 PDS source exception over shared preparation."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text
from fr_file_decryption import decrypt_file, is_encrypted_file

from src.inline_domain.application.ports.measurement_snapshot import (
    MeasurementPreparationPort,
)
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
    InlineMeasurementSnapshotRepository,
)
from src.inline_domain.infrastructure.shared.measurement_preprocessor import (
    EXCLUDED_MEASUREMENT_STEP_ID,
)
from src.shared_kernel.snapshot_paths import inline_measurement_directory, snapshot_component
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.shared.measurement_data_loader import RAW_MEASUREMENT_COLUMNS
from src.inline_domain.infrastructure.shared.measurement_metadata_loader import SPEC_COLUMNS
from src.inline_domain.infrastructure.shared.rolling_snapshot import read_metadata, publish_snapshots
from src.inline_domain.core.shared.measurement_correction import apply_spc_value_corrections

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

Z622_PRODUCT = "Z622"
Z622_LOT = "L3ZB6BE03"
Z622_SOURCE_START = pd.Timestamp("2026-10-01")
Z622_SOURCE_POLICY = "z622-array-pds-v1-measurements-only"


def _monitoring_path() -> Path:
    return ConfigLoader.get_domain_resource_path("inline_domain", "spc_z622_monitoring")


def _z622_source_signature() -> str:
    path = _monitoring_path()
    configuration = _z622_monitoring().sort_values(["step_id", "param_name"]).to_csv(index=False)
    digest = hashlib.sha256(configuration.encode("utf-8")).hexdigest()
    return f"{Z622_SOURCE_POLICY}:{Z622_LOT}:{path.resolve()}:{digest}"


def _z622_monitoring() -> pd.DataFrame:
    """Read the maintained pair whitelist; absence/invalidity is an input failure."""
    path = _monitoring_path().resolve()
    stat = path.stat()
    return _read_z622_monitoring(str(path), f"{stat.st_mtime_ns}:{stat.st_size}").copy()


@lru_cache(maxsize=8)
def _read_z622_monitoring(source: str, signature: str) -> pd.DataFrame:
    path = Path(source)
    decrypted_path = None
    try:
        if is_encrypted_file(path):
            result = decrypt_file(
                path, output_dir=ConfigLoader.get_project_root() / "output" / "decrypted_files",
                strategy="excel-values",
            )
            decrypted_path = Path(result.output_path)
            path = decrypted_path
        frame = pd.read_excel(path, sheet_name="monitoring", dtype=str)
    finally:
        if decrypted_path is not None:
            decrypted_path.unlink(missing_ok=True)
    columns = ["factory", "step_id", "param_name", "data_type"]
    if frame.empty or not set(columns).issubset(frame.columns):
        raise ValueError("Z622 monitoring configuration is empty or missing columns")
    frame = frame[columns].copy()
    for column in columns:
        frame[column] = frame[column].astype("string").str.strip()
    if frame.isna().any().any() or frame.eq("").any().any():
        raise ValueError("Z622 monitoring configuration contains blank identities")
    frame["factory"] = frame["factory"].str.upper()
    frame["data_type"] = frame["data_type"].str.upper()
    if (not frame.factory.isin(["ARRAY", "OLED", "TP"]).all()
            or not frame.data_type.eq("SPC").all()
            or frame.step_id.eq(EXCLUDED_MEASUREMENT_STEP_ID).any()
            or frame.duplicated(["factory", "step_id", "param_name"]).any()):
        raise ValueError("Z622 monitoring configuration contains invalid or duplicate pairs")
    array = frame.loc[frame.factory.eq("ARRAY")].reset_index(drop=True)
    if array.empty:
        raise ValueError("Z622 monitoring configuration contains no ARRAY pairs")
    return array


def load_z622_measurements(
    db_manager: DatabaseManager, start_date: str, end_date: str, prod_code: str,
) -> pd.DataFrame:
    """Fetch bounded daily PDS windows; shared preparation keeps latest points."""
    if prod_code != Z622_PRODUCT:
        raise ValueError("PDS source is restricted to Z622")
    signature = _z622_source_signature()
    monitoring = _z622_monitoring()
    start = max(pd.Timestamp(start_date).normalize(), Z622_SOURCE_START)
    end = pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1)
    if start >= end:
        empty = pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)
        empty.attrs["z622_source_signature"] = signature
        return empty
    if db_manager.engine is None:
        raise ValueError("Database engine is not initialized")
    pair_predicates = []
    params = {"prod_code": Z622_PRODUCT, "lot_id": Z622_LOT,
              "lot_prefix": Z622_LOT + "%"}
    for i, row in enumerate(monitoring.itertuples(index=False)):
        pair_predicates.append(f"(p.step_id = :step_{i} AND p.param_name = :param_{i})")
        params[f"step_{i}"] = row.step_id
        params[f"param_{i}"] = row.param_name
    sql = text("""
        SELECT 'ARRAY' AS factory, :prod_code AS prod_code,
               p.glass_start_time AS start_time, p.glass_id AS sheet_id,
               :lot_id AS lot_id, p.step_id, p.param_name, p.site_name,
               p.sub_equip_id AS unit_id, p.value AS param_value
        FROM eda.array_pds_result_t p
        WHERE p.glass_start_time >= :start_time
          AND p.glass_start_time < :end_time
          AND p.glass_id LIKE :lot_prefix
          AND (""" + " OR ".join(pair_predicates) + ")")
    frames = []
    allowed = pd.MultiIndex.from_frame(monitoring[["step_id", "param_name"]])
    with db_manager.engine.connect().execution_options(postgresql_readonly=True) as connection:
        with connection.begin():
            connection.exec_driver_sql("SET LOCAL statement_timeout = '30s'")
            for day in pd.date_range(start, end, inclusive="left", freq="D"):
                day_end = min(day + pd.Timedelta(days=1), end)
                query_params = dict(params, start_time=day.to_pydatetime(), end_time=day_end.to_pydatetime())
                for chunk in pd.read_sql(sql, connection, params=query_params, chunksize=50000):
                    if chunk.empty:
                        continue
                    chunk = chunk.reindex(columns=RAW_MEASUREMENT_COLUMNS).copy()
                    chunk["start_time"] = pd.to_datetime(chunk["start_time"], errors="coerce")
                    chunk["param_value"] = pd.to_numeric(chunk["param_value"], errors="coerce")
                    pairs = pd.MultiIndex.from_frame(chunk[["step_id", "param_name"]])
                    valid = (chunk.sheet_id.astype("string").str.startswith(Z622_LOT, na=False)
                             & chunk.start_time.ge(day) & chunk.start_time.lt(day_end)
                             & pairs.isin(allowed))
                    frames.append(chunk.loc[valid])
    if _z622_source_signature() != signature:
        raise ValueError("Z622 monitoring configuration changed during source read")
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)
    result.attrs["z622_source_signature"] = signature
    return result


class _Z622MeasurementSnapshotRepository(InlineMeasurementSnapshotRepository):
    """Use the shared lifecycle, binding cached facts to their PDS/config source."""

    def _matches_source(self, path: Path) -> bool:
        metadata = read_metadata(path, self.SNAPSHOT_POLICY_VERSION, policy_file=True)
        return metadata is not None and metadata.get("z622_source_signature") == _z622_source_signature()

    def _read_snapshot(self, snapshot_path: Path) -> pd.DataFrame:
        if not self._matches_source(snapshot_path):
            raise ValueError("Z622 snapshot belongs to another source configuration")
        return super()._read_snapshot(snapshot_path)

    def _covers_start(self, snapshot_path: Path, required_start: object) -> bool:
        return self._matches_source(snapshot_path) and super()._covers_start(snapshot_path, required_start)

    def _write_snapshot(
        self, snapshot_path: Path, measurements: pd.DataFrame, end_timestamp: pd.Timestamp,
        *, window_start: object | None = None,
    ) -> None:
        signature = measurements.attrs.get("z622_source_signature")
        if not signature or signature != _z622_source_signature():
            raise ValueError("Z622 source configuration changed before snapshot publication")
        publish_snapshots(
            [(snapshot_path, measurements, self.SNAPSHOT_POLICY_VERSION, end_timestamp, True)],
            window_start=window_start, extra_metadata={"z622_source_signature": signature},
        )


def build_z622_measurement_repository(
    db_manager: DatabaseManager, snapshot_dir: Path | None = None,
) -> InlineMeasurementSnapshotRepository:
    """Compose the source exception without changing shared report calculations."""
    return _Z622MeasurementSnapshotRepository(
        snapshot_dir=snapshot_dir if snapshot_dir is not None else inline_measurement_directory(ConfigLoader.get_project_root() / "data"),
        db_manager=db_manager, measurement_loader=load_z622_measurements,
        measurement_corrector=apply_spc_value_corrections,
    )


class _Z622MetadataRepository:
    """Declare measured identities with unknown limits; never borrow specifications."""

    def get_parameter_catalog(self, prod_code: str) -> pd.DataFrame:
        if prod_code != Z622_PRODUCT:
            raise ValueError("PDS metadata is restricted to Z622")
        frame = _z622_monitoring()
        return pd.DataFrame({"ref_param_name": frame.param_name.str.upper(), "data_type": "SPC"}).drop_duplicates()

    def get_parameter_specs(self, prod_code: str) -> pd.DataFrame:
        if prod_code != Z622_PRODUCT:
            raise ValueError("PDS metadata is restricted to Z622")
        frame = _z622_monitoring()[["step_id", "param_name"]].copy()
        frame["prod_code"] = Z622_PRODUCT
        for column in ("usl", "lsl", "ucl", "lcl", "target"):
            frame[column] = float("nan")
        frame["param_type"] = "SPC"
        frame["main_step_id"] = frame.step_id
        frame["main_eqp_type"] = "EQP"
        return frame.reindex(columns=SPEC_COLUMNS)


def build_z622_metadata_repository(db_manager: DatabaseManager | None = None) -> _Z622MetadataRepository:
    return _Z622MetadataRepository()


def spc_input_signature(project_root: Path, prod_code: str) -> str:
    """Detect updated product snapshots before cached Sheet/capability computations."""
    path = inline_measurement_directory(project_root / "data") / (
        f"inline_measurements_{snapshot_component(prod_code)}.parquet"
    )
    parts = [
        InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION,
        f"excluded-step={EXCLUDED_MEASUREMENT_STEP_ID}",
    ]
    if prod_code == Z622_PRODUCT:
        parts.append(_z622_source_signature())
    for source in [path, path.with_suffix(".policy")]:
        try:
            stat = source.stat()
            parts.append(f"{source.resolve()}:{stat.st_mtime_ns}:{stat.st_size}")
        except FileNotFoundError:
            parts.append(f"{source.resolve()}:missing")
    return "|".join(parts)


class SpcRepository:
    supports_shared_history_persistence = True

    """Expose the SPC data contract while delegating preparation to measurement."""

    def __init__(self, preparation: MeasurementPreparationPort) -> None:
        if preparation is None:
            raise ValueError("SPC repository requires a measurement preparation port")
        self._preparation = preparation

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        """Return the SPC projection prepared by the shared measurement pipeline."""
        return self._preparation.get_prepared_measurements(config, force_refresh)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """Return the product spec limits prepared by the shared measurement pipeline."""
        return self._preparation.get_spec_limits(prod_code)
