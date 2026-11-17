"""Atomic read-only metadata captures for offline SPC capability denominators."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

from src.inline_domain.infrastructure.shared.measurement_metadata_loader import CATALOG_COLUMNS, SPEC_COLUMNS
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_paths import snapshot_component, snapshot_directory

RELATIONS = {
    ("eda", "imp_spc_tzbjx"): {"parmtername", "data_type", "productspecname", "update_time"},
    ("mdw", "dwr_mes_productspec"): {"productspecname", "productcode", "createtime", "checkouttime", "interface_time"},
    ("mdw", "dwd_imp_dv_param_spec"): {*SPEC_COLUMNS, "interface_time"},
}
CATALOG_SQL = """
SELECT DISTINCT parameter.parmtername AS ref_param_name, parameter.data_type,
       product.productcode AS prod_code
FROM eda.imp_spc_tzbjx AS parameter
JOIN mdw.dwr_mes_productspec AS product ON parameter.productspecname = product.productspecname
WHERE product.productcode = ANY(:products)
  AND (parameter.update_time < :as_of OR parameter.update_time IS NULL)
  AND (COALESCE(product.interface_time, product.checkouttime, product.createtime) < :as_of
       OR COALESCE(product.interface_time, product.checkouttime, product.createtime) IS NULL)
"""
SPECS_SQL = """
SELECT prod_code, step_id, param_name, param_type, usl, lsl, ucl, lcl, target,
       main_step_id, main_eqp_type
FROM mdw.dwd_imp_dv_param_spec
WHERE prod_code = ANY(:products)
  AND (interface_time < :as_of OR interface_time IS NULL)
"""


class CapabilityMetadataSnapshotError(RuntimeError):
    """Metadata is absent, invalid or unsafe to capture."""


def default_metadata_directory() -> Path:
    return snapshot_directory(ConfigLoader.get_project_root() / "data", "inline_domain", "spc") / "capability_metadata"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _guard_sources(connection) -> list[dict]:
    """Inspect schema and low-cost relation estimates before reading business rows."""
    schema = pd.read_sql(text("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE (table_schema = 'eda' AND table_name = 'imp_spc_tzbjx')
           OR (table_schema = 'mdw' AND table_name IN ('dwr_mes_productspec', 'dwd_imp_dv_param_spec'))
    """), connection)
    for (namespace, table), required in RELATIONS.items():
        columns = set(schema.loc[schema.table_schema.eq(namespace) & schema.table_name.eq(table), "column_name"])
        if not required.issubset(columns):
            raise CapabilityMetadataSnapshotError(f"Required metadata columns unavailable: {namespace}.{table}")
    sizes = pd.read_sql(text("""
        SELECT namespace.nspname AS table_schema, relation.relname AS table_name,
               relation.relkind, relation.reltuples AS estimated_rows,
               pg_total_relation_size(relation.oid) AS estimated_bytes
        FROM pg_class AS relation JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE (namespace.nspname = 'eda' AND relation.relname = 'imp_spc_tzbjx')
           OR (namespace.nspname = 'mdw' AND relation.relname IN ('dwr_mes_productspec', 'dwd_imp_dv_param_spec'))
    """), connection)
    if len(sizes) != len(RELATIONS):
        raise CapabilityMetadataSnapshotError("Metadata relation estimates unavailable")
    for row in sizes.itertuples(index=False):
        if row.relkind not in {"r", "p"}:
            raise CapabilityMetadataSnapshotError("Metadata views require an explicit underlying-source review")
        if row.estimated_rows < 0 or row.estimated_rows > 1_000_000 or row.estimated_bytes > 512 * 1024 * 1024:
            raise CapabilityMetadataSnapshotError("Metadata relation volume requires a bounded capture review")
    return sizes.to_dict("records")


def capture_capability_metadata(
    engine, products: Iterable[str], *, directory: Path | str | None = None, as_of: object | None = None,
) -> dict:
    """Read dimension rows as of a timestamp, then atomically publish native frames."""
    selected = list(dict.fromkeys(snapshot_component(product) for product in products))
    if not selected:
        raise CapabilityMetadataSnapshotError("No capability products selected")
    cutoff = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
    if pd.isna(cutoff):
        raise CapabilityMetadataSnapshotError("Invalid metadata capture timestamp")
    if cutoff.tzinfo is not None:
        cutoff = cutoff.tz_convert("Asia/Shanghai").tz_localize(None)
    parameters = {"products": selected, "as_of": cutoff.to_pydatetime()}
    with engine.connect().execution_options(postgresql_readonly=True) as connection:
        with connection.begin():
            connection.exec_driver_sql("SET LOCAL statement_timeout = '30s'")
            estimates = _guard_sources(connection)
            catalog = pd.read_sql(text(CATALOG_SQL), connection, params=parameters)
            specs = pd.read_sql(text(SPECS_SQL), connection, params=parameters)
    for frame in (catalog, specs):
        frame.columns = frame.columns.str.lower()
        if not frame.empty and not frame.prod_code.astype(str).isin(selected).all():
            raise CapabilityMetadataSnapshotError("Metadata query returned an unrequested product")
    root = Path(directory) if directory is not None else default_metadata_directory()
    generation = uuid4().hex
    target = root / generation
    target.mkdir(parents=True, exist_ok=False)
    provenance = {"version": 1, "generation": generation, "captured_at": datetime.now(timezone.utc).isoformat(),
                  "as_of": cutoff.isoformat(), "tables": estimates, "products": {},
                  "read_only": True, "temporal_policy": "Dimension rows known before capture; null timestamps retained"}
    for product in selected:
        cat = catalog.loc[catalog.prod_code.astype(str).eq(product), CATALOG_COLUMNS].copy()
        if product == "Z622":
            from src.inline_domain.infrastructure.spc.spc_repository import build_z622_metadata_repository

            cat = build_z622_metadata_repository().get_parameter_catalog(product)
        cat["ref_param_name"] = cat.ref_param_name.astype(str).str.strip().str.upper()
        spec = specs.loc[specs.prod_code.astype(str).eq(product), SPEC_COLUMNS].copy()
        for column in ("usl", "lsl", "ucl", "lcl", "target"):
            spec[column] = pd.to_numeric(spec[column], errors="coerce")
        main_steps = spec.main_step_id.astype("string").str.strip()
        spec["main_step_id"] = main_steps.mask(main_steps.eq("")).fillna(spec.step_id.astype(str))
        route_types = spec.main_eqp_type.astype("string").str.strip().str.upper()
        spec["main_eqp_type"] = route_types.where(route_types.isin({"EQP", "CHAMBER"}), "EQP")
        files = {}
        for kind, frame in (("catalog", cat), ("specs", spec)):
            path = target / f"{product}_{kind}.parquet"
            frame.to_parquet(path, index=False)
            files[kind] = {"filename": path.name, "rows": len(frame), "sha256": _sha(path)}
        provenance["products"][product] = files
    manifest = target / "manifest.json"
    manifest.write_text(json.dumps(provenance, ensure_ascii=False, indent=2), encoding="utf-8")
    pointer = root / f"current.{uuid4().hex}.tmp"
    pointer.write_text(json.dumps({"generation": generation, "manifest_sha256": _sha(manifest)}), encoding="utf-8")
    os.replace(pointer, root / "current.json")
    return provenance


class CapabilityMetadataSnapshot:
    """Validate a complete committed metadata generation before returning copies."""

    def __init__(self, directory: Path | str | None = None) -> None:
        self.directory = Path(directory) if directory is not None else default_metadata_directory()

    def _manifest(self) -> tuple[Path, dict]:
        try:
            pointer = json.loads((self.directory / "current.json").read_text(encoding="utf-8"))
            generation = snapshot_component(pointer["generation"])
            manifest = self.directory / generation / "manifest.json"
            if _sha(manifest) != pointer["manifest_sha256"]:
                raise ValueError("Generation identity mismatch")
            data = json.loads(manifest.read_text(encoding="utf-8"))
            if data.get("version") != 1 or data.get("generation") != generation:
                raise ValueError("Unsupported generation")
            return manifest.parent, data
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise CapabilityMetadataSnapshotError("Verified capability metadata snapshot unavailable") from exc

    def source_signature(self, products: Iterable[str] = ()) -> str:
        """Verify referenced files before a caller may reuse cached denominators.

        The pointer and manifest alone cannot establish current input health:
        metadata files may be deleted or changed after totals were cached.
        Their small committed byte payloads are therefore verified on every
        signature request, including calls that otherwise become cache hits.
        """
        root, manifest = self._manifest()
        try:
            selected = tuple(sorted({snapshot_component(product) for product in products}))
            selected = selected or tuple(sorted(manifest["products"]))
            if not selected:
                raise ValueError("Metadata generation contains no products")
            signatures = {product: self._verified_entries(root, manifest, product) for product in selected}
            return hashlib.sha256(json.dumps([root.name, signatures], sort_keys=True).encode()).hexdigest()
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise CapabilityMetadataSnapshotError("Verified capability metadata signature unavailable") from exc

    @staticmethod
    def _verified_entries(root: Path, manifest: dict, product: str) -> dict:
        records = manifest["products"][snapshot_component(product)]
        verified = {}
        for kind in ("catalog", "specs"):
            entry = records[kind]
            path = root / snapshot_component(entry["filename"])
            if _sha(path) != entry["sha256"]:
                raise ValueError("Metadata file identity mismatch")
            verified[kind] = entry
        return verified

    def read_product(self, product: str) -> dict[str, pd.DataFrame]:
        root, manifest = self._manifest()
        try:
            records = self._verified_entries(root, manifest, product)
            frames = {}
            for kind, columns in (("catalog", CATALOG_COLUMNS), ("specs", SPEC_COLUMNS)):
                entry = records[kind]
                path = root / snapshot_component(entry["filename"])
                frame = pd.read_parquet(path)
                if len(frame) != entry["rows"] or not set(columns).issubset(frame.columns):
                    raise ValueError("Metadata content contract mismatch")
                frames[kind] = frame[columns].copy()
            return frames
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise CapabilityMetadataSnapshotError(f"Verified capability metadata unavailable for {product}") from exc

    def __call__(self, product: str) -> dict[str, pd.DataFrame]:
        return self.read_product(product)
