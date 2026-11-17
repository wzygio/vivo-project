"""Explicit read-only recovery of authorized complete weeks outside raw retention.

The capture is an isolated replay input. Existing measurement snapshots and
maintained capability/decision workbooks are never rewritten by this module.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

from src.inline_domain.infrastructure.monitor.capability_snapshot_source import (
    SnapshotCapabilityInputError, SnapshotCapabilitySource, TOTAL_COLUMNS, _week_start,
)
from src.inline_domain.infrastructure.shared.measurement_data_loader import FACTORY_TABLES, RAW_MEASUREMENT_COLUMNS
from src.inline_domain.infrastructure.shared.measurement_preprocessor import EXCLUDED_MEASUREMENT_STEP_ID
from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
from src.inline_domain.infrastructure.shared.rolling_snapshot import publish_snapshots, read_metadata, snapshot_process_lock
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_paths import snapshot_component

POLICY = InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION
RELATIONS = {
    ("eda", "spc_tzbjx_array"): {"sheet_id", "sheet_start_time", "product_spec", "step_id", "lot_id", "param_name", "site_name", "unit_id", "param_value"},
    ("eda", "spc_tzbjx_oled"): {"glass_id", "glass_start_time", "product_spec", "step_id", "lot_id", "param_name", "site_name", "unit_id", "param_value", "ppid"},
    ("eda", "spc_tzbjx_tsp"): {"glass_id", "glass_start_time", "product_spec", "step_id", "lot_id", "param_name", "site_name", "unit_id", "param_value"},
    ("mdw", "dwr_mes_productspec"): {"productspecname", "productcode", "interface_time", "checkouttime", "createtime"},
    ("eda", "imp_spc_tzbjx_mqc"): {"ppid", "prodcode", "update_time"},
}


def _scope(authorized_weeks: Mapping[str, Iterable[str]], as_of: object) -> dict[str, list[str]]:
    monday = pd.Timestamp(as_of).normalize()
    monday -= pd.Timedelta(days=monday.weekday())
    result = {}
    for product, labels in authorized_weeks.items():
        product = snapshot_component(product)
        selected = sorted(set(labels))
        if not selected or any(_week_start(label) >= monday for label in selected):
            raise SnapshotCapabilityInputError("Historical capture requires explicit completed weeks")
        result[product] = selected
    if not result:
        raise SnapshotCapabilityInputError("Historical capture scope is empty")
    return result


def _coverage(snapshot_dir: Path, product: str) -> dict:
    path = snapshot_dir / f"inline_measurements_{snapshot_component(product)}.parquet"
    metadata = read_metadata(path, POLICY, policy_file=True)
    if metadata is None:
        raise SnapshotCapabilityInputError(f"Verified current snapshot unavailable for {product}")
    return metadata


def _source_window(label: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    first = _week_start(label)
    return ConfigLoader.get_data_forward_policy().to_source_window(first, first + pd.Timedelta(days=6))


def _guard_sources(connection) -> list[dict]:
    """Inspect every base source before any bounded business read."""
    estimates = []
    for (namespace, name), required in RELATIONS.items():
        columns = connection.execute(text("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema=:schema AND table_name=:table
        """), {"schema": namespace, "table": name}).mappings().all()
        if not required.issubset({row["column_name"] for row in columns}):
            raise SnapshotCapabilityInputError(f"Historical source schema unavailable: {namespace}.{name}")
        relation = connection.execute(text("""
            SELECT relation.relkind, relation.reltuples AS estimated_rows,
                   pg_total_relation_size(relation.oid) AS estimated_bytes
            FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE namespace.nspname=:schema AND relation.relname=:table
        """), {"schema": namespace, "table": name}).mappings().one_or_none()
        if relation is None or relation["relkind"] not in {"r", "p"} or relation["estimated_rows"] < 0:
            raise SnapshotCapabilityInputError("Historical views or unknown source volume require explicit review")
        if relation["estimated_rows"] > 100_000_000 or relation["estimated_bytes"] > 50 * 1024**3:
            raise SnapshotCapabilityInputError("Historical source exceeds reviewed bounded capture volume")
        estimates.append({"table": f"{namespace}.{name}", **dict(relation)})
    return estimates


def _measurement_sql() -> str:
    selects = []
    for factory, (table, identifier, timestamp) in FACTORY_TABLES.items():
        normal = "AND (fact.step_id <> :mqc_step OR fact.step_id IS NULL)" if factory == "OLED" else ""
        selects.append(f"""
            SELECT '{factory}' AS factory, product.productcode AS prod_code,
                   fact.{timestamp} AS start_time, fact.{identifier} AS sheet_id,
                   fact.lot_id, fact.step_id, fact.param_name, fact.site_name, fact.unit_id, fact.param_value
            FROM eda.{table} fact
            JOIN mdw.dwr_mes_productspec product ON fact.product_spec=product.productspecname
            WHERE fact.{timestamp} >= :start_time AND fact.{timestamp} < :end_time
              AND product.productcode=:product
              AND (COALESCE(product.interface_time,product.checkouttime,product.createtime) < :as_of
                   OR COALESCE(product.interface_time,product.checkouttime,product.createtime) IS NULL)
              AND (fact.step_id <> :excluded_step OR fact.step_id IS NULL) {normal}
        """)
    selects.append("""
        SELECT 'OLED' AS factory, mapping.prodcode AS prod_code,
               fact.glass_start_time AS start_time, fact.glass_id AS sheet_id,
               fact.lot_id, fact.step_id, fact.param_name, fact.site_name, fact.unit_id, fact.param_value
        FROM eda.spc_tzbjx_oled fact
        JOIN (SELECT DISTINCT ppid, prodcode FROM eda.imp_spc_tzbjx_mqc
              WHERE prodcode=:product AND (update_time < :as_of OR update_time IS NULL)) mapping
          ON fact.ppid=mapping.ppid
        WHERE fact.glass_start_time >= :start_time AND fact.glass_start_time < :end_time
          AND fact.step_id=:mqc_step AND (fact.step_id <> :excluded_step OR fact.step_id IS NULL)
    """)
    return " UNION ALL ".join(selects)


def _read_missing(connection, product: str, first: pd.Timestamp, last: pd.Timestamp,
                  as_of: pd.Timestamp) -> pd.DataFrame:
    if last < first or (last - first).days > 31:
        raise SnapshotCapabilityInputError("Historical capture requires a reviewed bounded missing window")
    frames = []
    for day in pd.date_range(first, last, freq="D"):
        params = {"start_time": day.to_pydatetime(), "end_time": (day + pd.Timedelta(days=1)).to_pydatetime(),
                  "as_of": as_of.to_pydatetime(), "product": product, "mqc_step": "2120A",
                  "excluded_step": EXCLUDED_MEASUREMENT_STEP_ID}
        statement = text(_measurement_sql()).execution_options(stream_results=True, max_row_buffer=50_000)
        for chunk in pd.read_sql(statement, connection, params=params, chunksize=50_000):
            chunk = chunk.reindex(columns=RAW_MEASUREMENT_COLUMNS).copy()
            chunk["start_time"] = pd.to_datetime(chunk.start_time, errors="raise")
            chunk["param_value"] = pd.to_numeric(chunk.param_value, errors="coerce")
            if (not chunk.prod_code.eq(product).all()
                    or not (chunk.start_time.ge(day) & chunk.start_time.lt(day + pd.Timedelta(days=1))).all()):
                raise SnapshotCapabilityInputError("Historical query returned facts outside its explicit window")
            frames.append(chunk)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)


def capture_historical_capability_inputs(engine, *, snapshot_dir: Path | str, output_dir: Path | str,
                                         authorized_weeks: Mapping[str, Iterable[str]], as_of: object) -> dict:
    """Capture only missing prefixes, reusing retained raw facts for the rest."""
    current, output = Path(snapshot_dir), Path(output_dir)
    authorized = _scope(authorized_weeks, as_of)
    output.mkdir(parents=True, exist_ok=True)
    captured = {}
    cutoff = pd.Timestamp.now()
    with engine.connect().execution_options(postgresql_readonly=True, isolation_level="REPEATABLE READ") as connection:
        with connection.begin():
            connection.exec_driver_sql("SET LOCAL statement_timeout = '30s'")
            estimates = _guard_sources(connection)
            for product, labels in authorized.items():
                metadata = _coverage(current, product)
                retained_first, retained_last = pd.Timestamp(metadata["covered_from"]), pd.Timestamp(metadata["covered_through"])
                older = [label for label in labels if _source_window(label)[0] < retained_first]
                if not older:
                    continue
                windows = sorted(_source_window(label) for label in older)
                first, last = windows[0][0], max(window[1] for window in windows)
                if last > retained_last:
                    raise SnapshotCapabilityInputError("Requested historical week extends beyond verified current coverage")
                prefix_last = min(last, retained_first - pd.Timedelta(days=1))
                continuous_end = min(windows[0][1], prefix_last)
                for start, end in windows[1:]:
                    if start > continuous_end + pd.Timedelta(days=1) and start <= prefix_last:
                        raise SnapshotCapabilityInputError("Disjoint missing windows must be captured separately")
                    continuous_end = max(continuous_end, min(end, prefix_last))
                missing = _read_missing(connection, product, first, prefix_last, cutoff)
                base = current / f"inline_measurements_{product}.parquet"
                with snapshot_process_lock(base):
                    if _coverage(current, product) != metadata:
                        raise SnapshotCapabilityInputError("Current raw snapshot changed during historical capture")
                    tail = (pd.read_parquet(base, filters=[("start_time", ">=", retained_first),
                                                          ("start_time", "<", last + pd.Timedelta(days=1))])
                            if last >= retained_first else pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS))
                    if _coverage(current, product) != metadata:
                        raise SnapshotCapabilityInputError("Current raw snapshot changed during historical read")
                merged = pd.concat([missing, tail], ignore_index=True).reindex(columns=RAW_MEASUREMENT_COLUMNS)
                merged["start_time"] = pd.to_datetime(merged.start_time, errors="raise")
                merged = merged.loc[merged.start_time.ge(first) & merged.start_time.lt(last + pd.Timedelta(days=1))]
                target = output / f"inline_measurements_{product}.parquet"
                publish_snapshots([(target, merged, POLICY, last, True)], window_start=first,
                                  extra_metadata={"historical_read_only_capture": True})
                captured[product] = {"weeks": older, "source_from": str(first.date()), "source_through": str(last.date()),
                                     "queried_from": str(first.date()), "queried_through": str(prefix_last.date()),
                                     "queried_rows": len(missing), "retained_rows": len(tail), "rows": len(merged),
                                     "base_snapshot": metadata, "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}
    manifest = {"version": 1, "read_only": True, "as_of": str(pd.Timestamp(as_of).date()),
                "captured_at": datetime.now(timezone.utc).isoformat(), "authorized_weeks": authorized,
                "relations": estimates, "products": captured}
    temporary = output / f".capture-{uuid4().hex}.json"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, output / "capture.json")
    return manifest


class HistoricalCapabilitySource:
    """Replay approved older weeks locally; normal weeks keep their existing source."""
    def __init__(self, base_source: SnapshotCapabilitySource, historical_dir: Path | str,
                 authorized_weeks: Mapping[str, Iterable[str]]) -> None:
        self.base = base_source
        self.directory = Path(historical_dir)
        self.authorized = {product: set(weeks) for product, weeks in authorized_weeks.items()}
        self.historical = SnapshotCapabilitySource(self.directory, base_source.metadata_provider,
                                                   decoration_port=base_source.decoration_port,
                                                   outlier_rules=base_source.outlier_rules)

    def _manifest(self) -> dict:
        try:
            manifest = json.loads((self.directory / "capture.json").read_text(encoding="utf-8"))
            if manifest.get("version") != 1 or manifest.get("read_only") is not True:
                raise ValueError("Invalid historical capture")
            for product, entry in manifest["products"].items():
                path = self.directory / f"inline_measurements_{snapshot_component(product)}.parquet"
                if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
                    raise ValueError("Historical capture identity mismatch")
                if _coverage(self.base.snapshot_dir, product) != entry["base_snapshot"]:
                    raise ValueError("Retained source generation changed after historical capture")
            return manifest
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise SnapshotCapabilityInputError("Verified historical capability capture unavailable") from exc

    def source_signature(self, products: Iterable[str]) -> str:
        selected = tuple(products)
        manifest = self._manifest()
        recovered = sorted(set(selected).intersection(manifest["products"]))
        historical = self.historical.source_signature(recovered) if recovered else "no-historical-products"
        signature = [self.base.source_signature(selected), historical, manifest,
                     {product: sorted(weeks) for product, weeks in sorted(self.authorized.items())}]
        return hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()

    def read_weekly_totals(self, product: str, weeks: Iterable[str], *, metric: str, as_of: object) -> pd.DataFrame:
        labels = list(dict.fromkeys(weeks))
        metadata = _coverage(self.base.snapshot_dir, product)
        normal, older = [], []
        for label in labels:
            first, last = _source_window(label)
            target = normal if first >= pd.Timestamp(metadata["covered_from"]) and last <= pd.Timestamp(metadata["covered_through"]) else older
            target.append(label)
        if set(older) - self.authorized.get(product, set()):
            raise SnapshotCapabilityInputError("Unapproved historical week requested")
        frames = []
        if normal:
            frames.append(self.base.read_weekly_totals(product, normal, metric=metric, as_of=as_of))
        if older:
            manifest = self._manifest()
            if not set(older).issubset(set(manifest["products"].get(product, {}).get("weeks", []))):
                raise SnapshotCapabilityInputError("Requested historical week has not been captured")
            frames.append(self.historical.read_weekly_totals(product, older, metric=metric, as_of=as_of))
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=TOTAL_COLUMNS)
        return combined.set_index("period_label").reindex(labels).reset_index()[TOTAL_COLUMNS]
