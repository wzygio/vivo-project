"""Bind reusable weekly rebuild results to their scope and source generation.

Callers capture the source/ledger signatures before computing and confirm them
again afterward. This module records that identity only after all native output
files exist. Reuse validates the same identity and exact output bytes before the
caller reads or publishes the prepared results. It never reads source facts or
accesses a database.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from numbers import Integral, Real
import os
from pathlib import Path
import re
from uuid import uuid4

import numpy as np
import pandas as pd


MANIFEST_FILENAME = "rebuild-inputs.json"
MANIFEST_VERSION = 1
MAX_MANIFEST_BYTES = 1024 * 1024
METRICS = {"cpk", "cpm"}


class RebuildProvenanceError(ValueError):
    """Prepared weekly results cannot be proven to match the requested inputs."""


def _tokens(values: Iterable[str], label: str) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise RebuildProvenanceError(f"{label} must be a collection of identities")
    try:
        tokens = list(values)
    except TypeError as exc:
        raise RebuildProvenanceError(f"{label} must be a collection of identities") from exc
    if (not tokens or any(not isinstance(token, str) or not token.strip() for token in tokens)
            or len(set(tokens)) != len(tokens)):
        raise RebuildProvenanceError(f"{label} must contain unique nonblank identities")
    if any(token != token.strip() for token in tokens):
        raise RebuildProvenanceError(f"{label} must not contain padded identities")
    return sorted(tokens)


def _scope(as_of: date | str, products: Iterable[str],
           weeks_by_metric: Mapping[str, Iterable[str]]) -> dict:
    try:
        if isinstance(as_of, datetime):
            day = as_of.date()
        elif isinstance(as_of, date):
            day = as_of
        else:
            day = date.fromisoformat(as_of)
        canonical_day = date.fromisoformat(day.isoformat()).isoformat()
    except (TypeError, ValueError) as exc:
        raise RebuildProvenanceError("Invalid rebuild reference date") from exc
    if (not isinstance(weeks_by_metric, Mapping) or not weeks_by_metric
            or not set(weeks_by_metric).issubset(METRICS)):
        raise RebuildProvenanceError("Rebuild scope requires CPK and/or CPM week lists")
    weeks = {}
    for metric in sorted(weeks_by_metric):
        labels = _tokens(weeks_by_metric[metric], f"{metric} weeks")
        for label in labels:
            if not re.fullmatch(r"\d{4}-W\d{2}", label):
                raise RebuildProvenanceError("Invalid canonical ISO week in rebuild scope")
            year, number = label.split("-W")
            try:
                date.fromisocalendar(int(year), int(number), 1)
            except ValueError as exc:
                raise RebuildProvenanceError("Invalid ISO week in rebuild scope") from exc
        weeks[metric] = labels
    return {"as_of": canonical_day, "products": _tokens(products, "Products"),
            "weeks_by_metric": weeks}


def _workbook_cell(value: object) -> object:
    """Canonicalize ordinary Excel cell values without rounding numeric facts."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    missing = pd.isna(value)
    if isinstance(missing, (bool, np.bool_)) and missing:
        return None
    if isinstance(value, (bool, np.bool_)):
        return ["boolean", bool(value)]
    if isinstance(value, str):
        return ["text", value]
    if isinstance(value, (Integral, Real, Decimal)):
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise RebuildProvenanceError("Workbook contains an invalid numeric cell") from exc
        if number.is_nan():
            return None
        if number.is_infinite():
            return ["number", "-Infinity" if number.is_signed() else "Infinity"]
        if number.is_zero():
            return ["number", "0"]
        parts = number.as_tuple()
        digits, exponent = list(parts.digits), parts.exponent
        while digits[-1] == 0:
            digits.pop()
            exponent += 1
        return ["number", parts.sign, "".join(str(digit) for digit in digits), exponent]
    if isinstance(value, (date, datetime, np.datetime64)):
        return ["datetime", pd.Timestamp(value).isoformat()]
    raise RebuildProvenanceError("Workbook contains an unsupported cell value type")


def workbook_signature(sheets: Mapping[str, pd.DataFrame]) -> str:
    """Hash logical worksheet values across ordinary and protected Excel reads.

    Sheet order and DataFrame indexes do not affect this identity. Header names
    remain exact; only trailing blank rows and blank-header/blank-data columns
    are removed. Native numeric representations and empty-cell representations
    compare equally. Formulas, styles and workbook objects require a separate
    preservation check because DataFrames contain their values only.
    """
    if not isinstance(sheets, Mapping) or any(not isinstance(name, str) for name in sheets):
        raise RebuildProvenanceError("Workbook identity requires named worksheet frames")
    values = []
    for name in sorted(sheets):
        frame = sheets[name]
        if not isinstance(frame, pd.DataFrame):
            raise RebuildProvenanceError("Workbook identity requires worksheet DataFrames")
        columns = [_workbook_cell(column) for column in frame.columns]
        rows = [[_workbook_cell(value) for value in row]
                for row in frame.itertuples(index=False, name=None)]
        width = len(columns)
        while width and columns[width - 1] is None and all(row[width - 1] is None for row in rows):
            width -= 1
        columns = columns[:width]
        rows = [row[:width] for row in rows]
        while rows and all(value is None for value in rows[-1]):
            rows.pop()
        values.append([name, columns, rows])
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _signature_digest(signature: str, label: str) -> str:
    if not isinstance(signature, str) or not signature.strip():
        raise RebuildProvenanceError(f"A nonblank {label} signature is required")
    # Persist only digests, so input paths or incidental signature details do not
    # become part of the reusable result's on-disk manifest.
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _stat_identity(stat) -> tuple:
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _fingerprint(directory: Path, metric: str) -> dict:
    filename = f"{metric}-weekly.parquet"
    path = directory / filename
    try:
        if path.resolve().parent != directory.resolve() or not path.is_file():
            raise RebuildProvenanceError("Prepared weekly file must remain inside its result directory")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            initial = os.fstat(handle.fileno())
            if initial.st_size <= 0:
                raise RebuildProvenanceError("Prepared weekly file is empty")
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
            final = os.fstat(handle.fileno())
        if _stat_identity(initial) != _stat_identity(final) or _stat_identity(final) != _stat_identity(path.stat()):
            raise RebuildProvenanceError("Prepared weekly file changed during provenance validation")
        return {"filename": filename, "size_bytes": final.st_size, "sha256": digest.hexdigest()}
    except OSError as exc:
        raise RebuildProvenanceError(f"Prepared {metric.upper()} weekly file is unavailable") from exc


def _identity(*, as_of: date | str, products: Iterable[str],
              weeks_by_metric: Mapping[str, Iterable[str]], source_signature: str,
              ledger_signature: str) -> dict:
    return {"scope": _scope(as_of, products, weeks_by_metric),
            "source_signature_sha256": _signature_digest(source_signature, "source"),
            "ledger_signature_sha256": _signature_digest(ledger_signature, "ledger")}


def write_rebuild_manifest(
    directory: Path | str, *, as_of: date | str, products: Iterable[str],
    weeks_by_metric: Mapping[str, Iterable[str]], source_signature: str, ledger_signature: str,
) -> dict:
    """Atomically attest successfully computed files; retain any old manifest on failure."""
    root = Path(directory)
    identity = _identity(as_of=as_of, products=products, weeks_by_metric=weeks_by_metric,
                         source_signature=source_signature, ledger_signature=ledger_signature)
    files = {metric: _fingerprint(root, metric) for metric in identity["scope"]["weeks_by_metric"]}
    manifest = {"version": MANIFEST_VERSION, **identity, "files": files,
                "created_at": datetime.now(timezone.utc).isoformat()}
    temporary = root / f".rebuild-inputs-{uuid4().hex}.tmp"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, root / MANIFEST_FILENAME)
    except OSError as exc:
        raise RebuildProvenanceError("Could not atomically publish rebuild provenance") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return manifest


def validate_rebuild_manifest(
    directory: Path | str, *, as_of: date | str, products: Iterable[str],
    weeks_by_metric: Mapping[str, Iterable[str]], source_signature: str, ledger_signature: str,
) -> dict:
    """Reject unproven, stale, corrupted or differently scoped prepared weekly files."""
    root = Path(directory)
    expected = _identity(as_of=as_of, products=products, weeks_by_metric=weeks_by_metric,
                         source_signature=source_signature, ledger_signature=ledger_signature)
    try:
        with (root / MANIFEST_FILENAME).open("rb") as handle:
            data = handle.read(MAX_MANIFEST_BYTES + 1)
        if len(data) > MAX_MANIFEST_BYTES:
            raise RebuildProvenanceError("Prepared weekly manifest exceeds its supported size")
        manifest = json.loads(data.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RebuildProvenanceError("Prepared weekly manifest is missing or malformed; recompute the results") from exc
    if (not isinstance(manifest, dict) or type(manifest.get("version")) is not int
            or manifest["version"] != MANIFEST_VERSION):
        raise RebuildProvenanceError("Unsupported prepared weekly manifest")
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise RebuildProvenanceError("Prepared weekly input signatures or calculation scope changed; recompute the results")
    files = manifest.get("files")
    metrics = expected["scope"]["weeks_by_metric"]
    if not isinstance(files, dict) or set(files) != set(metrics):
        raise RebuildProvenanceError("Prepared weekly manifest has a different output scope")
    for metric in metrics:
        if files[metric] != _fingerprint(root, metric):
            raise RebuildProvenanceError(f"Prepared {metric.upper()} weekly file differs from its verified result")
    return manifest
