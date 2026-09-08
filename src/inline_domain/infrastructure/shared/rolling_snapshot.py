"""Coverage-driven incremental raw snapshots and recoverable file publication."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
from uuid import uuid4

import pandas as pd

from .snapshot_window import inline_snapshot_window_start

OVERLAP_DAYS = 2
logger = logging.getLogger(__name__)


def read_metadata(path: Path, policy: str, *, policy_file: bool = False) -> dict | None:
    """Reject incomplete generations, unknown policies and damaged metadata."""
    try:
        metadata = json.loads(path.with_suffix(".snapshot.json").read_text(encoding="utf-8"))
        if not isinstance(metadata, dict) or metadata.get("policy_version") != policy:
            return None
        if policy_file and path.with_suffix(".policy").read_text(encoding="utf-8").strip() != policy:
            return None
        stat = path.stat()
        if (metadata.get("snapshot_size_bytes"), metadata.get("snapshot_mtime_ns")) != (stat.st_size, stat.st_mtime_ns):
            return None
        start, end = pd.Timestamp(metadata["covered_from"]), pd.Timestamp(metadata["covered_through"])
        refreshed = pd.Timestamp(metadata["refreshed_at"])
        if (pd.isna(start) or pd.isna(end) or start.tzinfo is not None
                or end.tzinfo is not None or start > end
                or pd.isna(refreshed) or refreshed.tzinfo is None):
            return None
        return metadata
    except (OSError, KeyError, TypeError, ValueError, OverflowError):
        return None


def is_fresh(metadata: dict | None, end: object, ttl_hours: float) -> bool:
    if metadata is None:
        return False
    start = inline_snapshot_window_start(end)
    age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(metadata["refreshed_at"])).total_seconds() / 3600
    return (pd.Timestamp(metadata["covered_from"]) == start
            and pd.Timestamp(metadata["covered_through"]) >= pd.Timestamp(end)
            and 0 <= age < ttl_hours)


def incremental_start(metadata: dict | None, end: object) -> pd.Timestamp:
    start = inline_snapshot_window_start(end)
    if metadata is None:
        return start
    covered_from = pd.Timestamp(metadata["covered_from"])
    covered_through = pd.Timestamp(metadata["covered_through"])
    if covered_from > start or covered_through < start or covered_through > pd.Timestamp(end):
        return start
    return max(start, covered_through - pd.Timedelta(days=OVERLAP_DAYS))


def replace_tail(old: pd.DataFrame | None, new: pd.DataFrame, start: object, end: object) -> pd.DataFrame:
    """Replace the entire queried interval, so removed/corrected records disappear.

    Deduplicate only identical complete facts: different RS quantities are not
    aggregated or conflated using an underspecified business key.
    """
    new = new.copy()
    if "start_time" not in new:
        if not new.empty:
            raise ValueError("Raw snapshot input is missing start_time")
        new["start_time"] = pd.Series(dtype="datetime64[ns]")
    new["start_time"] = pd.to_datetime(new["start_time"], errors="raise")
    new = new.loc[new.start_time.ge(pd.Timestamp(start)) & new.start_time.lt(pd.Timestamp(end) + pd.Timedelta(days=1))]
    if old is not None and not old.empty:
        old = old.loc[pd.to_datetime(old.start_time).lt(pd.Timestamp(start))]
        new = pd.concat([old, new], ignore_index=True)
    return new.loc[new.start_time.ge(inline_snapshot_window_start(end))].drop_duplicates().reset_index(drop=True)


def publish_snapshots(items: list[tuple[Path, pd.DataFrame, str, object, bool]]) -> None:
    """Stage every file first, roll back all replacements on publication failure.

    Metadata is the commit marker and binds to the exact parquet generation.
    A process interruption between renames is detected on the next read.
    """
    staged: dict[Path, Path] = {}
    backups: dict[Path, Path | None] = {}
    replaced: list[Path] = []
    retained_backups: set[Path] = set()
    try:
        for path, frame, policy, end, policy_file in items:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
            staged[path] = temp
            frame.to_parquet(temp, index=False)
            stat = temp.stat()
            metadata = {"policy_version": policy, "covered_from": inline_snapshot_window_start(end).date().isoformat(),
                        "covered_through": pd.Timestamp(end).date().isoformat(), "refreshed_at": datetime.now(timezone.utc).isoformat(),
                        "snapshot_size_bytes": stat.st_size, "snapshot_mtime_ns": stat.st_mtime_ns}
            if policy_file:
                target = path.with_suffix(".policy")
                staged[target] = target.with_name(f"{target.name}.{uuid4().hex}.tmp")
                staged[target].write_text(policy, encoding="utf-8")
            target = path.with_suffix(".snapshot.json")
            staged[target] = target.with_name(f"{target.name}.{uuid4().hex}.tmp")
            staged[target].write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
        for target in staged:
            backup = target.with_name(f"{target.name}.{uuid4().hex}.tmp") if target.exists() else None
            backups[target] = backup
            if backup is not None:
                shutil.copy2(target, backup)
        for target, temp in staged.items():
            os.replace(temp, target)
            replaced.append(target)
    except Exception:
        for target in reversed(replaced):
            backup = backups[target]
            try:
                if backup is None:
                    target.unlink(missing_ok=True)
                else:
                    # copy2 restores bytes and mtime even if replace is failing.
                    shutil.copy2(backup, target)
            except OSError:
                if backup is not None:
                    retained_backups.add(backup)
                logger.exception("Snapshot rollback failed for %s; recovery copy: %s", target, backup)
        raise
    finally:
        for temp in [*staged.values(), *backups.values()]:
            if temp is not None and temp not in retained_backups:
                temp.unlink(missing_ok=True)
