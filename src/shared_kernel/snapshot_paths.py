"""Snapshot layout: data/domain/module, with product identity in filenames."""

from pathlib import Path
import re


def snapshot_component(value: str) -> str:
    """Reject path fragments, including Windows reserved filename characters."""
    component = str(value).strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", component):
        raise ValueError(f"Invalid snapshot path component: {value!r}")
    if component.endswith("."):
        raise ValueError(f"Invalid snapshot path component: {value!r}")
    return component


def snapshot_directory(data_root: Path | str, domain: str, module: str) -> Path:
    return Path(data_root) / snapshot_component(domain) / snapshot_component(module)


def inline_measurement_directory(data_root: Path | str) -> Path:
    return snapshot_directory(data_root, "inline_domain", "shared")


def aoi_rs_snapshot_directory(data_root: Path | str) -> Path:
    return snapshot_directory(data_root, "inline_domain", "aoi_rs")
