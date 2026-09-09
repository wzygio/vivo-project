"""Shared manual cache invalidation, scoped to one indicator and one product."""

import hashlib
from collections.abc import Sequence
from pathlib import Path
from uuid import uuid4

from src.shared_kernel.config import ConfigLoader

INDICATOR_CACHE_REVISION_DIR = Path("output/tmp/indicator_product_cache_revisions")


def _revision_path(indicator_key: str, product_code: str, revision_dir: Path) -> Path:
    indicator = str(indicator_key).strip()
    product = str(product_code).strip().upper()
    if not indicator or not product:
        raise ValueError("indicator_key and product_code must be nonempty")
    digest = hashlib.sha256(f"{indicator}\0{product}".encode()).hexdigest()
    return revision_dir / f"{digest}.revision"


def get_indicator_product_revision(
    indicator_key: str, product_code: str, *,
    revision_dir: Path = INDICATOR_CACHE_REVISION_DIR,
) -> str:
    try:
        return _revision_path(indicator_key, product_code, revision_dir).read_text(encoding="utf-8").strip() or "0"
    except FileNotFoundError:
        return "0"


def bump_indicator_product_revision(
    indicator_key: str, product_code: str, *,
    revision_dir: Path = INDICATOR_CACHE_REVISION_DIR,
) -> str:
    path = _revision_path(indicator_key, product_code, revision_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    revision = uuid4().hex
    temporary = path.with_suffix(f".{revision}.tmp")
    temporary.write_text(revision, encoding="utf-8")
    temporary.replace(path)
    return revision


def build_indicator_product_cache_signature(
    base_signature: str, product_code: str, indicator_keys: Sequence[str], *,
    revision_dir: Path = INDICATOR_CACHE_REVISION_DIR,
) -> str:
    if not indicator_keys:
        raise ValueError("At least one indicator is required")
    revisions = tuple(
        (key, get_indicator_product_revision(key, product_code, revision_dir=revision_dir))
        for key in sorted(set(indicator_keys))
    )
    return (
        f"{base_signature}|product={str(product_code).strip().upper()}"
        f"|indicators={revisions!r}"
        f"|data_forward={ConfigLoader.get_data_forward_policy().signature}"
    )
