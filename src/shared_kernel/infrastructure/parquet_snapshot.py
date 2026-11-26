"""Atomic publication for single-file Parquet snapshots."""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd


def publish_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, suffix=".parquet.tmp", delete=False) as handle:
        staged = Path(handle.name)
    try:
        frame.to_parquet(staged, index=False)
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)
