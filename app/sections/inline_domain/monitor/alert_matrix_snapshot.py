"""JSON status snapshots shared by scheduled jobs and Streamlit processes."""
from __future__ import annotations

from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)
SNAPSHOT_VERSION = 1


class DailyMatrixSnapshot:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path if path is not None else (
            ConfigLoader.get_project_root() / "output/cache/alert_matrix/daily.json"
        )

    def read(self, day: date, ttl_seconds: int, *, now: datetime | None = None) -> dict | None:
        try:
            content = json.loads(self.path.read_text(encoding="utf-8"))
            if content["version"] != SNAPSHOT_VERSION or content["reference_date"] != day.isoformat():
                return None
            if content.get("report_cutoff") != ConfigLoader.get_report_cutoff_policy().signature:
                return None
            generated = datetime.fromisoformat(content["saved_at"])
            age = ((now or datetime.now()) - generated).total_seconds()
            if age < 0 or age > ttl_seconds:
                return None
            cells = {}
            for item in content["cells"]:
                key = (item["row_key"], item["product"])
                cell = item["cell"]
                if (not all(isinstance(part, str) and part for part in key)
                        or key in cells or not isinstance(cell, dict)
                        or cell.get("state") not in {"ok", "alert", "error", "no_data"}
                        or not isinstance(cell.get("cache_signature"), str)
                        or cell.get("detail_key") != "|".join(key)):
                    raise ValueError("Invalid matrix snapshot cell")
                cells[key] = cell
            if not cells:
                return None
            return {"saved_at": content["saved_at"], "cells": cells}
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError, KeyError):
            logger.warning("MATRIX_DAILY_SNAPSHOT_UNREADABLE")
            return None

    def write(self, payload: dict, *, now: datetime | None = None) -> None:
        cells = []
        for (row, product), value in payload["cells"].items():
            cell = dict(value)
            if cell.get("state") == "error":
                cell["message"] = "定时计算未成功，请在看板中定向刷新后检查。"
                cell.setdefault("cache_signature", "")
            cells.append({"row_key": row, "product": product, "cell": cell})
        content = {
            "version": SNAPSHOT_VERSION,
            "report_cutoff": ConfigLoader.get_report_cutoff_policy().signature,
            "reference_date": payload["reference_date"],
            "saved_at": (now or datetime.now()).isoformat(timespec="seconds"),
            "cells": cells,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f"{self.path.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_text(json.dumps(content, ensure_ascii=False, allow_nan=False), encoding="utf-8")
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


def has_daily_matrix_snapshot() -> bool:
    return DailyMatrixSnapshot().read(date.today(), ConfigLoader.get_cache_ttl_seconds()) is not None
