"""Read extracted workbook values without requiring Excel at runtime."""

import json
from pathlib import Path

import pandas as pd

from src.shared_kernel.config import ConfigLoader

DEMO_DIR = Path(__file__).resolve().parents[3] / "resources/iqc_domain/demo"


def read_demo_report(report: str) -> pd.DataFrame:
    if report not in {"inspection", "lifetime"}:
        raise ValueError(f"Unknown IQC report: {report}")
    payload = json.loads((DEMO_DIR / f"{report}.json").read_text(encoding="utf-8"))
    frame = pd.DataFrame(payload["rows"], columns=payload["columns"])
    date_columns = ("报检日期", "检验时间") if report == "inspection" else ("批次",)
    frame = frame.assign(**{column: pd.to_datetime(frame[column]) for column in date_columns})
    return ConfigLoader.get_report_cutoff_policy().filter_frame(
        frame, "检验时间" if report == "inspection" else "批次",
    )
