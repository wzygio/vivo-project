"""Read extracted workbook values without requiring Excel at runtime."""

import json

import pandas as pd

from src.shared_kernel.config import ConfigLoader

def read_demo_report(report: str) -> pd.DataFrame:
    if report not in {"inspection", "lifetime"}:
        raise ValueError(f"Unknown IQC report: {report}")
    source_path = ConfigLoader.get_domain_resource_path("iqc_domain", f"demo_{report}")
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    frame = pd.DataFrame(payload["rows"], columns=payload["columns"])
    date_columns = ("报检日期", "检验时间") if report == "inspection" else ("批次",)
    frame = frame.assign(**{column: pd.to_datetime(frame[column]) for column in date_columns})
    return ConfigLoader.get_report_cutoff_policy().filter_frame(
        frame, "检验时间" if report == "inspection" else "批次",
    )
