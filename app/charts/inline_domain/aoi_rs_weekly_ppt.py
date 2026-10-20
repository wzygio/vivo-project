"""RS-specific labels on the shared Inline weekly presentation adapter."""
from app.charts.inline_domain.weekly_summary_ppt import build_weekly_summary_ppt
from src.inline_domain.application.shared.weekly_summary import WeeklySummaryTable


def build_weekly_rs_ppt(tables: list[WeeklySummaryTable], *, template_bytes: bytes) -> bytes:
    return build_weekly_summary_ppt(tables, template_bytes=template_bytes,
        category="RS Code", metric_label="平均每片 RS 个数", report_name="AOI_RS")
