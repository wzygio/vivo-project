"""Explicit historical capability repair, separate from report publication."""
from __future__ import annotations

from collections.abc import Iterable
import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS
from src.inline_domain.core.monitor.cpk_latest import released_capability_mask


def closed_week_labels(as_of: object, *, count: int | None = None,
                       from_month: object | None = None) -> list[str]:
    """Select complete weeks; a month window includes its first cross-month week."""
    today = pd.Timestamp(as_of).normalize()
    monday = today - pd.Timedelta(days=today.weekday())
    if count is not None:
        first = monday - pd.Timedelta(weeks=count)
    elif from_month is not None:
        first = pd.Timestamp(from_month).normalize()
        first -= pd.Timedelta(days=first.weekday())
    else:
        first = monday - pd.Timedelta(weeks=1)
    return [f"{day.isocalendar().year}-W{day.isocalendar().week:02d}"
            for day in pd.date_range(first, monday, freq="7D", inclusive="left")]


def compute_weekly_records(source, latest_reader, *, products: Iterable[str],
                           weeks: Iterable[str], metric: str, as_of: object) -> pd.DataFrame:
    """Reuse report denominators and count exactly released saved alert projects."""
    columns = capability_summary_columns(metric)
    labels = list(dict.fromkeys(weeks))
    rows = []
    for product in products:
        totals = source.read_weekly_totals(product, labels, metric=metric, as_of=as_of)
        if (set(totals.period_label) != set(labels) or totals.period_label.duplicated().any()
                or not totals.prod_code.eq(product).all()):
            raise ValueError(f"Incomplete weekly project totals for {product}")
        detail = latest_reader.read_product(product)
        for item in totals.itertuples(index=False):
            total = int(item.total_projects)
            if total < 0 or total != item.total_projects:
                raise ValueError("Weekly capability totals must be nonnegative integers")
            warning = 0
            if detail is not None and not detail.empty:
                selected = detail.loc[detail.prod_code.eq(product) & detail.period_type.eq("week")
                                      & detail.period_label.eq(item.period_label)].drop_duplicates(CPK_KEY_COLUMNS)
                warning = int(released_capability_mask(selected, metric).sum())
            if warning > total:
                raise ValueError(f"Saved warning projects exceed computed total for {product}/{item.period_label}/{metric}")
            passed = total - warning
            rows.append({"产品": product, "监控类型": "SPC", "厂别": "ALL", "周期类型": "周度",
                         "时间标签": item.period_label, "显示标签": item.period_label.split("-")[-1],
                         columns[6]: total, columns[7]: passed / total if total else 1.0,
                         "达标项目数": passed, "预警项目数": warning})
    return pd.DataFrame(rows, columns=columns)
