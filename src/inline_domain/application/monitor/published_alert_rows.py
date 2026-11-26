"""Select request dates and current release decisions over inspection-selected rows."""
from datetime import date, timedelta

import pandas as pd

from src.inline_domain.core.monitor.cpk_latest import capability_period_records


def read_published_rows(rows, keys, controls, *, scope, product, event, reference: date,
                        start_date=None, end_date=None):
    if not rows.empty and not rows["prod_code"].astype(str).eq(product).all():
        raise ValueError("Invalid published alert product")
    if event in ("CPK", "CPM"):
        rows = capability_period_records(rows, reference, start_date=start_date, end_date=end_date)
    elif not rows.empty:
        monday = reference - timedelta(days=reference.weekday())
        start = pd.Timestamp(start_date or monday - timedelta(days=7))
        end = pd.Timestamp(end_date or monday - timedelta(days=1)) + pd.Timedelta(days=1)
        times = pd.to_datetime(rows["start_time" if scope == "aoi_tt" else "sheet_start_time"])
        rows = rows.loc[times.ge(start) & times.lt(end)]
    if rows.empty or controls.empty:
        return rows.iloc[:0]
    controls = controls[[*keys, "flag"]].copy()
    for key in keys:
        controls[key] = controls[key].astype("string").str.strip()
    released = {"false", "0", "0.0"}
    if event not in ("CPK", "CPM"):
        released.update({"no", "n", "否", "不修饰", "不截断"})
    controls["released"] = controls["flag"].astype(str).str.strip().str.lower().isin(released)
    allowed = controls.groupby(keys, dropna=False)["released"].all()
    identities = pd.MultiIndex.from_frame(rows[keys].astype("string").apply(lambda col: col.str.strip()))
    return rows.loc[identities.isin(allowed.index[allowed])]
