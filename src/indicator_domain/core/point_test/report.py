"""Pure parsing of the maintained point-test workbook contract."""

from collections.abc import Mapping
import re

import numpy as np
import pandas as pd

STAGES = ("调整前", "调整后", "修正后")
METRICS = ("MAX", "MIN", "AVE", "STD", "Δ/（MAX-MIN)")
SUMMARY_COLUMNS = ("产品", "SHEET_ID", "膜位", *METRICS)
GROUP_STARTS = (0, 8, 16)
SHEET_PATTERN = re.compile(r"^(.+)-(detail|summary)$")
TIME_COLUMNS = ("SHEET_START_TIME", "SHEET_END_TIME", "RECIPE_MODIFY_TIME")


def _text(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def _detail(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    headers = [_text(value) for value in raw.iloc[0]]
    if not all(headers) or len(set(headers)) != len(headers):
        raise ValueError("Point-test detail contains blank or duplicate headers")
    required = {"SHEET_ID", "SITE_NAME", "PARAM_NAME", "PARAM_VALUE"}
    if not required <= set(headers):
        raise ValueError("Point-test detail is missing required columns")
    result = raw.iloc[1:].set_axis(headers, axis=1).dropna(how="all").reset_index(drop=True)
    for column in TIME_COLUMNS:
        if column not in result:
            continue
        # Values-only Excel normalization returns date serials; ordinary XLSX
        # can already contain datetime objects. Do not interpret serials as ns.
        values = result[column]
        numeric = values.map(lambda value: isinstance(value, (int, float)) and pd.notna(value))
        dates = pd.to_datetime(values.where(~numeric), errors="raise")
        dates.loc[numeric] = pd.to_datetime(
            pd.to_numeric(values.loc[numeric]), unit="D", origin="1899-12-30",
        ).dt.round("us")
        result[column] = dates
    return result


def _summary(raw: pd.DataFrame, product: str) -> pd.DataFrame:
    records = []
    stage = ""
    sheet_ids = [""] * len(GROUP_STARTS)
    if raw.empty:
        return pd.DataFrame(columns=list(SUMMARY_COLUMNS))
    if raw.shape[1] != 23:
        raise ValueError("Point-test summary must contain three seven-column groups")
    for row in raw.itertuples(index=False, name=None):
        first = _text(row[0])
        if first in STAGES:
            stage = first
            sheet_ids = [""] * len(GROUP_STARTS)
            for start in GROUP_STARTS:
                if tuple(_text(value) for value in row[start + 1:start + 7]) != ("膜位", *METRICS):
                    raise ValueError("Point-test summary has invalid metric headers")
            continue
        for group, start in enumerate(GROUP_STARTS):
            values = row[start:start + 7]
            if all(not _text(value) for value in values):
                sheet_ids[group] = ""
                continue
            sheet_ids[group] = _text(values[0]) or sheet_ids[group]
            position = _text(values[1])
            if not stage or not sheet_ids[group] or not position:
                raise ValueError("Point-test summary row has no stage, sheet ID or position")
            metrics = pd.to_numeric(pd.Series(values[2:]), errors="raise")
            if not np.isfinite(metrics).all():
                raise ValueError("Point-test summary metrics must be finite numbers")
            records.append([product, stage, sheet_ids[group], position, *metrics.tolist()])
    result = pd.DataFrame(records, columns=["产品", "阶段", "SHEET_ID", "膜位", *METRICS])
    if not result.empty and set(result["阶段"]) != set(STAGES):
        raise ValueError("Point-test summary is missing a business stage")
    if result.duplicated(["阶段", "SHEET_ID", "膜位"]).any():
        raise ValueError("Point-test summary contains duplicate identities")
    return result.loc[result["阶段"] == "修正后", list(SUMMARY_COLUMNS)].reset_index(drop=True)


def parse_report(sheets: Mapping[str, pd.DataFrame]) -> dict[str, dict[str, pd.DataFrame]]:
    """Keep complete product pairs and reject ambiguous/incomplete sources."""
    products: dict[str, dict[str, pd.DataFrame]] = {}
    for name, frame in sheets.items():
        match = SHEET_PATTERN.fullmatch(name)
        if match:
            product, kind = match.groups()
            products.setdefault(product, {})[kind] = frame
    if not products:
        raise ValueError("Point-test workbook has no product sheet pairs")
    report = {}
    for product, pair in products.items():
        if set(pair) != {"detail", "summary"}:
            raise ValueError(f"Point-test product {product!r} has an incomplete sheet pair")
        report[product] = {"detail": _detail(pair["detail"]),
                           "summary": _summary(pair["summary"], product)}
    return report
