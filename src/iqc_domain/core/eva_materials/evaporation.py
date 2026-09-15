"""Evaporation report projection; measurement decisions remain source facts."""

from collections.abc import Mapping, Sequence

import pandas as pd


REPORT_FIELDS = {
    "prod_code": "项目名", "materialtype": "量产/非量产",
    "requestdate": "报检日期", "checkeddate": "检验时间",
    "mitemname": "物料号", "mitemdesc": "物料描述",
    "check_user": "检验人员", "mater_type": "物料分类", "factory": "工厂",
    "cha_item": "特性项目", "spec_req2": "规格样式1", "upp_spec": "规格上限",
    "spec_req1": "规格样式2", "low_spec": "规格下限",
    **{f"iqc_{i}": f"IQC-{i}" for i in range(1, 6)},
    **{f"coa_{i}": f"COA-{i}" for i in range(1, 6)},
    "iqc_result": "IQC结果", "coa_result": "COA结果",
}
REPORT_COLUMNS = ("序号", *REPORT_FIELDS.values())
FILTER_COLUMNS = ("项目名", "工厂", "物料分类", "特性项目", "IQC结果", "COA结果")


def project_report(source: pd.DataFrame) -> pd.DataFrame:
    """Reject malformed payloads, keep NULLs, and remove non-public fields."""
    missing = set(REPORT_FIELDS) - set(source.columns)
    if missing:
        raise ValueError("IQC_REPORT_SCHEMA_INVALID")
    frame = source.loc[:, list(REPORT_FIELDS)].rename(columns=REPORT_FIELDS).copy()
    for column in ("报检日期", "检验时间"):
        frame[column] = pd.to_datetime(frame[column], errors="raise")
    frame = frame.sort_values(
        ["物料描述", "检验时间", "物料号", "特性项目"],
        ascending=[True, False, True, True], kind="stable", na_position="last",
    ).reset_index(drop=True)
    frame.insert(0, "序号", range(1, len(frame) + 1))
    return frame


def filter_report(
    frame: pd.DataFrame, selections: Mapping[str, Sequence[str]],
) -> pd.DataFrame:
    mask = pd.Series(True, index=frame.index)
    for column, values in selections.items():
        if column not in FILTER_COLUMNS:
            raise ValueError("IQC_FILTER_INVALID")
        if values:
            mask &= frame[column].isin(values)
    return frame.loc[mask, list(REPORT_COLUMNS)].copy()
