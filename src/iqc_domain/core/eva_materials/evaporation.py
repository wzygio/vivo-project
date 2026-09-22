"""Evaporation projection and supplied SQL CASE measurement decisions."""

import pandas as pd


REPORT_FIELDS = {
    "prod_code": "产品型号", "materialtype": "量产/非量产",
    "requestdate": "报检日期", "checkeddate": "检验时间",
    "mitemname": "物料号", "mitemdesc": "物料描述",
    "check_user": "检验人员", "mater_type": "物料分类", "factory": "工厂",
    "cha_item": "特性项目", "spec_req2": "规格样式1", "upp_spec": "规格上限",
    "spec_req1": "规格样式2", "low_spec": "规格下限",
    "iqc_result": "IQC结果", "coa_result": "COA结果",
    **{f"iqc_{i}": f"IQC-{i}" for i in range(1, 6)},
    **{f"coa_{i}": f"COA-{i}" for i in range(1, 6)},
}
REPORT_COLUMNS = ("序号", *REPORT_FIELDS.values())


RESULT_RULE_VERSION = "measurement-case-v1"


def measurement_results(source: pd.DataFrame) -> pd.DataFrame:
    """Mirror the supplied SQL CASE, including ELSE OK for missing measurements."""
    results = pd.DataFrame(index=source.index)
    violations = {">=": "lt", "<=": "gt", ">": "le", "<": "ge", "=": "ne"}
    bounds = [
        (source["spec_req1"], pd.to_numeric(source["low_spec"], errors="raise")),
        (source["spec_req2"], pd.to_numeric(source["upp_spec"], errors="raise")),
    ]
    for prefix in ("iqc", "coa"):
        for index in range(1, 6):
            value = pd.to_numeric(source[f"{prefix}_{index}"], errors="raise")
            failed = pd.Series(False, index=source.index)
            for requirement, bound in bounds:
                present = value.notna() & bound.notna()
                for operator, comparison in violations.items():
                    failed |= present & requirement.eq(operator) & getattr(value, comparison)(bound)
            results[f"{prefix}_{index}_result"] = failed.map({True: "NG", False: "OK"})
    return results


def project_report(source: pd.DataFrame) -> pd.DataFrame:
    """Reject malformed payloads, keep NULLs, and remove non-public fields."""
    missing = set(REPORT_FIELDS) - set(source.columns)
    if missing:
        raise ValueError("IQC_REPORT_SCHEMA_INVALID")
    frame = source.loc[:, list(REPORT_FIELDS)].rename(columns=REPORT_FIELDS).copy()
    decisions = measurement_results(source)
    for prefix, label in (("iqc", "IQC结果"), ("coa", "COA结果")):
        failed = decisions[[f"{prefix}_{i}_result" for i in range(1, 6)]].eq("NG").any(axis=1)
        frame[label] = failed.map({True: "NG", False: "OK"})
    for column in ("报检日期", "检验时间"):
        frame[column] = pd.to_datetime(frame[column], errors="raise")
    frame = frame.sort_values(
        ["物料描述", "检验时间", "物料号", "特性项目"],
        ascending=[True, False, True, True], kind="stable", na_position="last",
    ).reset_index(drop=True)
    frame.insert(0, "序号", range(1, len(frame) + 1))
    return frame
