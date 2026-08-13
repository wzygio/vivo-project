import logging
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, TypedDict, Union

import pandas as pd


ExcelSource = Union[Path, BinaryIO, BytesIO]
COMPLIANCE_EXCEL_COLUMNS = ("厂别", "产品型号", "监控类型", "月份")


class ComplianceRule(TypedDict):
    factory: str
    prod_code: str
    data_type: str
    month: str


def load_compliance_config_from_xlsx(excel_path: ExcelSource) -> dict[str, Any]:
    """Load enabled four-dimension compliance scopes from an xlsx workbook."""
    try:
        sheets = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")
    except Exception as error:
        logging.warning(
            "[ComplianceConfig] openpyxl 读取 xlsx 配置失败，尝试 COM 兜底: %s",
            error,
        )
        try:
            sheets = _read_compliance_sheets_via_com(excel_path)
        except Exception as com_error:
            logging.error(
                "[ComplianceConfig] 读取 xlsx 配置失败: %s",
                com_error,
                exc_info=True,
            )
            raise ValueError("无法读取修饰配置工作簿") from com_error

    rules_df = _select_rule_sheet(sheets)
    if rules_df.empty:
        return {"rules": []}

    missing_columns = [
        column for column in COMPLIANCE_EXCEL_COLUMNS if column not in rules_df.columns
    ]
    if missing_columns:
        raise ValueError(f"修饰配置缺少必要列: {missing_columns}")

    rules: list[ComplianceRule] = []
    seen_rules: set[tuple[str, str, str, str]] = set()
    for row_index, row in rules_df.dropna(how="all").iterrows():
        rule = _parse_rule_row(row, row_index + 2)
        identity = tuple(rule.values())
        if identity in seen_rules:
            continue
        seen_rules.add(identity)
        rules.append(rule)

    return {"rules": rules}


def _read_compliance_sheets_via_com(excel_path: ExcelSource) -> dict[str, pd.DataFrame]:
    """Read an enterprise-encrypted workbook through the shared Excel COM helper."""
    if not isinstance(excel_path, (str, Path)):
        raise TypeError("COM fallback only supports filesystem paths")

    from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

    path = Path(excel_path)
    try:
        rules_df = _read_encrypted_xlsx_via_com(path, sheet_name="规则配置")
    except Exception as named_sheet_error:
        logging.warning(
            "[ComplianceConfig] COM 按名称读取规则配置失败，回退到第一个 Sheet: %s",
            named_sheet_error,
        )
        rules_df = _read_encrypted_xlsx_via_com(path)
    return {"规则配置": rules_df}


def _select_rule_sheet(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if "规则配置" in sheets:
        return sheets["规则配置"]
    return next(iter(sheets.values()), pd.DataFrame())


def _parse_rule_row(row: pd.Series, excel_row_number: int) -> ComplianceRule:
    factory = _normalize_required_dimension(row.get("厂别"), "厂别", excel_row_number)
    prod_code = _normalize_required_dimension(row.get("产品型号"), "产品型号", excel_row_number)
    data_type = _normalize_required_dimension(row.get("监控类型"), "监控类型", excel_row_number)
    month = normalize_compliance_month(row.get("月份"), excel_row_number=excel_row_number)
    return {
        "factory": factory,
        "prod_code": prod_code,
        "data_type": data_type,
        "month": month,
    }


def _normalize_required_dimension(value: Any, column: str, excel_row_number: int) -> str:
    if _is_blank(value):
        raise ValueError(f"修饰配置第 {excel_row_number} 行的“{column}”不能为空，请显式填写 ALL")
    return str(value).strip().upper()


def normalize_compliance_month(
    value: Any,
    *,
    excel_row_number: int | None = None,
) -> str:
    """Normalize a month value to ALL or M01-M12."""
    if _is_blank(value):
        location = f"第 {excel_row_number} 行" if excel_row_number is not None else ""
        raise ValueError(f"修饰配置{location}的“月份”不能为空，请显式填写 ALL")

    text = str(value).strip().upper()
    if text == "ALL":
        return text
    if text.startswith("M"):
        text = text[1:]
    try:
        month = int(float(text))
    except (TypeError, ValueError) as error:
        raise ValueError(f"无效月份: {value!r}，应为 ALL、1-12 或 M01-M12") from error
    if not 1 <= month <= 12:
        raise ValueError(f"无效月份: {value!r}，应位于 1-12")
    return f"M{month:02d}"


def compliance_rule_matches(
    rule: ComplianceRule,
    *,
    factory: str,
    prod_code: str,
    data_type: str,
    month: int | str | None,
) -> bool:
    """Return whether one enabled rule matches a concrete four-dimension context."""
    context_month = None if month is None else normalize_compliance_month(month)
    context = {
        "factory": str(factory).strip().upper(),
        "prod_code": str(prod_code).strip().upper(),
        "data_type": str(data_type).strip().upper(),
        "month": context_month,
    }
    for field, rule_value in rule.items():
        if rule_value == "ALL":
            continue
        if context[field] is None or rule_value != context[field]:
            return False
    return True


def write_compliance_config_to_xlsx(config: dict[str, Any], excel_path: Path) -> None:
    """Persist only the supported four rule columns."""
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    rules_df = build_compliance_config_dataframe(config)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")


def compliance_config_to_xlsx_bytes(config: dict[str, Any]) -> bytes:
    """Serialize the supported four-column rule table for download."""
    output = BytesIO()
    rules_df = build_compliance_config_dataframe(config)
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")
    return output.getvalue()


def build_compliance_config_dataframe(config: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "厂别": rule["factory"],
            "产品型号": rule["prod_code"],
            "监控类型": rule["data_type"],
            "月份": rule["month"],
        }
        for rule in config.get("rules", [])
    ]
    return pd.DataFrame(rows, columns=COMPLIANCE_EXCEL_COLUMNS)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip() == ""
