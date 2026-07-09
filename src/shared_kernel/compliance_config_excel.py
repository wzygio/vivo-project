import logging
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Union

import pandas as pd


ExcelSource = Union[Path, BinaryIO, BytesIO]


def load_compliance_config_from_xlsx(excel_path: ExcelSource) -> Dict[str, Any]:
    """Load SPC compliance rules from an xlsx workbook."""
    try:
        xls = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        logging.warning(f"[ComplianceConfig] openpyxl 读取 xlsx 配置失败，尝试 COM 兜底: {e}")
        try:
            xls = _read_compliance_sheets_via_com(excel_path)
        except Exception as com_error:
            logging.error(f"[ComplianceConfig] 读取 xlsx 配置失败: {com_error}", exc_info=True)
            return {"default": False, "rules": {}}

    default_value = _parse_default_sheet(xls.get("默认配置"))

    rules: Dict[str, bool] = {}
    for rules_df in _iter_rule_sheets(xls):
        for _, row in rules_df.dropna(how="all").iterrows():
            rule_key = _build_rule_key(row)
            if not rule_key:
                continue
            is_enabled = _parse_bool(_get_first_value(row, ["启用", "enabled", "enable"]), default=False)
            rules[rule_key] = is_enabled

    return {"default": default_value, "rules": rules}


def _read_compliance_sheets_via_com(excel_path: ExcelSource) -> Dict[str, pd.DataFrame]:
    """Read encrypted enterprise Excel config files through the shared COM helper."""
    if not isinstance(excel_path, (str, Path)):
        raise TypeError("COM fallback only supports filesystem paths")

    from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

    path = Path(excel_path)
    sheets: Dict[str, pd.DataFrame] = {}
    for sheet_name in ("规则配置", "默认配置"):
        try:
            sheets[sheet_name] = _read_encrypted_xlsx_via_com(path, sheet_name=sheet_name)
        except Exception as e:
            logging.warning(f"[ComplianceConfig] COM 读取 sheet={sheet_name} 失败: {e}")

    if sheets:
        return sheets

    first_sheet = _read_encrypted_xlsx_via_com(path)
    return {"规则配置": first_sheet}


def _iter_rule_sheets(xls: Dict[str, pd.DataFrame]) -> List[pd.DataFrame]:
    """Yield configured rule sheets, including legacy rule-like default sheets."""
    if "规则配置" in xls:
        yield xls["规则配置"]

    yielded_rule_sheet = "规则配置" in xls
    for sheet_name, df in xls.items():
        if sheet_name == "规则配置":
            continue
        if _looks_like_rule_sheet(df):
            yield df
            yielded_rule_sheet = True

    if not yielded_rule_sheet:
        first_sheet = next(iter(xls.values()), pd.DataFrame())
        if _looks_like_rule_sheet(first_sheet):
            yield first_sheet


def _looks_like_rule_sheet(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    rule_columns = {"规则键", "rule_key", "监控类型", "monitor_type", "data_type"}
    return any(col in df.columns for col in rule_columns)


def write_compliance_config_to_xlsx(config: Dict[str, Any], excel_path: Path) -> None:
    """Persist a compliance config dict to an xlsx workbook."""
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    default_df, rules_df = build_compliance_config_dataframes(config)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")
        default_df.to_excel(writer, index=False, sheet_name="默认配置")


def compliance_config_to_xlsx_bytes(config: Dict[str, Any]) -> bytes:
    """Serialize a compliance config dict to xlsx bytes for downloads."""
    output = BytesIO()
    default_df, rules_df = build_compliance_config_dataframes(config)
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        rules_df.to_excel(writer, index=False, sheet_name="规则配置")
        default_df.to_excel(writer, index=False, sheet_name="默认配置")
    return output.getvalue()


def build_compliance_config_dataframes(config: Dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    default_df = pd.DataFrame([{"默认启用": bool(config.get("default", False))}])
    rows: List[Dict[str, Any]] = []
    for rule_key, is_enabled in (config.get("rules") or {}).items():
        parts = [part.strip() for part in str(rule_key).split("-") if part.strip()]
        rows.append(
            {
                "规则键": rule_key,
                "监控类型": parts[0] if len(parts) >= 1 else "",
                "产品型号": parts[1] if len(parts) >= 2 else "",
                "厂别": parts[2] if len(parts) >= 3 else "",
                "月份": parts[3] if len(parts) >= 4 else "",
                "周别": parts[4] if len(parts) >= 5 else "",
                "启用": bool(is_enabled),
                "备注": "",
            }
        )
    rules_df = pd.DataFrame(
        rows,
        columns=["规则键", "监控类型", "产品型号", "厂别", "月份", "周别", "启用", "备注"],
    )
    return default_df, rules_df


def _parse_default_sheet(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    first_row = df.iloc[0]
    return _parse_bool(_get_first_value(first_row, ["默认启用", "default", "默认"]), default=False)


def _build_rule_key(row: pd.Series) -> str:
    explicit_key = _normalize_text(_get_first_value(row, ["规则键", "rule_key"]))
    if explicit_key:
        return explicit_key

    parts = [
        _normalize_text(_get_first_value(row, ["监控类型", "monitor_type", "data_type"]), default="ALL"),
        _normalize_text(_get_first_value(row, ["产品型号", "产品", "product"]), default="ALL"),
        _normalize_text(_get_first_value(row, ["厂别", "factory"]), default="ALL"),
        _normalize_period(_get_first_value(row, ["月份", "month"]), prefix="M"),
        _normalize_period(_get_first_value(row, ["周别", "week"]), prefix="W"),
    ]

    last_meaningful_index = 0
    for index, part in enumerate(parts):
        if part != "ALL":
            last_meaningful_index = index

    return "-".join(parts[: last_meaningful_index + 1])


def _normalize_period(value: Any, prefix: str) -> str:
    text = _normalize_text(value, default="ALL").upper()
    if text == "ALL":
        return text
    if text.startswith(prefix):
        return text
    try:
        return f"{prefix}{int(float(text)):02d}"
    except (TypeError, ValueError):
        return text


def _get_first_value(row: pd.Series, names: List[str]) -> Any:
    for name in names:
        if name in row.index:
            return row.get(name)
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    return str(value).strip() == ""


def _normalize_text(value: Any, default: str = "") -> str:
    if _is_blank(value):
        return default
    text = str(value).strip()
    if text.lower() == "nan":
        return default
    return text.upper()


def _parse_bool(value: Any, default: bool = False) -> bool:
    if _is_blank(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "启用", "是"}:
        return True
    if text in {"false", "0", "no", "n", "禁用", "否"}:
        return False
    return default
