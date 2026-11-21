"""Load and apply workbook exclusions and offsets for Inline measurements."""

from __future__ import annotations

import logging
from math import isfinite
from pathlib import Path

import pandas as pd
from fr_file_decryption import decrypt_file, is_encrypted_file

from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)

REQUIRED_RULE_COLUMNS = frozenset({"step_col", "param_col"})
OUTLIER_FILTER_POLICY = "inline-outlier-filter-v2-offset"


class OutlierFilterConfigurationError(RuntimeError):
    """Raised when mandatory outlier-filter rules cannot be loaded safely."""


def outlier_filter_workbook_path() -> Path:
    return ConfigLoader.get_domain_resource_path(
        "inline_domain", "spc_outlier_filters", "spc_outlier_filters.xlsx",
    )


def outlier_filter_input_signature() -> str:
    """Track saved workbook edits without relying on the protected file's byte view."""
    path = outlier_filter_workbook_path().resolve()
    try:
        generation = path.stat().st_mtime_ns
    except FileNotFoundError:
        generation = "missing"
    return f"{OUTLIER_FILTER_POLICY}:{path}:{generation}"


def load_outlier_filter_rules(rule_file: Path, decrypted_dir: Path) -> pd.DataFrame:
    """Read rules, decrypting encrypted Excel input before parsing it."""
    source = rule_file.resolve()
    if not source.exists():
        raise OutlierFilterConfigurationError(f"异常值过滤配置不存在: {source}")

    decrypted_path: Path | None = None
    try:
        read_path = source
        if is_encrypted_file(source):
            decryption = decrypt_file(source, output_dir=decrypted_dir)
            decrypted_path = Path(decryption.output_path)
            read_path = decrypted_path
        rules = pd.read_excel(read_path, dtype=str).fillna("")
    except Exception as exc:
        raise OutlierFilterConfigurationError(
            f"无法加载异常值过滤配置: {source}"
        ) from exc
    finally:
        if decrypted_path is not None:
            try:
                decrypted_path.unlink(missing_ok=True)
            except OSError:
                logger.warning(
                    "Failed to remove temporary decrypted outlier rules: %s",
                    decrypted_path,
                    exc_info=True,
                )

    rules = rules.rename(columns=lambda column: str(column).strip())
    missing_columns = REQUIRED_RULE_COLUMNS.difference(rules.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise OutlierFilterConfigurationError(
            f"异常值过滤配置缺少必需列: {missing}"
        )
    return rules


def apply_outlier_filter_rules(
    measurements: pd.DataFrame,
    prod_code: str,
    rules: pd.DataFrame,
) -> pd.DataFrame:
    """Filter points by their input values, then offset the retained points."""
    if measurements.empty or rules.empty:
        return measurements.copy()

    missing_columns = REQUIRED_RULE_COLUMNS.difference(rules.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise OutlierFilterConfigurationError(
            f"异常值过滤配置缺少必需列: {missing}"
        )

    values = pd.to_numeric(measurements["param_value"], errors="coerce")
    step_ids = measurements["step_id"].astype(str).str.strip()
    param_names = measurements["param_name"].astype(str).str.upper()
    outlier_mask = pd.Series(False, index=measurements.index)
    offsets: pd.Series | None = None

    for _, rule in rules.fillna("").iterrows():
        rule_prod = (_rule_text(rule, "prod_code") or _rule_text(rule, "prod_col")).upper()
        rule_step = _rule_text(rule, "step_col")
        rule_param = _rule_text(rule, "param_col")
        if not rule_step:
            continue
        if rule_prod not in {"", "ALL", prod_code.upper()}:
            continue

        lower = pd.to_numeric(_rule_text(rule, "lower_col"), errors="coerce")
        upper = pd.to_numeric(_rule_text(rule, "upper_col"), errors="coerce")
        if rule_param and pd.isna(lower) and pd.isna(upper):
            raise OutlierFilterConfigurationError(
                "参数级异常值过滤规则必须至少配置一个有效数值边界: "
                f"prod={rule_prod or 'ALL'}, step={rule_step}, param={rule_param}"
            )
        offset = _rule_offset(rule, rule_prod, rule_step, rule_param) if rule_param else 0.0

        step_mask = step_ids.isin(_step_variants(rule_step))
        if not step_mask.any():
            continue

        if not rule_param:
            outlier_mask |= step_mask
            continue

        target_mask = step_mask & param_names.eq(rule_param.upper())
        if not target_mask.any():
            continue

        if not pd.isna(lower):
            outlier_mask |= target_mask & values.le(lower)
        if not pd.isna(upper):
            outlier_mask |= target_mask & values.ge(upper)
        if offset:
            if offsets is None:
                offsets = pd.Series(0.0, index=measurements.index)
            offsets.loc[target_mask] += offset

    dropped_count = int(outlier_mask.sum())
    logger.info("Inline outlier filters removed %s measurement rows", dropped_count)
    retained = ~outlier_mask
    result = measurements.loc[retained].copy()
    if offsets is not None:
        retained_offsets = offsets.loc[retained]
        shifted = retained_offsets.ne(0)
        if shifted.any():
            original = result["param_value"]
            if pd.api.types.is_extension_array_dtype(original.dtype):
                dtype = float if pd.api.types.is_numeric_dtype(original.dtype) else object
                original = original.astype(dtype)
            result["param_value"] = original.where(~shifted, values.loc[retained] + retained_offsets)
    return result


def _rule_offset(rule: pd.Series, product: str, step: str, param: str) -> float:
    offset = pd.to_numeric(_rule_text(rule, "offset") or "0", errors="coerce")
    if not isfinite(offset):
        raise OutlierFilterConfigurationError(
            f"异常值过滤规则的 offset 必须为有限数值: prod={product or 'ALL'}, step={step}, param={param}"
        )
    return float(offset)


def _rule_text(rule: pd.Series, column: str) -> str:
    return str(rule.get(column, "")).strip()


def _step_variants(step: str) -> tuple[str, ...]:
    normalized = step.rstrip("0").rstrip(".") if "." in step else step
    return (step,) if normalized == step else (step, normalized)
