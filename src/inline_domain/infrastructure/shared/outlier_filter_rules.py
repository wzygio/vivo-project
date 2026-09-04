"""Load and apply physical outlier-filter rules for Inline measurements."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from fr_file_decryption import decrypt_file, is_encrypted_file

logger = logging.getLogger(__name__)

REQUIRED_RULE_COLUMNS = frozenset({"step_col", "param_col"})


class OutlierFilterConfigurationError(RuntimeError):
    """Raised when mandatory outlier-filter rules cannot be loaded safely."""


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
    """Apply parameter bounds or unconditional step-level exclusions."""
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

    for _, rule in rules.fillna("").iterrows():
        rule_prod = _rule_text(rule, "prod_col", default="ALL").upper()
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

    dropped_count = int(outlier_mask.sum())
    logger.info("Inline outlier filters removed %s measurement rows", dropped_count)
    return measurements.loc[~outlier_mask].copy()


def _rule_text(rule: pd.Series, column: str, default: str = "") -> str:
    value = rule[column] if column in rule.index else default
    return str(value).strip()


def _step_variants(step: str) -> tuple[str, ...]:
    normalized = step.rstrip("0").rstrip(".") if "." in step else step
    return (step,) if normalized == step else (step, normalized)
