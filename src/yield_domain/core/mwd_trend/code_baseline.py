import logging
from datetime import datetime as dt
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from src.shared_kernel.config_model import AppConfig


CODE_BASELINE_MAX_AGE_DAYS = 30
CODE_BASELINE_FALLBACK_MIN_TOTAL_PANELS = 1000
CODE_BASELINE_SHEET = "Sheet1"
CODE_BASELINE_METADATA_SHEET = "_metadata"
CODE_BASELINE_COLUMNS = [
    'baseline_month',
    'source_month',
    'defect_desc',
    'baseline_rate',
    'source_total_panels',
]


def code_baseline_path(prod_code: str) -> Path:
    return Path(f"resources/{prod_code}/{prod_code}_codebaseline.xlsx")


def defect_multipliers_signature(config: AppConfig | None) -> str:
    """Return a stable signature for defect_multipliers that affect Code baseline."""
    if config is None:
        return ""

    try:
        multipliers = config.processing.get('defect_multipliers', {}) or {}
    except Exception:
        return ""

    signature_parts = []
    for code, factor in sorted(multipliers.items(), key=lambda item: str(item[0])):
        code_text = str(code).strip()
        if not code_text:
            continue
        try:
            factor_text = f"{float(factor):.12g}"
        except (TypeError, ValueError):
            factor_text = str(factor).strip()
        if factor_text:
            signature_parts.append(f"{code_text}={factor_text}")

    return ";".join(signature_parts)


def build_code_baseline(
    df: pd.DataFrame,
    as_of: dt | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build next-month baseline rates from each Code-level source month."""
    columns = CODE_BASELINE_COLUMNS
    required_cols = {'warehousing_time', 'defect_desc', 'defect_panel_count', 'total_panels'}
    if df.empty or not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    df_calc = df.copy()
    df_calc['warehousing_time'] = pd.to_datetime(df_calc['warehousing_time'], errors='coerce')
    df_calc = df_calc[
        df_calc['warehousing_time'].notna()
        &
        df_calc['defect_desc'].notna()
        & (df_calc['defect_desc'].astype(str).str.strip() != "")
        & (df_calc['defect_desc'].astype(str) != "NoDefect")
    ].copy()
    if df_calc.empty:
        return pd.DataFrame(columns=columns)

    as_of_raw = pd.Timestamp(as_of) if as_of is not None else df_calc['warehousing_time'].max()
    if pd.isna(as_of_raw):
        return pd.DataFrame(columns=columns)
    current_month_period = pd.Period(as_of_raw, freq='M')

    df_calc['defect_panel_count'] = pd.to_numeric(
        df_calc['defect_panel_count'], errors='coerce'
    ).fillna(0)
    df_calc['total_panels'] = pd.to_numeric(
        df_calc['total_panels'], errors='coerce'
    ).fillna(0)
    df_calc['source_month_period'] = df_calc['warehousing_time'].dt.to_period('M')
    df_calc = df_calc[df_calc['source_month_period'] < current_month_period].copy()
    if df_calc.empty:
        return pd.DataFrame(columns=columns)

    grouped = df_calc.groupby(['source_month_period', 'defect_desc'], dropna=True).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    grouped['baseline_rate'] = np.where(
        grouped['total_panels'] > 0,
        np.round(grouped['defect_panel_count'] / grouped['total_panels'], 5),
        0.0
    )
    grouped['source_month'] = grouped['source_month_period'].astype(str)
    grouped['baseline_month'] = (grouped['source_month_period'] + 1).astype(str)
    grouped['source_total_panels'] = grouped['total_panels']
    return grouped[columns].sort_values(['baseline_month', 'defect_desc']).reset_index(drop=True)


def code_baseline_source_window(df: pd.DataFrame) -> Tuple[str, str]:
    if df.empty or 'warehousing_time' not in df.columns:
        return "", ""
    dates = pd.to_datetime(df['warehousing_time'], errors='coerce').dropna()
    if dates.empty:
        return "", ""
    return dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d')


def write_code_baseline_file(
    out_path: Path,
    baseline: pd.DataFrame,
    prod_code: str,
    generated_at: pd.Timestamp,
    refresh_reason: str,
    source_start: str,
    source_end: str,
    defect_multipliers_signature: str = "",
) -> None:
    metadata = pd.DataFrame(
        [
            {'key': 'product_code', 'value': prod_code},
            {'key': 'generated_at', 'value': generated_at.isoformat()},
            {'key': 'max_age_days', 'value': str(CODE_BASELINE_MAX_AGE_DAYS)},
            {'key': 'refresh_reason', 'value': refresh_reason},
            {'key': 'source_start', 'value': source_start},
            {'key': 'source_end', 'value': source_end},
            {'key': 'code_count', 'value': str(len(baseline))},
            {'key': 'defect_multipliers_signature', 'value': defect_multipliers_signature},
        ]
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        baseline.to_excel(writer, index=False, sheet_name=CODE_BASELINE_SHEET)
        metadata.to_excel(writer, index=False, sheet_name=CODE_BASELINE_METADATA_SHEET)


def read_code_baseline_metadata(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        metadata_df = pd.read_excel(path, sheet_name=CODE_BASELINE_METADATA_SHEET)
        if not {'key', 'value'}.issubset(metadata_df.columns):
            return {}
        return {
            str(row['key']): "" if pd.isna(row['value']) else str(row['value'])
            for _, row in metadata_df.dropna(subset=['key']).iterrows()
        }
    except Exception:
        return {}


def is_code_baseline_expired(
    path: Path,
    now: dt | pd.Timestamp | None = None,
    max_age_days: int = CODE_BASELINE_MAX_AGE_DAYS,
) -> bool:
    if not path.exists():
        return True

    metadata = read_code_baseline_metadata(path)
    generated_at_raw = metadata.get('generated_at')
    if not generated_at_raw:
        return True

    generated_at = pd.to_datetime(generated_at_raw, errors='coerce')
    if pd.isna(generated_at):
        return True

    now_ts = pd.Timestamp(now or dt.now())
    if getattr(generated_at, "tzinfo", None) is not None:
        generated_at = generated_at.tz_localize(None)
    if getattr(now_ts, "tzinfo", None) is not None:
        now_ts = now_ts.tz_localize(None)

    return (now_ts - generated_at).days >= max_age_days


def load_code_baseline_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=CODE_BASELINE_COLUMNS)
    try:
        df = pd.read_excel(path, sheet_name=CODE_BASELINE_SHEET)
        if 'defect_desc' not in df.columns or 'baseline_rate' not in df.columns:
            return pd.DataFrame(columns=CODE_BASELINE_COLUMNS)
        df = df.copy()
        if 'baseline_month' not in df.columns:
            df['baseline_month'] = ""
        if 'source_month' not in df.columns:
            df['source_month'] = ""
        if 'source_total_panels' not in df.columns:
            df['source_total_panels'] = np.nan
        return df[CODE_BASELINE_COLUMNS].copy()
    except Exception as e:
        logging.warning(f"[Baseline Loader] 加载失败: {e}")
        return pd.DataFrame(columns=CODE_BASELINE_COLUMNS)


def is_legacy_code_baseline_frame(df: pd.DataFrame) -> bool:
    if df.empty or 'baseline_month' not in df.columns:
        return False
    return not df['baseline_month'].astype(str).str.strip().astype(bool).any()


def code_baseline_keys(df: pd.DataFrame) -> set[tuple[str, str]]:
    if df.empty or not {'baseline_month', 'defect_desc'}.issubset(df.columns):
        return set()
    keyed = df.copy()
    keyed['baseline_month'] = keyed['baseline_month'].astype(str).str.strip()
    keyed['defect_desc'] = keyed['defect_desc'].astype(str)
    keyed = keyed[keyed['baseline_month'] != ""]
    return set(zip(keyed['baseline_month'], keyed['defect_desc']))


def sort_code_baseline(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CODE_BASELINE_COLUMNS)
    return df[CODE_BASELINE_COLUMNS].sort_values(
        ['baseline_month', 'defect_desc']
    ).reset_index(drop=True)


def generate_code_baseline(
    df: pd.DataFrame,
    prod_code: str,
    *,
    generated_at: dt | pd.Timestamp | None = None,
    refresh_reason: str = "manual_refresh",
    defect_multipliers_signature: str = "",
    baseline: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build or append period-scoped Code baselines without rewriting closed months.
    """
    out_path = code_baseline_path(prod_code)
    new_baseline = sort_code_baseline(
        baseline.copy() if baseline is not None else build_code_baseline(df, as_of=generated_at)
    )
    generated_at_ts = pd.Timestamp(generated_at or dt.now())
    source_start, source_end = code_baseline_source_window(df)

    try:
        write_code_baseline_file(
            out_path=out_path,
            baseline=new_baseline,
            prod_code=prod_code,
            generated_at=generated_at_ts,
            refresh_reason=refresh_reason,
            source_start=source_start,
            source_end=source_end,
            defect_multipliers_signature=defect_multipliers_signature,
        )
        logging.info(
            f"[Baseline Generator] Code 基准已重建: {out_path} "
            f"(reason={refresh_reason}, codes={len(new_baseline)}, window={source_start}->{source_end})"
        )
    except Exception as e:
        logging.warning(f"[Baseline Generator] 写入失败: {e}")

    return new_baseline


def ensure_code_baseline_current(
    df: pd.DataFrame,
    prod_code: str,
    *,
    now: dt | pd.Timestamp | None = None,
    max_age_days: int = CODE_BASELINE_MAX_AGE_DAYS,
    defect_multipliers_signature: str = "",
) -> pd.DataFrame:
    """
    Ensure period-scoped baselines exist without automatic rewrites of closed months.

    max_age_days is retained for caller compatibility. Age-based whole-file refresh is
    intentionally disabled because it makes historical months drift without user action.
    """
    path = code_baseline_path(prod_code)
    current_baseline = build_code_baseline(df, as_of=now)
    if current_baseline.empty:
        return load_code_baseline_frame(path)

    existing = load_code_baseline_frame(path)
    metadata = read_code_baseline_metadata(path)
    existing_scoped = existing[existing['baseline_month'].astype(str).str.strip() != ""].copy()
    existing_keys = code_baseline_keys(existing_scoped)
    current_keys = code_baseline_keys(current_baseline)
    missing_keys = current_keys - existing_keys
    existing_multiplier_signature = metadata.get('defect_multipliers_signature', "")

    if not path.exists():
        reason = "missing_file"
    elif (
        existing_multiplier_signature != defect_multipliers_signature
        and (existing_multiplier_signature or defect_multipliers_signature)
    ):
        reason = "multiplier_changed"
    elif is_legacy_code_baseline_frame(existing):
        reason = "legacy_schema"
    elif missing_keys:
        missing_df = current_baseline[
            current_baseline.apply(
                lambda row: (
                    str(row['baseline_month']).strip(),
                    str(row['defect_desc']),
                )
                in missing_keys,
                axis=1,
            )
        ]
        merged = pd.concat([existing_scoped, missing_df], ignore_index=True)
        merged = merged.drop_duplicates(
            subset=['baseline_month', 'defect_desc'],
            keep='first',
        )
        return generate_code_baseline(
            df,
            prod_code,
            generated_at=now,
            refresh_reason="missing_period_rows",
            defect_multipliers_signature=defect_multipliers_signature,
            baseline=merged,
        )
    else:
        logging.info(f"[Baseline] Code 月度基准已覆盖当前窗口，沿用: {path}")
        return existing_scoped

    return generate_code_baseline(
        df,
        prod_code,
        generated_at=now,
        refresh_reason=reason,
        defect_multipliers_signature=defect_multipliers_signature,
    )


def load_code_baseline(prod_code: str) -> dict:
    """
    Read Code baseline mapping from resources/<prod_code>/<prod_code>_codebaseline.xlsx.
    Returns the latest-month {defect_desc: baseline_rate} mapping.
    """
    path = code_baseline_path(prod_code)
    df = load_code_baseline_frame(path)
    if df.empty:
        return {}
    df = df.sort_values(['baseline_month', 'defect_desc'])
    return dict(zip(df['defect_desc'], df['baseline_rate'].astype(float)))


def build_code_baseline_lookup(
    baseline_df: pd.DataFrame,
) -> tuple[dict[tuple[str, str], tuple[float, float | None]], dict[str, float]]:
    by_month: dict[tuple[str, str], tuple[float, float | None]] = {}
    legacy: dict[str, float] = {}
    if baseline_df.empty:
        return by_month, legacy

    for _, row in baseline_df.iterrows():
        code = str(row['defect_desc'])
        rate = float(row['baseline_rate'])
        baseline_month = str(row.get('baseline_month', '')).strip()
        source_total_raw = row.get('source_total_panels', np.nan)
        source_total = None if pd.isna(source_total_raw) else float(source_total_raw)
        if baseline_month:
            by_month[(baseline_month, code)] = (rate, source_total)
        else:
            legacy[code] = rate
    return by_month, legacy


def first_stable_nonzero_day_rate(
    counts,
    totals,
    min_total_panels: int = CODE_BASELINE_FALLBACK_MIN_TOTAL_PANELS,
) -> float:
    if counts is None or totals is None:
        return 0.0

    for count_raw, total_raw in zip(counts, totals):
        count = pd.to_numeric(count_raw, errors='coerce')
        total = pd.to_numeric(total_raw, errors='coerce')
        if pd.isna(count) or pd.isna(total):
            continue
        if float(count) > 0 and float(total) > min_total_panels:
            return float(count) / float(total)
    return 0.0


def resolve_code_baseline_rate(
    by_month: dict[tuple[str, str], tuple[float, float | None]],
    legacy: dict[str, float],
    code: str,
    month: str,
    counts=None,
    totals=None,
) -> float:
    baseline = by_month.get((month, code))
    if baseline is not None:
        rate, source_total = baseline
        if rate <= 0 or (source_total is not None and source_total <= 0):
            return first_stable_nonzero_day_rate(counts, totals)
        return rate
    if code in legacy:
        rate = legacy[code]
        if rate <= 0:
            return first_stable_nonzero_day_rate(counts, totals)
        return rate
    return first_stable_nonzero_day_rate(counts, totals)

