"""EMA and deterministic-noise steps for MWD trend processing."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd

from src.shared_kernel.config_model import AppConfig
from .code_baseline import (
    CODE_BASELINE_COLUMNS,
    build_code_baseline_lookup,
    code_baseline_path,
    code_baseline_sheet_name,
    defect_multipliers_signature,
    ensure_code_baseline_current,
    load_code_baseline_frame,
    resolve_code_baseline_rate,
)


def calculate_adaptive_shadow_ema(
    counts: np.ndarray,
    totals: np.ndarray,
    span: int,
    use_global_init: bool = True,
    initial_anchor_rate: float | None = None,
) -> list[float]:
    """Calculate the existing shadow EMA series for one calendar month."""
    n = len(counts)
    if n == 0:
        return []

    alpha = 2 / (span + 1)
    result: list[float] = []
    denominator_momentum = totals[0]
    first_rate = counts[0] / totals[0] if totals[0] > 0 else 0

    if initial_anchor_rate is not None:
        numerator_momentum = denominator_momentum * initial_anchor_rate
        if totals[0] > 0:
            result.append(0.5 * initial_anchor_rate + 0.5 * first_rate)
        else:
            result.append(initial_anchor_rate)
    elif use_global_init:
        # The initial shadow anchor must not be contaminated by a later
        # incident day. Use the first observed stable rate as the seed; the
        # rolling momentum below still incorporates normal fluctuations.
        base = first_rate
        numerator_momentum = denominator_momentum * base
        result.append(0.5 * base + 0.5 * first_rate)
    else:
        base = 0.0
        numerator_momentum = denominator_momentum * first_rate
        result.append(first_rate)

    for count, total in zip(counts[1:], totals[1:]):
        if total == 0:
            result.append(0.0)
            continue

        real_rate = count / total
        if denominator_momentum > 0:
            base_rate = numerator_momentum / denominator_momentum
        else:
            base_rate = (
                initial_anchor_rate
                if initial_anchor_rate is not None
                else (base if use_global_init else 0)
            )

        is_surge_abs = abs(real_rate - base_rate) > 0.05
        is_surge_ratio = real_rate > base_rate * 5.0
        is_plunge_ratio = real_rate < base_rate / 3.0 or real_rate < 1e-4

        if is_surge_abs or is_surge_ratio or is_plunge_ratio:
            # Show the exceptional day's actual rate, but do not feed it into
            # the internal momentum. The following day can therefore recover
            # immediately instead of carrying an EMA tail.
            if initial_anchor_rate is None and real_rate > base_rate:
                result.append(real_rate)
            else:
                result.append(base_rate)
            continue

        numerator_momentum = alpha * count + (1 - alpha) * numerator_momentum
        denominator_momentum = alpha * total + (1 - alpha) * denominator_momentum
        result.append(
            numerator_momentum / denominator_momentum
            if denominator_momentum > 0
            else 0.0
        )

    return result


def inject_deterministic_noise(
    df: pd.DataFrame,
    columns: list[str],
    volatility: float,
) -> pd.DataFrame:
    """Apply stable row noise to wide Group counts."""
    result = df.copy()
    for column in columns:
        if column not in result:
            continue
        for index, row_index in enumerate(result.index):
            value = result.loc[row_index, column]
            if value == 0:
                continue
            noise = np.sin(index * 12.345 + len(column) * 6.78) * volatility
            result.loc[row_index, column] = int(max(0, value * (1 + noise)))
    return result


def inject_deterministic_noise_code_level(
    df: pd.DataFrame,
    volatility: float,
) -> pd.DataFrame:
    """Apply vectorized stable noise to Code-level daily counts."""
    if df.empty:
        return df.copy()

    result = df.copy()
    timestamps = result["warehousing_time"].astype("int64") // 10**9
    code_hash = result["defect_desc"].fillna("NoDefect").map(
        lambda value: sum(ord(char) for char in str(value))
    ) % 1000
    noise = np.sin(timestamps + code_hash) * volatility
    result["defect_panel_count"] = np.round(
        result["defect_panel_count"] * (1 + noise)
    ).astype(int).clip(lower=0)
    return result


def calculate_code_ema_noise(
    raw_df: pd.DataFrame,
    span: int,
    scale: float,
    volatility: float,
    config: AppConfig | None = None,
    prod_code: str | None = None,
    ensure_baseline_current: Callable = ensure_code_baseline_current,
    load_baseline_frame: Callable = load_code_baseline_frame,
) -> pd.DataFrame:
    """Build the Code-level calendar EMA and apply stable noise."""
    if raw_df.empty:
        return raw_df.copy()

    ema_df = raw_df.copy()
    unique_codes = ema_df["defect_desc"].unique()
    result_frames: list[pd.DataFrame] = []

    if prod_code:
        baseline_df = ensure_baseline_current(
            ema_df,
            prod_code,
            defect_multipliers_signature=defect_multipliers_signature(config),
        )
        if baseline_df.empty:
            baseline_df = load_baseline_frame(
                code_baseline_path(prod_code),
                code_baseline_sheet_name(prod_code),
            )
    else:
        baseline_df = pd.DataFrame(columns=CODE_BASELINE_COLUMNS)

    baseline_by_month, legacy_baseline_map = build_code_baseline_lookup(baseline_df)

    for code in unique_codes:
        if code == "NoDefect":
            result_frames.append(ema_df[ema_df["defect_desc"] == code].copy())
            continue

        mask = ema_df["defect_desc"] == code
        sub = ema_df[mask].sort_values("warehousing_time").copy()
        sub["warehousing_time"] = pd.to_datetime(sub["warehousing_time"])

        min_date = sub["warehousing_time"].min()
        max_date = sub["warehousing_time"].max()
        full_dates = pd.date_range(
            start=min_date.replace(day=1),
            end=max_date,
            freq="D",
        )
        sub = pd.merge(
            pd.DataFrame({"warehousing_time": full_dates}),
            sub,
            on="warehousing_time",
            how="left",
        )
        sub["defect_desc"] = code
        group_candidates = ema_df.loc[mask, "defect_group"].dropna()
        group_value = group_candidates.iloc[0] if not group_candidates.empty else "Unknown"
        sub["defect_group"] = sub["defect_group"].fillna(group_value)
        sub["total_panels"] = sub["total_panels"].fillna(0).astype(int)
        sub["defect_panel_count"] = sub["defect_panel_count"].fillna(0).astype(int)

        sub["year_month"] = sub["warehousing_time"].dt.to_period("M").astype(str)
        smooth_all: list[float] = []
        for month in sorted(sub["year_month"].unique()):
            month_sub = sub[sub["year_month"] == month].sort_values("warehousing_time")
            anchor_rate = resolve_code_baseline_rate(
                baseline_by_month,
                legacy_baseline_map,
                str(code),
                str(month),
                counts=month_sub["defect_panel_count"].to_numpy(),
                totals=month_sub["total_panels"].to_numpy(),
            )
            smooth_all.extend(
                calculate_adaptive_shadow_ema(
                    month_sub["defect_panel_count"].to_numpy(),
                    month_sub["total_panels"].to_numpy(),
                    span,
                    use_global_init=False,
                    initial_anchor_rate=anchor_rate,
                )
            )

        sub = sub.sort_values("warehousing_time").copy()
        sub["defect_panel_count"] = np.round(
            np.asarray(smooth_all) * scale * sub["total_panels"].to_numpy()
        ).astype(int)
        result_frames.append(sub.drop(columns=["year_month"], errors="ignore"))

    return inject_deterministic_noise_code_level(
        pd.concat(result_frames, ignore_index=True),
        volatility,
    )
