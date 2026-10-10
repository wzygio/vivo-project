"""Complete calendar baselines and update only the latest complete CPK week."""
from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import date

import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import CPK_THRESHOLD, capability_summary_columns
from src.inline_domain.core.monitor.period_summary import _period_windows
from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion, exclude_inline_period_records
from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS, capability_alert_span_mask
from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range


def normalize_latest_cpk(frame: pd.DataFrame, product: str, *, metric: str = "cpk") -> pd.DataFrame:
    capability_summary_columns(metric)
    name, value_column = metric.upper(), f"{metric}_corrected"
    required = [*CPK_KEY_COLUMNS, value_column, "flag"]
    result = frame.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").copy()
    # Excel may pre-fill replacement values on otherwise unused worksheet rows.
    if set(required).issubset(result.columns):
        result = result.dropna(how="all", subset=required)
    if result.empty:
        return pd.DataFrame(columns=[*required, metric, "status"])
    if missing := set(required) - set(result.columns):
        raise ValueError(f"{product} {name} 明细缺少列: {sorted(missing)}")
    for column in CPK_KEY_COLUMNS:
        result[column] = result[column].astype("string").str.strip()
        if result[column].isna().any() or result[column].eq("").any():
            raise ValueError(f"{product} {name} 明细业务键为空: {column}")
    if not result["prod_code"].eq(product).all():
        raise ValueError(f"{product} {name} 明细混入其他产品")
    flags = result["flag"].astype("string").str.strip().str.lower().fillna("")
    if not flags.isin(["false", "0", "0.0", "true", "1", "1.0", "delete", ""]).all():
        raise ValueError(f"{product} {name} 明细 flag 无效")
    result["flag"] = flags.replace({"0": "false", "0.0": "false", "1": "true", "1.0": "true"})
    result[metric] = pd.to_numeric(result[value_column], errors="coerce")
    for _, group in result.groupby(CPK_KEY_COLUMNS, dropna=False):
        if len(group[[metric, "flag"]].drop_duplicates()) > 1:
            raise ValueError(f"{product} {name} 明细重复项目存在冲突")
    result = result.drop_duplicates(CPK_KEY_COLUMNS).reset_index(drop=True)
    alert = result["flag"].isin(["false", "0", "0.0"]) & result[metric].lt(CPK_THRESHOLD)
    insufficient_span = alert & result["period_type"].eq("week") & ~capability_alert_span_mask(result)
    alert &= ~insufficient_span
    result["status"] = alert.map({True: "预警", False: "已修饰或达标"})
    result.loc[insufficient_span, "status"] = "数据跨度不足"
    unknown = result["flag"].isin(["false", "0", "0.0"]) & result[metric].isna()
    result.loc[unknown, "status"] = "无法判定"
    return result


def latest_capability_alerts(
    detail: pd.DataFrame | None, reference_date: date, *, metric: str = "cpk",
    date_exclusion: InlineDateExclusion | None = None,
) -> pd.DataFrame:
    """Select alerts from normalized saved records, never a computed capability report."""
    capability_summary_columns(metric)
    columns = ["factory", "step_id", "param_name", "period_label", metric]
    required = {*columns, "period_type", "status"}
    if detail is None or detail.empty or not required.issubset(detail.columns):
        return pd.DataFrame(columns=columns)
    frame = exclude_inline_period_records(detail, date_exclusion, as_of=pd.Timestamp(reference_date))
    start, _ = previous_iso_week_range(reference_date)
    iso = start.isocalendar()
    mask = (frame["period_type"].eq("week")
            & frame["period_label"].eq(f"{iso.year}-W{iso.week:02d}")
            & frame["status"].eq("预警")
            & capability_alert_span_mask(frame))
    return frame.loc[mask, columns].reset_index(drop=True)


def _complete_cpk_baselines(
    records: pd.DataFrame, *, products: Iterable[str], factory_key: str,
    as_of: pd.Timestamp,
    metric: str = "cpk",
) -> tuple[pd.DataFrame, list[pd.Series]]:
    """Complete the displayed calendar without deriving totals from anomaly rows."""
    columns = capability_summary_columns(metric)
    existing = {
        (row["产品"], row["周期类型"], row["时间标签"]): row
        for _, row in records.loc[
            records["监控类型"].eq("SPC") & records["厂别"].eq(factory_key)
        ].iterrows()
    }
    rows, missing = [], []
    for product in products:
        for window in _period_windows(as_of, week_count=4):
            key = (product, window.period_type, window.time_label)
            if key in existing:
                row = existing[key].copy()
            else:
                row = pd.Series({
                    "产品": product, "监控类型": "SPC", "厂别": factory_key,
                    "周期类型": window.period_type, "时间标签": window.time_label,
                    "显示标签": window.display_label,
                    **dict.fromkeys(columns[6:], 0),
                })
                missing.append(row)
            rows.append(row)
    return pd.DataFrame(rows, columns=columns), missing


def plan_latest_cpk(
    records: pd.DataFrame, detail: pd.DataFrame, *, products: Iterable[str],
    factories: Iterable[str], factory_key: str, as_of: pd.Timestamp,
    date_exclusion: InlineDateExclusion | None = None,
    metric: str = "cpk",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = capability_summary_columns(metric)
    products, factories = tuple(dict.fromkeys(products)), tuple(factories)
    previous = as_of.normalize() - pd.Timedelta(days=as_of.weekday() + 7)
    iso = previous.isocalendar()
    label = f"{iso.year}-W{iso.week:02d}"
    completed, updates = _complete_cpk_baselines(
        records, products=products, factory_key=factory_key, as_of=as_of, metric=metric,
    )
    statuses = []
    for product in products:
        status = {"product": product, "period": label, "source": "preserved", "message": ""}
        baseline = completed.loc[
            completed["产品"].eq(product) & completed["周期类型"].eq("周度")
            & completed["时间标签"].eq(label)
        ]
        if exclude_inline_period_records(
            baseline, date_exclusion, as_of=as_of, factory_column="厂别",
            period_type_column="周期类型", period_label_column="时间标签",
        ).empty:
            row = baseline.iloc[0].copy()
            row[columns[6:]] = 0
            updates = [item for item in updates if not (
                item["产品"] == product and item["时间标签"] == label
                and item["周期类型"] == "周度"
            )]
            updates.append(row)
            status.update(source="excluded", message="最新完整周命中日期排除规则，汇总值置零")
        else:
            rows = detail.loc[
                detail["prod_code"].eq(product) & detail["factory"].isin(factories)
                & detail["period_type"].eq("week") & detail["period_label"].eq(label)
            ] if not detail.empty else detail
            row = baseline.iloc[0].copy()
            total = row[columns[6]]
            if rows.empty:
                status["message"] = "缺少对应周期明细，保留维护值或补零基线"
            elif rows["status"].eq("无法判定").any():
                status["message"] = f"未修饰项目缺少有效 {metric.upper()}，保留维护值"
            elif pd.isna(total) or not math.isfinite(float(total)) or total <= 0 or int(total) != total:
                status["message"] = "总项目数无效或为零，保留维护值或补零基线"
            else:
                alerts = int(rows.loc[rows["status"].eq("预警")].drop_duplicates(CPK_KEY_COLUMNS).shape[0])
                if alerts > total:
                    raise ValueError(f"{product} {label} {metric.upper()} 预警项目数超过汇总总项目数")
                row["预警项目数"] = alerts
                row["达标项目数"] = int(total) - alerts
                row[columns[7]] = (int(total) - alerts) / int(total)
                updates.append(row)
                status["source"] = "excel"
        statuses.append(status)
    return pd.DataFrame(updates, columns=columns), pd.DataFrame(statuses)
