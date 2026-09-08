"""Replace only supported latest CPK periods from an anomaly ledger."""
from __future__ import annotations

import math
from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import CPK_THRESHOLD
from src.inline_domain.core.spc.cpk_decoration import CPK_KEY_COLUMNS


def normalize_latest_cpk(frame: pd.DataFrame, product: str) -> pd.DataFrame:
    required = [*CPK_KEY_COLUMNS, "cpk_corrected", "flag"]
    result = frame.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").copy()
    # Excel may pre-fill replacement values on otherwise unused worksheet rows.
    if set(required).issubset(result.columns):
        result = result.dropna(how="all", subset=required)
    if result.empty:
        return pd.DataFrame(columns=[*required, "cpk", "status"])
    if missing := set(required) - set(result.columns):
        raise ValueError(f"{product} CPK 明细缺少列: {sorted(missing)}")
    for column in CPK_KEY_COLUMNS:
        result[column] = result[column].astype("string").str.strip()
        if result[column].isna().any() or result[column].eq("").any():
            raise ValueError(f"{product} CPK 明细业务键为空: {column}")
    if not result["prod_code"].eq(product).all():
        raise ValueError(f"{product} CPK 明细混入其他产品")
    flags = result["flag"].astype("string").str.strip().str.lower().fillna("")
    if not flags.isin(["false", "0", "0.0", "true", "1", "1.0", "delete", ""]).all():
        raise ValueError(f"{product} CPK 明细 flag 无效")
    result["flag"] = flags.replace({"0": "false", "0.0": "false", "1": "true", "1.0": "true"})
    result["cpk"] = pd.to_numeric(result["cpk_corrected"], errors="coerce")
    for _, group in result.groupby(CPK_KEY_COLUMNS, dropna=False):
        if len(group[["cpk", "flag"]].drop_duplicates()) > 1:
            raise ValueError(f"{product} CPK 明细重复项目存在冲突")
    result = result.drop_duplicates(CPK_KEY_COLUMNS).reset_index(drop=True)
    alert = result["flag"].isin(["false", "0", "0.0"]) & result["cpk"].lt(CPK_THRESHOLD)
    result["status"] = alert.map({True: "预警", False: "已修饰或达标"})
    unknown = result["flag"].isin(["false", "0", "0.0"]) & result["cpk"].isna()
    result.loc[unknown, "status"] = "无法判定"
    return result


def plan_latest_cpk(
    records: pd.DataFrame, detail: pd.DataFrame, *, products: Iterable[str],
    factories: Iterable[str], factory_key: str, as_of: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    previous = as_of.normalize() - pd.Timedelta(days=as_of.weekday() + 7)
    iso = previous.isocalendar()
    periods = [("week", "周度", f"{iso.year}-W{iso.week:02d}"),
               ("month", "月度", f"{as_of.year}-{as_of.month:02d}")]
    updates, statuses = [], []
    for product in products:
        for kind, period_type, label in periods:
            status = {"product": product, "period": label, "source": "preserved", "message": ""}
            rows = detail.loc[
                detail["prod_code"].eq(product) & detail["factory"].isin(factories)
                & detail["period_type"].eq(kind) & detail["period_label"].eq(label)
            ]
            baseline = records.loc[
                records["产品"].eq(product) & records["监控类型"].eq("SPC")
                & records["厂别"].eq(factory_key) & records["周期类型"].eq(period_type)
                & records["时间标签"].eq(label)
            ]
            if rows.empty or baseline.empty:
                status["message"] = "缺少对应周期明细或汇总基线，保留维护值"
            elif rows["status"].eq("无法判定").any():
                status["message"] = "未修饰项目缺少有效 CPK，保留维护值"
            else:
                row = baseline.iloc[0].copy()
                total = row["CPK总项目数"]
                alerts = int(rows.loc[rows["status"].eq("预警")].drop_duplicates(CPK_KEY_COLUMNS).shape[0])
                if pd.isna(total) or not math.isfinite(float(total)) or total <= 0 or int(total) != total:
                    status["message"] = "总项目数无效或为零，保留维护值"
                elif alerts > total:
                    raise ValueError(f"{product} {label} CPK 预警项目数超过汇总总项目数")
                else:
                    row["预警项目数"] = alerts
                    row["达标项目数"] = int(total) - alerts
                    row["Cpk≥1.33达标率"] = (int(total) - alerts) / int(total)
                    updates.append(row)
                    status["source"] = "excel"
            statuses.append(status)
    return pd.DataFrame(updates, columns=records.columns), pd.DataFrame(statuses)
