"""入库良率修饰表管理器。

职责：读取/解析 `resources/入库良率修饰表.xlsx`（企业加密，Sheet 按
`<产品型号>_Group级` / `<产品型号>_Code级` 划分），计算当月原始良损、
解析指定良损（含上月回退）、计算缩放倍数，并把结果写回工作簿。

列约定：`不良类型 | 周期类型 | 时间标签 | 当月良损 | 指定良损 | 缩放倍数`。
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

import pandas as pd

from src.shared_kernel.utils.excel_tools import replace_workbook_sheet

MODIFIER_TABLE_COLUMNS = [
    "不良类型",
    "周期类型",
    "时间标签",
    "当月良损",
    "指定良损",
    "缩放倍数",
]

COL_DEFECT = "不良类型"
COL_PERIOD = "周期类型"
COL_MONTH = "时间标签"
COL_RAW_LOSS = "当月良损"
COL_SPECIFIED_LOSS = "指定良损"
COL_SCALE_FACTOR = "缩放倍数"


def parse_rate_value(value) -> float | None:
    """解析良损数值：兼容 "1.03%" 字符串与 >1 的百分比防呆。空值返回 None。"""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "nan":
            return None
        try:
            if "%" in text:
                return float(text.replace("%", "")) / 100.0
            parsed = float(text)
        except ValueError:
            return None
    else:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
    if pd.isna(parsed):
        return None
    # 防呆：>1 视为业务人员漏写 % 的百分数
    if parsed > 1.0:
        parsed = parsed / 100.0
    return parsed


def _empty_table() -> pd.DataFrame:
    return pd.DataFrame(columns=MODIFIER_TABLE_COLUMNS)


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    """读取单个 Sheet；openpyxl 失败时回退 Excel COM（企业加密文件）。"""
    try:
        return pd.read_excel(xlsx_path, sheet_name=sheet_name, engine="openpyxl")
    except Exception as openpyxl_error:
        logging.warning(
            "[modifier_table] openpyxl 读取 %s[%s] 失败，尝试 COM: %s",
            xlsx_path.name,
            sheet_name,
            openpyxl_error,
        )
        from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

        return _read_encrypted_xlsx_via_com(xlsx_path, sheet_name=sheet_name)


def read_modifier_table(xlsx_path: Path, product_code: str) -> dict[str, pd.DataFrame]:
    """读取 `<产品>_Group级` / `<产品>_Code级` 两个 Sheet。

    文件不存在、Sheet 缺失或读取失败时，对应级别返回带标准列的空表（空表语义：
    调用方按"无指定"处理，回落原始数据）。
    """
    xlsx_path = Path(xlsx_path)
    table: dict[str, pd.DataFrame] = {}
    for level, suffix in (("group", "Group级"), ("code", "Code级")):
        if not xlsx_path.exists():
            table[level] = _empty_table()
            continue
        try:
            df = _read_sheet(xlsx_path, f"{product_code}_{suffix}")
        except Exception as error:
            logging.warning(
                "[modifier_table] 读取 %s 的 %s 失败，按空表处理: %s",
                xlsx_path.name,
                suffix,
                error,
            )
            df = _empty_table()
        if df.empty:
            df = _empty_table()
        else:
            missing = [c for c in MODIFIER_TABLE_COLUMNS if c not in df.columns]
            if missing:
                logging.warning(
                    "[modifier_table] %s[%s] 缺少列 %s，缺失列按空值补齐。",
                    xlsx_path.name,
                    suffix,
                    missing,
                )
                for column in missing:
                    df[column] = None
            df = df[MODIFIER_TABLE_COLUMNS]
        table[level] = df
    return table


def compute_current_month_loss(
    panel_details_df: pd.DataFrame,
    level: str,
    month: str,
) -> pd.Series:
    """计算目标月份的原始良损：当月不良 Panel 去重数 / 当月投入 Panel 去重数。

    Args:
        panel_details_df: 原始 panel 明细（每行一条不良记录）。
        level: "group" 按 defect_group 统计；"code" 按 defect_desc 统计。
        month: 目标月份，形如 "2026-07"。

    Returns:
        index 为不良类型、值为良损的 Series；当月无数据时返回空 Series。
    """
    key_column = {"group": "defect_group", "code": "defect_desc"}[level]
    if panel_details_df.empty:
        return pd.Series(dtype=float)

    working = panel_details_df.copy()
    working["warehousing_time"] = pd.to_datetime(
        working["warehousing_time"], format="%Y%m%d", errors="coerce"
    )
    working = working[
        working["warehousing_time"].dt.strftime("%Y-%m") == month
    ]
    if working.empty:
        return pd.Series(dtype=float)

    total_panels = working["panel_id"].nunique()
    defective = (
        working[working[key_column].notna()]
        .groupby(key_column)["panel_id"]
        .nunique()
    )
    if total_panels == 0:
        return pd.Series(dtype=float)
    return defective / total_panels


def _parse_table_rates(table_df: pd.DataFrame) -> pd.DataFrame:
    """把 `当月良损`/`指定良损` 列解析为 float 列（_raw_loss/_specified_loss）。"""
    parsed = table_df.copy()
    parsed["_raw_loss"] = parsed[COL_RAW_LOSS].map(parse_rate_value)
    parsed["_specified_loss"] = parsed[COL_SPECIFIED_LOSS].map(parse_rate_value)
    parsed[COL_MONTH] = parsed[COL_MONTH].astype(str).str.strip()
    parsed[COL_DEFECT] = parsed[COL_DEFECT].astype(str).str.strip()
    return parsed


def resolve_monthly_targets(
    table_df: pd.DataFrame,
    months: list[str],
) -> dict[str, dict[str, float]]:
    """解析每个不良类型在目标月份的目标良损。

    回退链：当月 `指定良损` → 最近一个有 `指定良损` 的上个月 → 当月 `当月良损`
    → 不给目标（表中无该月行且从未指定时，日度生成器保持原始日度不良数；
    原始数据即 `当月良损` 水准，与 Mapping 侧倍数 1.0 保持一致）。
    """
    months = sorted(str(m) for m in months)
    targets: dict[str, dict[str, float]] = {}
    if table_df.empty:
        return targets

    parsed = _parse_table_rates(table_df)
    for defect, rows in parsed.groupby(COL_DEFECT, sort=False):
        rows = rows.sort_values(COL_MONTH)
        specified_by_month = {
            r[COL_MONTH]: r["_specified_loss"]
            for _, r in rows.iterrows()
            if pd.notna(r["_specified_loss"])
        }
        raw_by_month = {
            r[COL_MONTH]: r["_raw_loss"]
            for _, r in rows.iterrows()
            if pd.notna(r["_raw_loss"])
        }
        defect_targets: dict[str, float] = {}
        for month in months:
            if month in specified_by_month:
                defect_targets[month] = specified_by_month[month]
                continue
            earlier = [m for m in specified_by_month if m < month]
            if earlier:
                defect_targets[month] = specified_by_month[max(earlier)]
                continue
            if month in raw_by_month:
                defect_targets[month] = raw_by_month[month]
            # 从未指定且表中无该月行：不给目标，日度生成器保持原始（= 当月良损水准）
        targets[defect] = defect_targets
    return targets


def compute_scale_factors(table_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    """缩放倍数 = round(回退后指定良损 / 当月良损, 2)。

    回退口径与 `resolve_monthly_targets` 一致：当月未指定时用最近上月的指定良损，
    保证趋势日度与 Mapping 缩放使用同一水准。从未指定或当月良损为 0/缺失时记 1.0。
    """
    factors: dict[tuple[str, str], float] = {}
    if table_df.empty:
        return factors

    parsed = _parse_table_rates(table_df)
    for defect, rows in parsed.groupby(COL_DEFECT, sort=False):
        rows = rows.sort_values(COL_MONTH)
        specified_by_month = {
            r[COL_MONTH]: r["_specified_loss"]
            for _, r in rows.iterrows()
            if pd.notna(r["_specified_loss"])
        }
        for _, row in rows.iterrows():
            month = row[COL_MONTH]
            raw = row["_raw_loss"]
            resolved = specified_by_month.get(month)
            if resolved is None:
                earlier = [m for m in specified_by_month if m < month]
                if earlier:
                    resolved = specified_by_month[max(earlier)]
            if resolved is None or pd.isna(raw) or not raw:
                factors[(defect, month)] = 1.0
            else:
                factors[(defect, month)] = round(resolved / raw, 2)
    return factors


def specified_signature(table_df: pd.DataFrame) -> str:
    """对 `不良类型|时间标签|指定良损` 的稳定哈希，用于检测人工指定是否改动。"""
    if table_df.empty:
        return "empty"
    parsed = _parse_table_rates(table_df)
    tokens = sorted(
        f"{r[COL_DEFECT]}|{r[COL_MONTH]}|{r['_specified_loss']}"
        for _, r in parsed.iterrows()
    )
    return hashlib.blake2b("|".join(tokens).encode("utf-8"), digest_size=8).hexdigest()


def _load_stored_signatures(signature_path: Path) -> dict[str, str]:
    try:
        return json.loads(Path(signature_path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _store_signatures(signature_path: Path, signatures: dict[str, str]) -> None:
    try:
        signature_path = Path(signature_path)
        signature_path.parent.mkdir(parents=True, exist_ok=True)
        signature_path.write_text(
            json.dumps(signatures, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as error:
        logging.warning("[modifier_table] 签名写入失败（忽略）: %s", error)


def _apply_current_month_loss(
    table_df: pd.DataFrame,
    loss: pd.Series,
    current_month: str,
) -> tuple[pd.DataFrame, bool]:
    """把当月原始良损合并进表：已有当月行更新，缺失行追加。

    返回 (更新后的表, 当月良损内容是否发生变化)。
    """
    updated = table_df.copy()
    changed = False

    if updated.empty:
        updated = pd.DataFrame(columns=MODIFIER_TABLE_COLUMNS)

    month_mask = updated[COL_MONTH].astype(str).str.strip() == current_month
    for defect, rate in loss.items():
        row_mask = month_mask & (updated[COL_DEFECT].astype(str).str.strip() == defect)
        if row_mask.any():
            existing = parse_rate_value(updated.loc[row_mask, COL_RAW_LOSS].iloc[0])
            if existing is None or abs(existing - rate) > 1e-12:
                updated.loc[row_mask, COL_RAW_LOSS] = rate
                changed = True
        else:
            new_row = {column: None for column in MODIFIER_TABLE_COLUMNS}
            new_row.update(
                {
                    COL_DEFECT: defect,
                    COL_PERIOD: "月度",
                    COL_MONTH: current_month,
                    COL_RAW_LOSS: rate,
                }
            )
            updated = pd.concat(
                [updated, pd.DataFrame([new_row])], ignore_index=True
            )
            changed = True
    return updated, changed


def sync_modifier_table(
    xlsx_path: Path,
    product_code: str,
    panel_details_df: pd.DataFrame,
    current_month: str,
    signature_path: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """同步修饰表：更新当月良损、按需重算缩放倍数并写回。

    - 每次调用只更新 `current_month` 的 `当月良损`（缺失行追加）；
    - `指定良损` 签名变化或当月良损内容变化时才写回工作簿；
    - 写回（含缩放倍数整列重算）失败仅记日志，不影响内存中的返回表；
    - 返回内存中的最新表（无论写回是否成功）。
    """
    xlsx_path = Path(xlsx_path)
    signature_path = Path(
        signature_path
        or xlsx_path.with_suffix(".sig.json")
    )
    stored = _load_stored_signatures(signature_path)
    table = read_modifier_table(xlsx_path, product_code)
    new_signatures: dict[str, str] = {}

    for level in ("group", "code"):
        sheet_name = f"{product_code}_{'Group级' if level == 'group' else 'Code级'}"
        loss = compute_current_month_loss(
            panel_details_df, level=level, month=current_month
        )
        updated, loss_changed = _apply_current_month_loss(
            table[level], loss, current_month
        )
        signature = specified_signature(updated)
        new_signatures[f"{product_code}:{level}"] = signature

        # 缩放倍数整列重算（口径含上月回退），保证与趋势生成一致
        factors = compute_scale_factors(updated)
        if not updated.empty:
            updated[COL_SCALE_FACTOR] = [
                factors.get(
                    (str(d).strip(), str(m).strip()), 1.0
                )
                for d, m in zip(updated[COL_DEFECT], updated[COL_MONTH])
            ]

        need_write = (
            not updated.empty
            and (loss_changed or stored.get(f"{product_code}:{level}") != signature)
        )
        if need_write:
            try:
                replace_workbook_sheet(xlsx_path, sheet_name, updated)
                logging.info(
                    "[modifier_table] 已写回 %s[%s]（当月良损%s更新，指定%s变化）。",
                    xlsx_path.name,
                    sheet_name,
                    "有" if loss_changed else "无",
                    "有" if stored.get(f"{product_code}:{level}") != signature else "无",
                )
            except Exception as error:
                logging.warning(
                    "[modifier_table] 写回 %s[%s] 失败（忽略，按内存数据继续）: %s",
                    xlsx_path.name,
                    sheet_name,
                    error,
                )
        table[level] = updated

    _store_signatures(signature_path, new_signatures)
    return table
