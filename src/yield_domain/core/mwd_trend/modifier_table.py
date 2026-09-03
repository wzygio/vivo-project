"""入库良率修饰表管理器。

职责：读取/解析 `resources/yield_domain/入库良率修饰表.xlsx`（企业加密，Sheet 按
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

from src.shared_kernel.utils.excel_tools import (
    read_workbook_sheet,
    replace_workbook_sheet,
)

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

# 小数位约定：良损按"百分数保留三位小数"存储（分数即 5 位小数，如 1.383% → 0.01383），
# 与历史 codebaseline `np.round(rate, 5)` 约定一致；缩放倍数为比值，保留 3 位小数。
RATE_DECIMALS = 5
FACTOR_DECIMALS = 3


class ModifierTableValidationError(ValueError):
    """修饰表包含无法安全用于良损计算的数据。"""


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


def _validate_rate_values(
    table_df: pd.DataFrame,
    *,
    product_code: str,
    sheet_name: str,
) -> None:
    """校验良损字段，并在错误中保留可定位的业务上下文。"""
    for _, row in table_df.iterrows():
        defect = str(row[COL_DEFECT]).strip()
        month = str(row[COL_MONTH]).strip()
        for column in (COL_RAW_LOSS, COL_SPECIFIED_LOSS):
            raw_value = row[column]
            if pd.isna(raw_value) or (
                isinstance(raw_value, str) and not raw_value.strip()
            ):
                continue
            parsed = parse_rate_value(raw_value)
            if parsed is None or not 0.0 <= parsed <= 1.0:
                raise ModifierTableValidationError(
                    "修饰表良损必须满足 0 <= 良损 <= 1："
                    f"产品={product_code}, Sheet={sheet_name}, Code={defect}, "
                    f"月份={month}, 字段={column}, 原值={raw_value!r}"
                )


def _empty_table() -> pd.DataFrame:
    return pd.DataFrame(columns=MODIFIER_TABLE_COLUMNS)


def _read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    """复用共享读取边界；缺 Sheet 返回空表，加密文件才回退 COM。"""
    return read_workbook_sheet(xlsx_path, sheet_name)


def read_modifier_table(xlsx_path: Path, product_code: str) -> dict[str, pd.DataFrame]:
    """读取 `<产品>_Group级` / `<产品>_Code级` 两个 Sheet。

    文件不存在或 Sheet 缺失时，对应级别返回带标准列的空表（空表语义：调用方
    按"无指定"处理）；工作簿无法读取时抛出异常，避免误把人工指定当作空值。
    """
    xlsx_path = Path(xlsx_path)
    table: dict[str, pd.DataFrame] = {}
    for level, suffix in (("group", "Group级"), ("code", "Code级")):
        if not xlsx_path.exists():
            table[level] = _empty_table()
            continue
        sheet_name = f"{product_code}_{suffix}"
        df = _read_sheet(xlsx_path, sheet_name)
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
            _validate_rate_values(
                df,
                product_code=product_code,
                sheet_name=sheet_name,
            )
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
    return compute_current_month_losses(panel_details_df, month)[level]


def compute_current_month_losses(
    panel_details_df: pd.DataFrame,
    month: str,
) -> dict[str, pd.Series]:
    """一次准备当月 Panel 切片，同时返回 Group/Code 两级原始良损。"""
    empty = {
        "group": pd.Series(dtype=float),
        "code": pd.Series(dtype=float),
    }
    if panel_details_df.empty:
        return empty

    working = panel_details_df[
        ["warehousing_time", "panel_id", "defect_group", "defect_desc"]
    ].copy()
    working["warehousing_time"] = pd.to_datetime(
        working["warehousing_time"], format="%Y%m%d", errors="coerce"
    )
    month_period = pd.Period(month, freq="M")
    month_start = month_period.start_time
    next_month_start = (month_period + 1).start_time
    working = working[
        working["warehousing_time"].ge(month_start)
        & working["warehousing_time"].lt(next_month_start)
    ]
    if working.empty:
        return empty

    total_panels = working["panel_id"].nunique()
    if total_panels == 0:
        return empty

    losses: dict[str, pd.Series] = {}
    for level, key_column in (
        ("group", "defect_group"),
        ("code", "defect_desc"),
    ):
        defective = (
            working[working[key_column].notna()]
            .groupby(key_column)["panel_id"]
            .nunique()
        )
        losses[level] = defective / total_panels
    return losses


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
    → 不给修饰目标。表中无该月行且从未指定时，Code 日度生成阶段使用从 Panel 明细
    按月汇总的原始月度良损，不回落原始日度不良数。
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
            # 从未指定且表中无该月行：不给修饰目标，日度生成器回退原始月度良损。
        targets[defect] = defect_targets
    return targets


def compute_scale_factors(table_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    """缩放倍数 = round(回退后指定良损 / 当月良损, 3)（保留三位小数）。

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
                factors[(defect, month)] = round(resolved / raw, FACTOR_DECIMALS)
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

    写入前按 `RATE_DECIMALS`（百分数三位小数）舍入。
    返回 (更新后的表, 当月良损内容是否发生变化)。
    """
    updated = table_df.copy()
    changed = False

    if updated.empty:
        updated = pd.DataFrame(columns=MODIFIER_TABLE_COLUMNS)

    month_mask = updated[COL_MONTH].astype(str).str.strip() == current_month
    for defect, rate in loss.items():
        rate = round(float(rate), RATE_DECIMALS)
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
    read_only: bool = False,
) -> dict[str, pd.DataFrame]:
    """同步修饰表：更新当月良损、按需重算缩放倍数并写回。

    - 每次调用只更新 `current_month` 的 `当月良损`（缺失行追加）；
    - `指定良损` 签名变化或当月良损内容变化时才写回工作簿；
    - 写回（含缩放倍数整列重算）失败不影响内存中的返回表，但保留旧签名以便重试；
    - 返回内存中的最新表（无论写回是否成功）；
    - ``read_only=True``（矩阵等只读消费方）：内存合并口径完全一致，
      但绝不写工作簿、绝不写签名文件。
    """
    xlsx_path = Path(xlsx_path)
    signature_path = Path(
        signature_path
        or xlsx_path.with_suffix(".sig.json")
    )
    stored = _load_stored_signatures(signature_path)
    table = read_modifier_table(xlsx_path, product_code)
    committed_signatures = stored.copy()
    current_month_losses = compute_current_month_losses(
        panel_details_df, month=current_month
    )

    for level in ("group", "code"):
        sheet_name = f"{product_code}_{'Group级' if level == 'group' else 'Code级'}"
        loss = current_month_losses[level]
        updated, loss_changed = _apply_current_month_loss(
            table[level], loss, current_month
        )
        signature = specified_signature(updated)
        signature_key = f"{product_code}:{level}"

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
            not read_only
            and not updated.empty
            and (loss_changed or stored.get(signature_key) != signature)
        )
        if need_write:
            try:
                write_succeeded = replace_workbook_sheet(
                    xlsx_path,
                    sheet_name,
                    updated,
                )
            except Exception as error:
                write_succeeded = False
                logging.error(
                    "[modifier_table] 写回 %s[%s] 失败，保留旧签名以便重试: %s",
                    xlsx_path.name,
                    sheet_name,
                    error,
                    exc_info=True,
                )
            if write_succeeded:
                committed_signatures[signature_key] = signature
                logging.info(
                    "[modifier_table] 已写回 %s[%s]（当月良损%s更新，指定%s变化）。",
                    xlsx_path.name,
                    sheet_name,
                    "有" if loss_changed else "无",
                    "有" if stored.get(signature_key) != signature else "无",
                )
            else:
                logging.error(
                    "[modifier_table] 未写回 %s[%s]，签名未推进；下次同步将重试。",
                    xlsx_path.name,
                    sheet_name,
                )
        table[level] = updated

    if not read_only:
        _store_signatures(signature_path, committed_signatures)
    return table
