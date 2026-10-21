"""CVD 备件日期寿命和月增量模拟规则；只处理内存数据，不执行 I/O。"""

from datetime import date
from decimal import Decimal
import math
from numbers import Real
import re

import pandas as pd

from src.equipment_domain.core.parts_calculator import (
    PartsAlertPolicy,
    REPLACEMENT_PROGRESS_THRESHOLD,
    STATUS_REPLACED,
    apply_replacement_measurement_dates,
    calculate_warning_status,
)


CVD_SOURCE_COLUMNS = (
    "厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格",
    "站点", "机台号", "测量值", "进度", "测量时间",
)
CVD_REPORT_COLUMNS = (
    "厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格",
    "站点", "机台号-腔室", "测量值", "测量时间", "使用进度", "预警状态",
)
DAYS_PER_MONTH = 30


def _calendar_cycle(
    replacement: pd.Timestamp, current_day: pd.Timestamp, lifespan_months: int,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """以源更换日为日历锚点，定位当前寿命周期，支持跨越多个周期。"""
    elapsed_months = (
        (current_day.year - replacement.year) * 12
        + current_day.month - replacement.month
    )
    cycle = max(elapsed_months // lifespan_months, 0)
    cycle_start = replacement + pd.DateOffset(months=cycle * lifespan_months)
    if cycle > 0 and cycle_start > current_day:
        cycle -= 1
        cycle_start = replacement + pd.DateOffset(months=cycle * lifespan_months)
    expiry = replacement + pd.DateOffset(months=(cycle + 1) * lifespan_months)
    return cycle_start, expiry


def parse_cvd_sheet_name(
    sheet_name: str, *, as_of_date: str | date,
) -> tuple[str, pd.Timestamp]:
    """解析厂别-MMDD，日期取不晚于报告日的最近一次有效月日。"""
    match = re.fullmatch(r"([^-]+)-(\d{4})", sheet_name.strip())
    if match is None or not match[1].strip():
        raise ValueError(f"CVD Sheet 名称须为厂别-MMDD: {sheet_name!r}")
    factory = match[1].strip().upper()
    month, day = int(match[2][:2]), int(match[2][2:])
    try:
        date(2000, month, day)  # 用闰年校验月日，允许 0229。
    except ValueError as error:
        raise ValueError(f"CVD Sheet 更新日期无效: {sheet_name!r}") from error
    report_day = _date_value(as_of_date)
    # 跨世纪时 2 月 29 日的相邻有效年份最多相隔 8 年。
    for year in range(report_day.year, report_day.year - 9, -1):
        try:
            updated_at = pd.Timestamp(date(year, month, day))
        except ValueError:
            continue
        if updated_at <= report_day:
            return factory, updated_at
    raise ValueError(f"CVD Sheet 无法确定更新日期: {sheet_name!r}")


def _date_value(value: object) -> pd.Timestamp:
    """同时支持 Excel 日期序号、日期对象和日期文本。"""
    if isinstance(value, Real):
        timestamp = pd.Timestamp("1899-12-30") + pd.Timedelta(days=float(value))
    else:
        timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("日期不能为空")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp.normalize()


def _nonnegative_number(value: object, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name}必须为非负数") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name}必须为有限非负数")
    return number


def _measurement(
    row: pd.Series, current_day: pd.Timestamp, baseline_day: pd.Timestamp,
) -> tuple[object, float]:
    spec = row["寿命规格"]
    years = re.fullmatch(r"(\d+(?:\.\d+)?)\s*年", str(spec).strip())
    if years:
        replacement = _date_value(row["测量值"])
        lifespan_months = Decimal(years[1]) * 12
        if lifespan_months <= 0:
            raise ValueError("寿命规格必须大于零")
        if lifespan_months != lifespan_months.to_integral_value():
            raise ValueError("年限寿命规格必须对应整数月")
        replacement, expiry = _calendar_cycle(
            replacement, current_day, int(lifespan_months),
        )
        elapsed_days = max((current_day - replacement).days, 0)
        progress = elapsed_days / (expiry - replacement).days * 100
        return replacement.strftime("%Y-%m-%d"), progress

    limit = _nonnegative_number(spec, "寿命规格")
    if limit == 0:
        raise ValueError("寿命规格必须大于零")
    value = _nonnegative_number(row["测量值"], "测量值")
    monthly_increment = _nonnegative_number(row["进度"], "进度")
    measured_at = row["测量时间"]
    origin = baseline_day if pd.isna(measured_at) or measured_at == "" else _date_value(measured_at)
    elapsed_days = max((current_day - origin).days, 0)
    measurement = value + monthly_increment / DAYS_PER_MONTH * elapsed_days
    if not math.isfinite(measurement):
        raise ValueError("推算测量值必须为有限非负数")
    # 从源值按自身月增量重算并保留溢出量；不使用常规仿造的TTL增长参数。
    if measurement >= limit:
        measurement %= limit
    return measurement, measurement / limit * 100


def build_cvd_report(
    source: pd.DataFrame,
    *,
    as_of_date: str | date,
    baseline_date: str | date,
    policy: PartsAlertPolicy,
) -> pd.DataFrame:
    """按报告日期重算寿命周期，重复刷新不累加，来源数据不改写。"""
    missing = set(CVD_SOURCE_COLUMNS) - set(source.columns)
    if missing:
        raise ValueError(f"CVD 数据缺少字段: {sorted(missing)}")
    current_day = _date_value(as_of_date)
    baseline_day = _date_value(baseline_date)
    records = []
    for index, row in source.iterrows():
        try:
            measurement, progress = _measurement(row, current_day, baseline_day)
        except (ValueError, TypeError, OverflowError) as error:
            raise ValueError(f"CVD 行 {index}: {error}") from error
        if progress > REPLACEMENT_PROGRESS_THRESHOLD:
            # 更换后的报告进度为0，测量值在计算结束后统一投影为更换日期。
            progress, status = 0.0, STATUS_REPLACED
        else:
            status = calculate_warning_status(progress, policy)
        record = {column: row[column] for column in CVD_SOURCE_COLUMNS[:7]}
        station = str(row["站点"]).strip()
        record.update({
            "站点": re.sub(r"^(\d+)\.0+$", r"\1", station),
            "机台号-腔室": str(row["机台号"]).strip(),
            "测量值": measurement,
            "测量时间": current_day,
            "使用进度": progress,
            "预警状态": status,
        })
        records.append(record)
    return apply_replacement_measurement_dates(
        pd.DataFrame(records, columns=CVD_REPORT_COLUMNS), as_of_date=current_day.date(),
    )
