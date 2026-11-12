"""Pure notification planning, with no HTTP, configuration or SMTP access."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from typing import Any, Mapping


DOMAINS = ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')
FACTORIES = ('ARRAY', 'OLED', 'TP')
TEST_RECIPIENT = 'wangzy03@visionox.com'
PUBLIC_FIELDS = ('分类', '发生站点', '部门', '层别', '监控参数', '异常事件',
                 '对象类型', '对象编号', '产品型号', '发生时间', '命中边界',
                 '指标值', '达标下限', '统计周期类型', '统计周期', '统计开始日期', '统计截止日期')


class NotificationError(ValueError):
    """Safe operational error; never include credential-bearing source values."""


def mailbox(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)+', value):
        raise NotificationError('邮箱配置无效。')
    return value


@dataclass(frozen=True)
class NotificationSettings:
    api_base_url: str
    api_timeout_seconds: float
    routes: Mapping[tuple[str, str], tuple[str, ...]]
    incomplete_policy: str = 'notify'
    smtp_host: str = 'smtp.qiye.aliyun.com'
    smtp_port: int = 587
    smtp_security: str = 'starttls'
    smtp_timeout_seconds: float = 30


@dataclass(frozen=True)
class NotificationPlan:
    recipient: str
    records: tuple[dict, ...]
    failed_sources: tuple[dict, ...]
    complete: bool
    period_start: str
    period_end: str
    limitations: tuple[str, ...]
    test_mode: bool


def _period(meta: dict) -> tuple[str, str]:
    try:
        start = datetime.fromisoformat(meta['period_start'])
        end = datetime.fromisoformat(meta['period_end_exclusive'])
        if (start.tzinfo is None or end.tzinfo is None or end <= start
                or meta.get('timezone') != 'Asia/Shanghai'):
            raise ValueError
        return start.date().isoformat(), (end - timedelta(microseconds=1)).date().isoformat()
    except (KeyError, TypeError, ValueError):
        raise NotificationError('API 查询周期无效。') from None


def plan_notifications(result: dict, settings: NotificationSettings, *, test_mode: bool = False) -> tuple[NotificationPlan, ...]:
    """Plan all deliveries first; unknown routes cannot cause a half-routed run."""
    rows, meta = result.get('data'), result.get('meta')
    if not isinstance(rows, list) or not isinstance(meta, dict):
        raise NotificationError('API 响应无效。')
    failures = meta.get('failed_sources')
    if (type(meta.get('complete')) is not bool or not isinstance(failures, list)
            or meta['complete'] != (not failures) or meta.get('has_more') is not False
            or meta.get('returned_count') != len(rows)):
        raise NotificationError('API 完整性说明无效。')
    if not meta['complete'] and settings.incomplete_policy == 'abort':
        raise NotificationError('部分来源不可用，本次已按配置取消发送。')
    start, end = _period(meta)
    grouped: dict[str, dict[str, list]] = {}

    def recipients(domain: Any, factory: Any) -> tuple[str, ...]:
        if not isinstance(domain, str) or not isinstance(factory, str):
            raise NotificationError('预警分类或厂别缺失，无法分派。')
        addresses = settings.routes.get((domain, factory))
        if not addresses:
            raise NotificationError('预警分类或厂别未配置责任人。')
        return (TEST_RECIPIENT,) if test_mode else addresses

    def add(address: str, key: str, item: dict) -> None:
        group = grouped.setdefault(address, {'records': [], 'failures': []})
        if key == 'records' or item not in group[key]:
            group[key].append(item)

    for row in rows:
        if not isinstance(row, dict):
            raise NotificationError('预警记录无效。')
        public = {key: row[key] for key in PUBLIC_FIELDS if key in row}
        for address in recipients(row.get('分类'), row.get('部门')):
            add(address, 'records', public)
    for failure in failures:
        if not isinstance(failure, dict) or failure.get('sub_domain') not in DOMAINS:
            raise NotificationError('不可用来源分类无效。')
        domain = failure['sub_domain']
        factories = (failure['factory'],) if failure.get('factory') else FACTORIES
        public_failure = {key: failure[key] for key in ('product', 'sub_domain', 'event_type', 'factory') if key in failure}
        for factory in factories:
            for address in recipients(domain, factory):
                add(address, 'failures', public_failure)
    limitations = meta.get('limitations', [])
    if not isinstance(limitations, list) or any(not isinstance(v, str) for v in limitations):
        raise NotificationError('API 数据限制说明无效。')
    return tuple(NotificationPlan(address, tuple(group['records']), tuple(group['failures']),
                                  meta['complete'], start, end, tuple(limitations), test_mode)
                 for address, group in sorted(grouped.items()))
