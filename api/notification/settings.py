"""Business configuration and explicit, secret-safe credential loading."""
from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
import os
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import yaml

from api.notification.core import DOMAINS, FACTORIES, NotificationError, NotificationSettings, mailbox
from tools.fetch_sheet_anomalies import validate_base_url


def read_yaml(path: Path) -> dict:
    try:
        value = yaml.safe_load(Path(path).read_text(encoding='utf-8-sig'))
    except (OSError, UnicodeError, yaml.YAMLError):
        raise NotificationError('无法读取有效的通知配置。') from None
    if not isinstance(value, dict):
        raise NotificationError('通知配置必须为字典。')
    return value


def _seconds(value: object, maximum: int) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not isfinite(value) or not 1 <= value <= maximum):
        raise NotificationError('通知超时配置无效。')
    return float(value)


def _list(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or any(not isinstance(v, str) or not v for v in value):
        raise NotificationError('通知路由列表无效。')
    if len(set(value)) != len(value):
        raise NotificationError('通知路由列表存在重复值。')
    return tuple(value)


def load_settings(path: Path) -> NotificationSettings:
    conf = read_yaml(path).get('notification')
    if not isinstance(conf, dict) or not isinstance(conf.get('routes'), list):
        raise NotificationError('缺少通知路由配置。')
    routes = {}
    for rule in conf['routes']:
        if not isinstance(rule, dict):
            raise NotificationError('通知路由无效。')
        domains, factories = _list(rule.get('sub_domains')), _list(rule.get('factories'))
        addresses = tuple(mailbox(v) for v in _list(rule.get('recipients')))
        if set(domains) - set(DOMAINS) or set(factories) - set(FACTORIES):
            raise NotificationError('通知分类或厂别配置无效。')
        for domain in domains:
            for factory in factories:
                if (domain, factory) in routes:
                    raise NotificationError('通知路由配置重叠。')
                routes[domain, factory] = addresses
    if set(routes) != {(d, f) for d in DOMAINS for f in FACTORIES}:
        raise NotificationError('通知路由未覆盖全部分类和厂别。')
    smtp = conf.get('smtp', {})
    if not isinstance(smtp, dict):
        raise NotificationError('SMTP 配置无效。')
    host, port = smtp.get('host', 'smtp.qiye.aliyun.com'), smtp.get('port', 587)
    security, policy = smtp.get('security', 'starttls'), conf.get('incomplete_policy', 'notify')
    if (not isinstance(host, str) or not host or any(c.isspace() for c in host)
            or type(port) is not int or not 1 <= port <= 65535
            or security not in ('starttls', 'ssl', 'plain') or policy not in ('notify', 'abort')):
        raise NotificationError('通知投递配置无效。')
    return NotificationSettings(
        validate_base_url(conf.get('api_base_url', 'http://127.0.0.1:8890')),
        _seconds(conf.get('api_timeout_seconds', 120), 120), MappingProxyType(routes), policy,
        host, port, security, _seconds(smtp.get('timeout_seconds', 30), 120))


@dataclass(frozen=True)
class MailCredentials:
    user: str = field(repr=False)
    sender: str
    passwords: tuple[str, ...] = field(repr=False)


def load_mail_credentials(path: Path | None = None, *, environ: Mapping[str, str] | None = None) -> MailCredentials:
    """The optional reference-format YAML contributes credentials ONLY, never recipients."""
    env = os.environ if environ is None else environ
    values = read_yaml(path) if path is not None else env
    user, sender = values.get('MAIL_USER'), values.get('MAIL_SENDER')
    raw = values.get('MAIL_PASSWORDS') or values.get('MAIL_PASSWORD')
    passwords = raw if isinstance(raw, list) else raw.split(',') if isinstance(raw, str) else []
    if (not isinstance(user, str) or not user or any(c in user for c in '\r\n')
            or not passwords or any(not isinstance(p, str) or not p for p in passwords)):
        raise NotificationError('缺少有效 MAIL_USER／MAIL_PASSWORD(S) 邮件凭证。')
    return MailCredentials(user, mailbox(sender), tuple(passwords))


def load_environment(path: Path | None = None, *, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Read only notification keys, without modifying the process environment."""
    env = os.environ if environ is None else environ
    keys = ('API_KEY', 'VIVO_API_KEY', 'MAIL_USER', 'MAIL_SENDER', 'MAIL_PASSWORD', 'MAIL_PASSWORDS')
    values = {}
    if path is not None:
        try:
            with Path(path).open(encoding='utf-8-sig') as stream:
                # python-dotenv otherwise logs invalid source lines. Keep diagnostics
                # within this boundary, and reject malformed credential lines safely.
                from dotenv.parser import parse_stream
                bindings = list(parse_stream(stream))
            if any(binding.error for binding in bindings):
                raise NotificationError('凭证环境文件格式无效。')
            values = {binding.key: binding.value for binding in bindings
                      if binding.key in keys and binding.value is not None}
        except (OSError, UnicodeError):
            raise NotificationError('无法读取凭证环境文件。') from None
    result = {key: env.get(key) or values.get(key, '') for key in keys}
    if env.get('API_KEY') or env.get('VIVO_API_KEY'):
        result['API_KEY'] = env.get('API_KEY') or env['VIVO_API_KEY']
    if env.get('MAIL_PASSWORD') and not env.get('MAIL_PASSWORDS'):
        result['MAIL_PASSWORDS'] = ''
    return result
