"""HTTP and SMTP adapters, plus public-only email formatting."""
from __future__ import annotations

from collections import Counter
import csv
from datetime import datetime
from email.message import EmailMessage
from html import escape
from io import StringIO
import json
import smtplib
import ssl
from typing import Callable

from api.notification.core import NotificationError, NotificationPlan, NotificationSettings, PUBLIC_FIELDS
from api.notification.settings import MailCredentials
from tools.fetch_sheet_anomalies import fetch_anomalies


class HttpAlertSource:
    def __init__(self, settings: NotificationSettings, api_key: str, *, opener=None):
        self.settings, self._key, self.opener = settings, api_key, opener

    def fetch(self) -> dict:
        # Omit params AND page_size: the API owns the previous-week/all-scope defaults.
        return fetch_anomalies(self.settings.api_base_url, self._key,
                               timeout=self.settings.api_timeout_seconds, opener=self.opener)


def _cell(value: object) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    text = '' if value is None else str(value)
    if text.lstrip().startswith(('=', '+', '-', '@')) or text.startswith(('\t', '\r', '\n')):
        return "'" + text
    return text


def _html_table(headers: tuple[str, ...], rows: list[list[str]], *, widths: tuple[int, ...] = ()) -> str:
    """Inline styles and table attributes also work in restricted mail clients."""
    min_width = 'min-width:880px;' if widths else ''
    parts = ['<div style="width:100%;max-width:100%;overflow-x:auto;">',
             '<table width="100%" cellpadding="0" cellspacing="0" '
             f'style="width:100%;{min_width}border-collapse:collapse;table-layout:fixed;margin:12px 0 24px;'
             'font-size:13px;line-height:1.6;text-align:left;">', '<thead><tr>']
    for index, header in enumerate(headers):
        width = f'width:{widths[index]}%;' if widths else ''
        parts.append(f'<th scope="col" bgcolor="#243b53" style="{width}background-color:#243b53;'
                     'color:#ffffff;padding:10px 12px;font-weight:600;text-align:left;'
                     'border:1px solid #243b53;">' + escape(header) + '</th>')
    parts.append('</tr></thead><tbody>')
    for index, cells in enumerate(rows):
        color = '#ffffff' if index % 2 == 0 else '#f3f6f9'
        parts.append('<tr>')
        for cell in cells:
            parts.append(f'<td valign="top" bgcolor="{color}" style="background-color:{color};'
                         'padding:10px 12px;border:1px solid #dce3eb;vertical-align:top;'
                         'overflow-wrap:anywhere;word-wrap:break-word;">' + cell + '</td>')
        parts.append('</tr>')
    parts.append('</tbody></table></div>')
    return ''.join(parts)


def _display(value: object) -> str:
    return escape('' if value is None else str(value))


def _stack(first: object, second: object) -> str:
    return _display(first) + ('<br><span style="color:#526477;">' + _display(second) + '</span>' if second else '')


def _number(value: object) -> str:
    if isinstance(value, float):
        return f'{value:.4f}'.rstrip('0').rstrip('.')
    return '' if value is None else str(value)


def _preview_row(record: dict) -> list[str]:
    identifiers = record.get('对象编号')
    if isinstance(identifiers, list):
        ids = '<br>'.join(_display(value) for value in identifiers[:5])
        if len(identifiers) > 5:
            ids += f'<br><span style="color:#526477;">共 {len(identifiers)} 片，完整名单见附件</span>'
    else:
        ids = _display(identifiers)
    ids = '<span style="word-break:break-all;overflow-wrap:anywhere;">' + ids + '</span>'
    date = record.get('发生时间')
    try:
        parsed = datetime.fromisoformat(date)
        date = parsed.strftime('%Y-%m-%d %H:%M') if 'T' in date else parsed.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass  # Keep unrecognized public date text rather than inventing a timestamp.
    metric = _number(record.get('指标值'))
    lower = _number(record.get('达标下限'))
    return [_display(record.get('产品型号')), _stack(record.get('分类'), record.get('部门')),
            _stack(record.get('发生站点'), record.get('层别')), _display(record.get('监控参数')),
            _display(record.get('异常事件')), _stack(metric, f'下限 {lower}' if lower else ''),
            ids, _stack(date, record.get('统计周期'))]


def render_message(plan: NotificationPlan, sender: str) -> EmailMessage:
    state = '预警周报' if plan.complete else '预警周报（部分来源不可用）'
    subject = f'北极星{state} {plan.period_start} 至 {plan.period_end}'
    text = f'统计日期：{plan.period_start} 至 {plan.period_end}\n'
    text += f'本邮件包含 {len(plan.records)} 条预警；完整明细见 CSV 附件。\n'
    if not plan.complete:
        text += '部分来源不可用，当前数据不能作为完整统计；缺失来源不代表没有异常。\n'
    sections = ['<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
                '<meta name="viewport" content="width=device-width,initial-scale=1"></head>'
                '<body style="margin:0;padding:0;background-color:#ffffff;color:#243447;'
                'font-family:Arial,\'Microsoft YaHei\',sans-serif;font-size:14px;line-height:1.7;">'
                '<div style="max-width:1120px;margin:0 auto;padding:24px 16px;">',
                '<h2 style="margin:0 0 14px;font-size:22px;font-weight:600;line-height:1.4;">北极星' + escape(state) + '</h2>',
                '<p style="margin:0 0 20px;">' + escape(text).replace('\n', '<br>') + '</p>']
    counts = Counter((r.get('分类', ''), r.get('部门', ''), r.get('异常事件', '')) for r in plan.records)
    if counts:
        sections.append('<h3 style="margin:24px 0 8px;font-size:16px;font-weight:600;">预警汇总</h3>')
        sections.append(_html_table(('分类', '厂别', '事件', '条数'),
                                    [[_display(v) for v in (*key, count)] for key, count in sorted(counts.items())]))
    if plan.failed_sources:
        text += '\n不可用来源（该分类未标明厂别时通知各厂责任人）：\n'
        sections.append('<h3 style="margin:24px 0 8px;font-size:16px;font-weight:600;">不可用来源</h3><ul>')
        for failure in plan.failed_sources:
            label = ' / '.join(str(failure.get(k, '全部厂别' if k == 'factory' else ''))
                               for k in ('product', 'sub_domain', 'event_type', 'factory'))
            text += label + '\n'
            sections.append('<li>' + escape(label) + '</li>')
        sections.append('</ul>')
    for limitation in plan.limitations:
        text += limitation + '\n'
        sections.append('<p>' + escape(limitation) + '</p>')
    if plan.records:
        sections.append('<h3 style="margin:24px 0 8px;font-size:16px;font-weight:600;">预警明细</h3>')
        if len(plan.records) > 20:
            sections.append('<p style="margin:0 0 8px;color:#526477;">正文展示前20条，全部明细见附件。</p>')
        sections.append(_html_table(('产品型号', '分类 / 厂别', '站点 / 层别', '监控参数', '事件',
                                     '指标 / 下限', '对象编号', '日期 / 周期'),
                                    [_preview_row(record) for record in plan.records[:20]],
                                    widths=(8, 11, 9, 16, 6, 10, 25, 15)))
    sections.append('</div></body></html>')
    message = EmailMessage()
    message['From'], message['To'], message['Subject'] = sender, plan.recipient, subject
    message.set_content(text)
    message.add_alternative(''.join(sections), subtype='html')
    if plan.records:
        stream = StringIO(newline='')
        writer = csv.writer(stream)
        writer.writerow(PUBLIC_FIELDS)
        writer.writerows([_cell(row.get(k)) for k in PUBLIC_FIELDS] for row in plan.records)
        message.add_attachment(stream.getvalue().encode('utf-8-sig'), maintype='text', subtype='csv',
                               filename=f'预警明细-{plan.period_start}-{plan.period_end}.csv')
    return message


class SmtpDelivery:
    def __init__(self, settings: NotificationSettings, credentials: MailCredentials, *, smtp_factory: Callable | None = None):
        self.settings, self.credentials = settings, credentials
        self.factory = smtp_factory

    def send(self, plan: NotificationPlan) -> None:
        conf, credentials = self.settings, self.credentials
        message = render_message(plan, credentials.sender)
        factory = self.factory or (smtplib.SMTP_SSL if conf.smtp_security == 'ssl' else smtplib.SMTP)
        for index, password in enumerate(credentials.passwords):
            try:
                kwargs = {'timeout': conf.smtp_timeout_seconds}
                if conf.smtp_security == 'ssl':
                    kwargs['context'] = ssl.create_default_context()
                with factory(conf.smtp_host, conf.smtp_port, **kwargs) as smtp:
                    smtp.ehlo()
                    if conf.smtp_security == 'starttls':
                        smtp.starttls(context=ssl.create_default_context())
                        smtp.ehlo()
                    smtp.login(credentials.user, password)
                    rejected = smtp.send_message(message, from_addr=credentials.sender, to_addrs=[plan.recipient])
                    if rejected:
                        raise NotificationError('SMTP 未接受全部收件人；本次不自动重发。')
                return
            except smtplib.SMTPAuthenticationError:
                if index + 1 < len(credentials.passwords):
                    continue
                raise NotificationError('SMTP 所有候选凭证均认证失败。') from None
            except (smtplib.SMTPException, OSError, ValueError):
                raise NotificationError('SMTP 投递未确认成功；请核对已投递邮件，本次不自动重发。') from None
