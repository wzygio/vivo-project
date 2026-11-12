from copy import deepcopy
from pathlib import Path

import pytest

from api.notification.core import NotificationError, plan_notifications
from api.notification.infrastructure import render_message
from api.notification.settings import load_settings


ROOT = Path(__file__).resolve().parents[3]
TEST_INBOX = 'wangzy03@visionox.com'


def result(rows=(), failures=()):
    return {'data': list(rows), 'meta': {
        'complete': not failures, 'failed_sources': list(failures),
        'returned_count': len(rows), 'has_more': False, 'next_cursor': None,
        'status': 'alert' if rows else 'ok',
        'period_start': '2026-11-02T00:00:00+08:00',
        'period_end_exclusive': '2026-11-09T00:00:00+08:00',
        'timezone': 'Asia/Shanghai', 'limitations': [],
    }}


def row(domain='SPC', factory='ARRAY', **fields):
    return {'分类': domain, '部门': factory, '发生站点': '00100',
            '层别': 'PI2', '监控参数': '=formula', '异常事件': 'CPK',
            '对象类型': 'capability', '对象编号': ['00001', '00002'],
            '产品型号': 'M626', '发生时间': '2026-11-08', '命中边界': 'spec',
            **fields}


@pytest.fixture
def settings():
    return load_settings(ROOT / 'config/domain/api_domain.yaml')


@pytest.mark.parametrize('domain,factory,email', [
    (d, f, email) for domains, email in [
        (('SPC', 'CTQ'), 'jieqiang@visionox.com'),
        (('Yield',), TEST_INBOX),
    ] for d in domains for f in ('ARRAY', 'OLED', 'TP')
] + [(d, f, email) for d in ('AOI_TT', 'AOI_RS', 'Q-Time')
     for f, email in [('ARRAY', 'sunyang01@visionox.com'),
                      ('OLED', 'liuzy08@visionox.com'), ('TP', 'panxy01@visionox.com')]])
def test_all_default_routes(settings, domain, factory, email):
    plans = plan_notifications(result([row(domain, factory)]), settings)
    assert len(plans) == 1
    assert plans[0].recipient == email
    assert plans[0].records[0]['部门'] == factory


def test_test_mode_merges_all_and_preserves_records(settings):
    source = result([row(d, f) for d in ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')
                     for f in ('ARRAY', 'OLED', 'TP')])
    before = deepcopy(source)
    plans = plan_notifications(source, settings, test_mode=True)
    assert len(plans) == 1 and plans[0].recipient == TEST_INBOX
    assert len(plans[0].records) == 18
    assert source == before


def test_complete_empty_sends_nothing(settings):
    assert plan_notifications(result(), settings) == ()


def test_missing_source_warns_owners_even_without_records(settings):
    failure = {'product': 'M626', 'sub_domain': 'AOI_TT', 'event_type': 'OOS', 'code': 'result_unavailable'}
    plans = plan_notifications(result(failures=[failure]), settings)
    assert {p.recipient for p in plans} == {'sunyang01@visionox.com', 'liuzy08@visionox.com', 'panxy01@visionox.com'}
    assert all(not p.complete and p.failed_sources for p in plans)


@pytest.mark.parametrize('bad', [row('UNKNOWN'), row(factory=None), row(factory='OTHER')])
def test_unroutable_data_fails_before_delivery(settings, bad):
    with pytest.raises(NotificationError):
        plan_notifications(result([row(), bad]), settings)


def test_message_public_fields_full_attachment_and_escaping(settings):
    source = result([row(**{'监控参数': '<script>=danger</script>', 'flag': 'private'})] * 31)
    plan, = plan_notifications(source, settings, test_mode=True)
    message = render_message(plan, 'sender@example.com')
    assert message['To'] == TEST_INBOX and message['Cc'] is None and message['Bcc'] is None
    html = message.get_body(preferencelist=('html',)).get_content()
    assert '<script>' not in html and '&lt;script&gt;' in html
    attachment, = message.iter_attachments()
    csv = attachment.get_payload(decode=True).decode('utf-8-sig')
    assert csv.count('["00001","00002"]'.replace('"', '""')) == 31
    assert 'private' not in csv and 'flag' not in csv


def test_formula_cells_protected(settings):
    plan, = plan_notifications(result([row()]), settings)
    msg = render_message(plan, 'sender@example.com')
    attachment, = msg.iter_attachments()
    assert "'=formula" in attachment.get_payload(decode=True).decode('utf-8-sig')


@pytest.mark.parametrize('test_mode', [False, True])
def test_email_copy_has_no_test_or_timezone_labels(settings, test_mode):
    plan, = plan_notifications(result([row()]), settings, test_mode=test_mode)
    message = render_message(plan, 'sender@example.com')
    visible = str(message['Subject']) + message.get_body(preferencelist=('plain',)).get_content()
    visible += message.get_body(preferencelist=('html',)).get_content()
    assert all(word not in visible for word in ('测试', '上海时区', '预览', 'test-mode'))


def test_readable_preview_keeps_all_ids_and_precision_in_attachment(settings):
    ids = ['000000000000000000000000000000000000' + str(i) for i in range(8)]
    plan, = plan_notifications(result([row(**{'对象编号': ids, '指标值': 1.304169492240151,
                                            '达标下限': 1.33})]), settings, test_mode=True)
    message = render_message(plan, 'sender@example.com')
    html = message.get_body(preferencelist=('html',)).get_content()
    assert '1.3042' in html and '共 8 片' in html and '完整名单见附件' in html
    assert ids[0] in html and ids[-1] not in html
    attachment, = message.iter_attachments()
    csv = attachment.get_payload(decode=True).decode('utf-8-sig')
    assert all(sheet in csv for sheet in ids) and '1.304169492240151' in csv
