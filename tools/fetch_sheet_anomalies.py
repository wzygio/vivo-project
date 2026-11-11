"""Portable API caller: stdlib only, no arguments opens a masked-key GUI.

The program only calls HTTP GET and exports returned public records. It does
not access the report workbooks, database, service profile or API key files.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import getpass
from io import StringIO
import json
import os
from pathlib import Path
import queue
import re
import sys
import threading
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

ENDPOINT = '/api/v1/sheet-anomalies'
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 120
DOMAINS = ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')
FILTERS = ('product', 'sub_domain', 'event_type', 'factory', 'start_date', 'end_date',
           'step_id', 'layer', 'parameter_name', 'entity_type', 'entity_id')
PUBLIC_FIELDS = ('分类', '发生站点', '部门', '层别', '监控参数', '异常事件',
                 '对象类型', '对象编号', '产品型号', '发生时间', '命中边界',
                 '指标值', '达标下限', '统计周期类型', '统计周期', '统计开始日期', '统计截止日期')
ERROR_HELP = {
    401: '密钥缺失或无效，请检查原始 API Key。',
    403: '查询范围未获授权，或产品未启用。',
    410: '分页凭条已失效，请从第一页重新查询；本次未保留未完成分页。',
    422: '请检查筛选条件和分页凭条；若服务已重启，请重新查询。',
    429: '请求过于频繁或读取忙，请等待后重试。',
    503: '所选来源或服务暂时不可用，请联系维护者核对发布结果。',
    500: '服务出现错误，请将请求编号交给维护者。',
}


class SafeArgumentParser(argparse.ArgumentParser):
    """Argument mistakes must not echo a credential pasted into the command."""

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(2, '命令参数无效，请用 --help 查看用法。API Key 只能通过隐藏输入或 API_KEY／VIVO_API_KEY 提供，不能放在命令行中。\n')


class ApiCallError(Exception):
    """Safe caller error: never copies raw request headers or server messages."""

    def __init__(self, message: str, *, status: int | None = None, request_id: str = ''):
        self.status = status
        self.request_id = request_id
        prefix = f'HTTP {status}：' if status is not None else ''
        suffix = f' 请求编号：{request_id}' if request_id else ''
        super().__init__(prefix + message + suffix)


class NoCredentialRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib otherwise forwards custom X-API-Key headers across redirects.
        return None


def direct_opener():
    """Use approved direct intranet access, ordinary TLS validation, no redirects."""
    return build_opener(ProxyHandler({}), NoCredentialRedirect())


def validate_base_url(value: str) -> str:
    try:
        parsed = urlsplit(value.strip())
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or any(c.isspace() for c in parsed.netloc)
                or any(c in value for c in '\r\n\t')):
            raise ValueError
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise ApiCallError('地址须为 HTTP／HTTPS 服务地址，不能包含凭证、查询条件或片段。') from None
    return value.strip().rstrip('/')


def _request_id(headers) -> str:
    value = str(headers.get('X-Request-ID', '')) if headers else ''
    return value if re.fullmatch(r'[a-fA-F0-9]{32}', value) else ''


def _read_page(opener, base: str, api_key: str, params: dict, timeout: float) -> tuple[dict, str]:
    url = base + ENDPOINT + ('?' + urlencode(params) if params else '')
    request = Request(url, headers={'X-API-Key': api_key, 'Accept': 'application/json'}, method='GET')
    try:
        with opener.open(request, timeout=timeout) as response:
            request_id = _request_id(response.headers)
            if response.status != 200:
                raise ApiCallError('服务返回了非预期状态。', status=response.status, request_id=request_id)
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        request_id = _request_id(exc.headers)
        status = exc.code
        # Inspect only a small structured error code. Never display the server's
        # free-text message, which may contain private paths or other details.
        code = ''
        try:
            if status == 503:
                error_body = exc.read(4097)
                if len(error_body) <= 4096:
                    error_payload = json.loads(error_body)
                    error = error_payload.get('error') if isinstance(error_payload, dict) else None
                    if isinstance(error, dict):
                        code = error.get('code')
        except (OSError, ValueError, UnicodeError):
            pass  # Malformed error bodies retain the fixed status-based help.
        finally:
            exc.close()
        help_text = ERROR_HELP.get(status, '服务返回了非预期状态；重定向不会自动跟随，请确认正式入口。')
        if status == 503 and code == 'read_timeout':
            help_text = '已连接到 API，但结果读取超时。可缩小产品或分类范围后重试；持续失败请将请求编号交给维护者。'
        raise ApiCallError(help_text, status=status, request_id=request_id) from None
    except (URLError, OSError, TimeoutError, ValueError, UnicodeError):
        raise ApiCallError('无法连接或请求超时，请核对地址、网络与服务状态。') from None
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ApiCallError('响应超过客户端读取上限，请联系维护者。')
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeError):
        raise ApiCallError('服务返回的内容不是有效 JSON。', request_id=request_id) from None
    return payload, request_id


def _validate_page(payload: Any) -> tuple[list[dict], dict]:
    if not isinstance(payload, dict) or not isinstance(payload.get('data'), list) or not isinstance(payload.get('meta'), dict):
        raise ApiCallError('响应缺少有效的 data／meta，未将结果当作成功。')
    rows, meta = payload['data'], payload['meta']
    if (any(not isinstance(row, dict) for row in rows)
            or type(meta.get('returned_count')) is not int or meta['returned_count'] != len(rows)
            or type(meta.get('complete')) is not bool or type(meta.get('has_more')) is not bool
            or meta.get('status') not in ('ok', 'alert')
            or not isinstance(meta.get('failed_sources'), list)
            or any(not isinstance(failed, dict) for failed in meta['failed_sources'])
            or meta['complete'] != (not meta['failed_sources'])
            or any(not isinstance(meta.get(key), str) for key in ('period_start', 'period_end_exclusive', 'timezone'))):
        raise ApiCallError('响应元数据不完整或与明细矛盾，未保留未完成结果。')
    cursor = meta.get('next_cursor')
    if (meta['has_more'] and (not isinstance(cursor, str) or not cursor)) or (not meta['has_more'] and cursor is not None):
        raise ApiCallError('响应的分页状态不一致，请重新查询。')
    return rows, meta


def fetch_anomalies(base_url: str, api_key: str, params: dict[str, str] | None = None, *,
                    page_size: int | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS,
                    opener=None, max_pages: int = 10000) -> dict:
    """Fetch a whole query or all explicit pages; errors never return a prefix.

    Tests/embedders can inject an opener exposing open(Request, timeout=...).
    The function does not save anything, prompt, or print credentials/results.
    Capability rows retain every Sheet ID in their entity-ID arrays. Page sizes
    count result rows, not individual IDs; entity_id filters one Sheet ID while
    the server still returns the complete matching capability row.
    """
    base = validate_base_url(base_url)
    if (not isinstance(api_key, str) or not api_key or len(api_key) > 1024
            or any(not 33 <= ord(char) <= 126 for char in api_key)):
        raise ApiCallError('请输入有效原始 API Key。')
    if page_size is not None and (type(page_size) is not int or not 1 <= page_size <= 500):
        raise ApiCallError('每页行数应为 1～500；留空表示不分页。')
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 1 <= timeout <= 120:
        raise ApiCallError('请求超时须为 1～120 秒。')
    if type(max_pages) is not int or max_pages < 1:
        raise ApiCallError('分页读取次数上限无效。')
    query = dict(params or {})
    if any(name not in FILTERS or not isinstance(value, str) or not value for name, value in query.items()):
        raise ApiCallError('筛选字段无效；全部范围请省略相应字段。')
    if page_size is not None:
        query['limit'] = page_size
    opener = opener if opener is not None else direct_opener()
    records, ids, seen = [], [], set()
    first_meta, context = None, None
    for page in range(1, max_pages + 1):
        payload, request_id = _read_page(opener, base, api_key, query, timeout)
        rows, meta = _validate_page(payload)
        identity = {k: meta[k] for k in ('period_start', 'period_end_exclusive', 'timezone', 'status', 'complete', 'failed_sources')}
        if context is not None and identity != context:
            raise ApiCallError('分页查询范围或来源状态已变化，请从第一页重新查询。')
        if first_meta is None:
            first_meta, context = dict(meta), identity
        records.extend(rows)
        if request_id:
            ids.append(request_id)
        if not meta['has_more']:
            first_meta.update(returned_count=len(records), has_more=False, next_cursor=None)
            return {'data': records, 'meta': first_meta,
                    'client': {'pages_fetched': page, 'pagination_enabled': page_size is not None, 'request_ids': ids}}
        if page_size is None:
            raise ApiCallError('未请求分页却收到后续页，未将部分结果当作完整结果。')
        cursor = meta['next_cursor']
        if cursor in seen:
            raise ApiCallError('服务重复返回相同分页凭条，请重新查询。')
        seen.add(cursor)
        query['cursor'] = cursor
    raise ApiCallError('超过分页读取次数上限，未保留未完成结果。')


def result_summary(result: dict) -> str:
    count = len(result['data'])
    if not result['meta']['complete']:
        return f'调用成功，但部分来源不可用：收到 {count} 条明细，不能作为完整统计。'
    if not count:
        return '调用成功：当前筛选范围没有可提供的异常明细。请核对产品、分类与日期。'
    return f'调用成功：已获取 {count} 条明细，所选来源读取完整。'


def _completed_export(result: dict) -> tuple[list[dict], dict]:
    rows, meta = _validate_page(result)
    if meta['has_more']:
        raise ApiCallError('分页尚未完成，不能下载未完成的明细；请重新查询。')
    return rows, meta


def _write_new_text(path: Path, text: str, *, encoding: str = 'utf-8') -> Path:
    path = Path(path)
    created = False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding=encoding, newline='') as stream:
            created = True
            stream.write(text)
    except FileExistsError:
        raise ApiCallError('目标文件已存在，请选择新的文件名。') from None
    except OSError:
        if created:
            path.unlink(missing_ok=True)
        raise ApiCallError('无法保存结果，请检查文件位置与写入权限。') from None
    return path


def save_result(result: dict, path: Path) -> Path:
    """Save the whole completed JSON envelope, including source-completeness flags."""
    _completed_export(result)
    try:
        text = json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2)
    except (TypeError, ValueError):
        raise ApiCallError('查询结果不能序列化为有效 JSON，未保存文件。') from None
    return _write_new_text(Path(path), text + '\n')


def _csv_value(value: Any) -> str:
    """Keep complete JSON arrays and neutralize scalar spreadsheet formulas."""
    if isinstance(value, list):
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        except (TypeError, ValueError):
            raise ApiCallError('查询结果包含无法转换为 JSON 的列表，未保存文件。') from None
    text = '' if value is None else str(value)
    candidate = re.sub(r'^[\s\ufeff]+', '', text)
    if text.startswith(('\t', '\r', '\n')) or candidate.startswith(('=', '+', '-', '@')):
        return "'" + text
    return text


def save_records_csv(result: dict, path: Path) -> Path:
    """Download every fetched row with a fixed public schema, plus a metadata receipt.

    No 20-row preview or 500-row page limit is applied here. API-side capacity
    remains applicable. CSV stores identifiers as text; Excel import should set
    identifier columns to Text to avoid Excel's own automatic number conversion.
    Capability Sheet IDs occupy one JSON-array cell per result row; parsing that
    cell with json.loads restores every original string, including leading zeros.
    The companion <name>.csv.meta.json carries complete/failed_sources and count.
    """
    rows, meta = _completed_export(result)
    path = Path(path)
    receipt_path = path.with_name(path.name + '.meta.json')
    if path.exists() or receipt_path.exists():
        raise ApiCallError('结果文件或说明文件已存在，请选择新的文件名。')
    buffer = StringIO(newline='')
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL, lineterminator='\r\n')
    writer.writerow(PUBLIC_FIELDS)
    writer.writerows([_csv_value(row.get(field)) for field in PUBLIC_FIELDS] for row in rows)
    receipt = {'meta': meta, 'client': result.get('client', {}),
               'export': {'format': 'csv', 'data_file': path.name,
                          'row_count': len(rows), 'complete': meta['complete']}}
    try:
        receipt_text = json.dumps(receipt, ensure_ascii=False, allow_nan=False, indent=2) + '\n'
    except (TypeError, ValueError):
        raise ApiCallError('查询说明不能序列化为有效 JSON，未保存文件。') from None
    created = []
    try:
        created.append(_write_new_text(path, buffer.getvalue(), encoding='utf-8-sig'))
        created.append(_write_new_text(receipt_path, receipt_text))
    except ApiCallError:
        for own_file in created:
            own_file.unlink(missing_ok=True)
        raise
    return path


def default_output() -> Path:
    return Path(__file__).resolve().parents[1] / 'output/reports/anomaly-api' / f'anomalies-{datetime.now():%Y%m%d-%H%M%S-%f}.json'


def configured_api_key() -> str:
    """Use the caller process environment only; never persist or load key files."""
    return os.environ.get('API_KEY') or os.environ.get('VIVO_API_KEY') or ''


def launch_gui(defaults: argparse.Namespace | None = None) -> int:
    """Human-operated local/remote caller, no credentials are written to disk."""
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title('异常明细查询')
    root.geometry('960x740')
    frame = ttk.Frame(root, padding=16)
    frame.pack(fill='both', expand=True)
    fields = {}
    values = [('base_url', 'API 地址', getattr(defaults, 'base_url', 'http://127.0.0.1:8890')),
              ('api_key', 'API Key（原始密钥）', configured_api_key()),
              ('product', '产品（留空为全部授权产品）', getattr(defaults, 'product', '') or ''),
              ('sub_domain', '分类', getattr(defaults, 'sub_domain', '') or '全部'),
              ('event_type', '异常事件', getattr(defaults, 'event_type', None) or '全部'),
              ('start_date', '开始日期 YYYY-MM-DD（可空）', getattr(defaults, 'start_date', '') or ''),
              ('end_date', '截止日期 YYYY-MM-DD（可空）', getattr(defaults, 'end_date', '') or ''),
              ('page_size', '每页行数（留空为不分页）', str(getattr(defaults, 'page_size', '') or ''))]
    for row, (name, label, initial) in enumerate(values):
        ttk.Label(frame, text=label).grid(row=row, column=0, sticky='w', pady=4)
        var = tk.StringVar(value=initial)
        fields[name] = var
        if name in ('sub_domain', 'event_type'):
            widget = ttk.Combobox(frame, textvariable=var, state='readonly',
                                  values=('全部', *DOMAINS) if name == 'sub_domain' else ('全部', 'OOS', 'OOC', 'CPK', 'CPM'))
        else:
            widget = ttk.Entry(frame, textvariable=var, show='*' if name == 'api_key' else '')
        widget.grid(row=row, column=1, sticky='ew', pady=4)
    ttk.Label(frame, text='本机地址用 127.0.0.1；其它设备填维护者提供的内网地址。日期留空为上周。'
              'CPK/CPM是SPC周统计项，请选择SPC或全部；仅返回日期内完整ISO周，发生时间为统计截止日期。'
              '其对象编号为完整Sheet ID列表，分页按统计项行数计算；CSV列表单元格为JSON文本。'
              '预览仅20行，下载包含本次收到的全部明细。',
              wraplength=880).grid(row=8, column=0, columnspan=2, sticky='w', pady=8)
    status = tk.StringVar(value='可从 API_KEY／VIVO_API_KEY 环境变量自动填入密钥，或手动输入；密钥不保存到文件。')
    ttk.Label(frame, textvariable=status, wraplength=880).grid(row=10, column=0, columnspan=2, sticky='w', pady=8)
    view = tk.Text(frame, wrap='none', height=18, state='disabled')
    view.grid(row=11, column=0, columnspan=2, sticky='nsew')
    scroll = ttk.Scrollbar(frame, orient='vertical', command=view.yview)
    scroll.grid(row=11, column=2, sticky='ns')
    view.configure(yscrollcommand=scroll.set)
    frame.columnconfigure(1, weight=1)
    frame.rowconfigure(11, weight=1)
    messages = queue.Queue()
    state = {'result': None, 'busy': False}

    def show_text(text):
        view.configure(state='normal')
        view.delete('1.0', 'end')
        view.insert('1.0', text)
        view.configure(state='disabled')

    def run_query():
        if state['busy']:
            return
        state['result'] = None
        csv_button.configure(state='disabled', text='下载全部明细(CSV)')
        save_button.configure(state='disabled')
        show_text('')
        params = {name: fields[name].get().strip() for name in ('product', 'sub_domain', 'event_type', 'start_date', 'end_date')
                  if fields[name].get().strip() and fields[name].get() != '全部'}
        try:
            size = int(fields['page_size'].get()) if fields['page_size'].get().strip() else None
        except ValueError:
            status.set('查询条件无效，本次没有可下载的结果。')
            messagebox.showerror('查询条件', '每页行数须为整数；留空表示不分页。', parent=root)
            return
        base, key = fields['base_url'].get().strip(), fields['api_key'].get() or configured_api_key()
        fields['api_key'].set('')
        state.update(result=None, busy=True)
        query_button.configure(state='disabled')
        status.set('正在读取，请稍候……')

        def work():
            try:
                messages.put(('ok', fetch_anomalies(base, key, params, page_size=size,
                                                    timeout=getattr(defaults, 'timeout', DEFAULT_TIMEOUT_SECONDS))))
            except ApiCallError as exc:
                messages.put(('error', str(exc)))
            except Exception:
                messages.put(('error', '客户端发生未预期错误，未保存结果；请联系维护者。'))
        threading.Thread(target=work, daemon=True, name='anomaly-api-query').start()

    def poll():
        try:
            kind, value = messages.get_nowait()
        except queue.Empty:
            root.after(100, poll)
            return
        state['busy'] = False
        query_button.configure(state='normal')
        if kind == 'error':
            status.set(value)
        else:
            state['result'] = value
            meta = value['meta']
            status.set(result_summary(value) + f" 查询时段：{meta['period_start']} 至 {meta['period_end_exclusive']}（不含结束时刻）。")
            show_text(json.dumps({'meta': meta, 'client': value['client'], '明细预览（最多20行）': value['data'][:20]},
                                 ensure_ascii=False, indent=2))
            csv_button.configure(state='normal', text='下载全部明细(CSV)' if meta['complete'] else '下载可用部分(CSV)')
            save_button.configure(state='normal')
        root.after(100, poll)

    def export(format='json'):
        if state['result'] is None:
            return
        suggested = default_output()
        if format == 'csv':
            suggested = suggested.with_suffix('.csv')
        if not state['result']['meta']['complete']:
            suggested = suggested.with_name(suggested.stem + '-partial' + suggested.suffix)
        try:
            suggested.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            messagebox.showerror('保存失败', '无法创建默认输出目录，请检查目录权限。', parent=root)
            return
        selected = filedialog.asksaveasfilename(parent=root, title='下载全部已取得明细' if format == 'csv' else '保存完整响应',
                    initialdir=str(suggested.parent), initialfile=suggested.name, defaultextension='.' + format,
                    filetypes=[('CSV 明细', '*.csv')] if format == 'csv' else [('JSON 数据', '*.json')])
        if not selected:
            return
        try:
            (save_records_csv if format == 'csv' else save_result)(state['result'], Path(selected))
            status.set(result_summary(state['result']) + (' CSV和完整性说明文件已保存。' if format == 'csv' else ' 完整响应已保存。'))
        except ApiCallError as exc:
            messagebox.showerror('保存失败', str(exc), parent=root)

    buttons = ttk.Frame(frame)
    buttons.grid(row=9, column=0, columnspan=2, sticky='w')
    query_button = ttk.Button(buttons, text='查询完整结果', command=run_query)
    query_button.pack(side='left')
    csv_button = ttk.Button(buttons, text='下载全部明细(CSV)', command=lambda: export('csv'), state='disabled')
    csv_button.pack(side='left', padx=12)
    save_button = ttk.Button(buttons, text='保存完整响应(JSON)', command=export, state='disabled')
    save_button.pack(side='left', padx=12)
    root.after(100, poll)
    root.mainloop()
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = SafeArgumentParser(description='异常明细 API 调用程序；无参数打开图形窗口。')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--cli', action='store_true', help='使用命令行；密钥从环境变量或隐藏输入取得')
    mode.add_argument('--gui', action='store_true', help='打开图形窗口')
    parser.add_argument('--base-url', default='http://127.0.0.1:8890')
    for field in FILTERS:
        help_text = {'event_type': '不填为全部适用事件；可选OOS、OOC、CPK或CPM；CPK/CPM仅支持SPC周统计项',
                     'entity_type': 'sheet、lot或capability（能力统计项）',
                     'entity_id': '单片或Lot编号；CPK/CPM按Sheet ID匹配，返回完整统计项及全部Sheet ID'}.get(field)
        parser.add_argument('--' + field.replace('_', '-'), dest=field, help=help_text)
    parser.add_argument('--page-size', type=int, help='可选1～500；不填则不分页')
    parser.add_argument('--timeout', type=float, default=DEFAULT_TIMEOUT_SECONDS,
                        help='每次 HTTP 请求的等待秒数，默认120，允许1～120')
    parser.add_argument('--output', type=Path, help='结果文件保存位置；不覆盖已有文件')
    parser.add_argument('--format', choices=('json', 'csv'), default='json', help='CLI下载格式；CSV同时保存完整性说明文件')
    args = parser.parse_args(argv)
    if not argv or args.gui:
        advanced = ('factory', 'step_id', 'layer', 'parameter_name', 'entity_type', 'entity_id', 'output')
        if any(getattr(args, field) is not None for field in advanced) or args.format != 'json':
            parser.exit(2, '图形窗口暂不支持指定的高级筛选、--output 或 --format，请改用 --cli；本次未发送查询。\n')
        try:
            return launch_gui(args)
        except Exception:
            print('无法打开图形窗口，可使用 --cli 运行；不会显示或保存密钥。', file=sys.stderr)
            return 2
    try:
        key = configured_api_key() or getpass.getpass('请输入原始 API Key（输入隐藏）：')
        params = {name: value for name in FILTERS if (value := getattr(args, name)) is not None}
        result = fetch_anomalies(args.base_url, key, params, page_size=args.page_size, timeout=args.timeout)
        del key
        suggested = default_output().with_suffix('.' + args.format)
        if not result['meta']['complete']:
            suggested = suggested.with_name(suggested.stem + '-partial' + suggested.suffix)
        output = (save_records_csv if args.format == 'csv' else save_result)(result, args.output or suggested)
        print(result_summary(result))
        print(f'已保存：{output}')
        print(json.dumps({'meta': result['meta'], 'client': result.get('client', {})}, ensure_ascii=False))
        return 0 if result['meta']['complete'] else 3
    except (ApiCallError, EOFError, KeyboardInterrupt) as exc:
        text = str(exc) if isinstance(exc, ApiCallError) else '输入已取消，未完成查询。'
        print(text, file=sys.stderr)
        return 2
    except Exception:
        print('客户端发生未预期错误，未完成查询；请联系维护者。', file=sys.stderr)
        return 2


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    raise SystemExit(main())
