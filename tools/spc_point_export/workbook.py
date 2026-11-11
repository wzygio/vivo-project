"""Bounded-memory XLSX generation and verification before enterprise encryption."""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import io
import math
from pathlib import Path
import re
import tempfile
from xml.etree import ElementTree as ET
import zipfile

import pandas as pd
from openpyxl.utils.datetime import to_excel
import xlsxwriter

from . import ExportError

MAX_DATA_ROWS = 1_048_575
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


def _values(row: tuple) -> list:
    return [None if pd.isna(value) else value for value in row]


def _xml_values(element: ET.Element, width: int) -> list:
    values = [None] * width
    for cell in element.findall('s:c', NS):
        offset = 0
        for letter in re.sub(r'\d', '', cell.attrib['r']):
            offset = offset * 26 + ord(letter) - 64
        if offset > width:
            raise ExportError('XLSX contains unexpected extra columns')
        value = cell.find('s:v', NS)
        if cell.attrib.get('t') == 'inlineStr':
            values[offset - 1] = ''.join(cell.itertext())
        elif value is not None:
            values[offset - 1] = value.text == '1' if cell.attrib.get('t') == 'b' else float(value.text)
    return values


def _compare(actual: list, expected: tuple) -> None:
    for value, original in zip(actual, _values(expected)):
        if original is None or original == '':
            equal = value in (None, '')
        elif isinstance(original, (datetime, date, pd.Timestamp)):
            equal = value is not None and math.isclose(value, to_excel(original), abs_tol=1e-9, rel_tol=0)
        elif isinstance(original, (float, int)):
            equal = value is not None and math.isclose(value, original, rel_tol=1e-14, abs_tol=1e-12)
        else:
            equal = value == original
        if not equal:
            raise ExportError('XLSX sample values do not match the point data')


def verify_workbook(payload: bytes, frame: pd.DataFrame, sheets: list[str]) -> None:
    """Check CRCs, every row number, headers and first/last values of each data sheet."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        if archive.testzip() is not None:
            raise ExportError('XLSX archive checksum failed')
        for index, _ in enumerate(sheets):
            start = index * MAX_DATA_ROWS
            count = min(MAX_DATA_ROWS, max(0, len(frame) - start))
            observed = 0
            with archive.open(f'xl/worksheets/sheet{index + 1}.xml') as stream:
                for _, element in ET.iterparse(stream, events=('end',)):
                    if element.tag != '{' + NS['s'] + '}row':
                        continue
                    observed += 1
                    if int(element.attrib['r']) != observed:
                        raise ExportError('XLSX row sequence is incomplete')
                    if observed == 1:
                        if _xml_values(element, len(frame.columns)) != list(frame.columns):
                            raise ExportError('XLSX headers differ from the point fields')
                    elif observed in {2, count + 1}:
                        _compare(_xml_values(element, len(frame.columns)),
                                 tuple(frame.iloc[start + observed - 2]))
                    element.clear()
            if observed != count + 1:
                raise ExportError('XLSX row count differs from the point data')


def build_workbook(frame: pd.DataFrame, product: str, metadata: dict,
                   temp_root: Path) -> tuple[bytes, list[str]]:
    """Keep one product in one file; large populations use multiple internal sheets."""
    if frame.columns.duplicated().any() or not len(frame.columns):
        raise ExportError('Point data must have unique, nonempty field names')
    if len(frame.columns) > 16_384:
        raise ExportError('Point field count exceeds the Excel column limit')
    sheets = []
    with tempfile.TemporaryDirectory(prefix='spc-xlsx-', dir=temp_root) as working:
        with io.BytesIO() as buffer:
            with xlsxwriter.Workbook(buffer, {
                'constant_memory': True, 'tmpdir': working,
                'strings_to_formulas': False, 'strings_to_urls': False,
                'default_date_format': 'yyyy-mm-dd hh:mm:ss',
            }) as book:
                header = book.add_format({'bold': True, 'bg_color': '#DCE6F1'})
                for part, start in enumerate(range(0, max(1, len(frame)), MAX_DATA_ROWS), 1):
                    name = product[:24] if len(frame) <= MAX_DATA_ROWS else f'{product[:24]}_{part}'
                    sheets.append(name)
                    sheet = book.add_worksheet(name)
                    sheet.write_row(0, 0, list(frame.columns), header)
                    sheet.freeze_panes(1, 0)
                    sheet.set_column(0, len(frame.columns) - 1, 20)
                    section = frame.iloc[start:start + MAX_DATA_ROWS]
                    if len(section):
                        sheet.autofilter(0, 0, len(section), len(frame.columns) - 1)
                    for row_number, row in enumerate(section.itertuples(index=False, name=None), 1):
                        sheet.write_row(row_number, 0, _values(row))
                info = book.add_worksheet('导出说明')
                info.write_row(0, 0, ['项目', '内容'], header)
                notes = {'产品': product, '点位数量': len(frame), '字段数': len(frame.columns),
                         '开始日期': metadata.get('start_date', ''),
                         '结束日期': metadata.get('end_date', ''),
                         '当日报表截止': metadata.get('cutoff', ''),
                         '数据口径': 'SPC 应用层修饰后、供绘图使用的完整点位明细'}
                for number, row in enumerate(notes.items(), 1):
                    info.write_row(number, 0, row)
                info.set_column(0, 0, 22)
                info.set_column(1, 1, 80)
            payload = buffer.getvalue()
    verify_workbook(payload, frame, sheets)
    return payload, sheets


def write_product(frame: pd.DataFrame, product: str, directory: Path,
                  metadata: dict, temp_root: Path) -> dict:
    filename = f"SPC_{product}_{metadata['start_date']}_{metadata['end_date']}.xlsx"
    target = directory / filename
    if target.exists():
        raise ExportError(f'Output already exists: {filename}')
    print(f'XLSX {product}: writing and verifying {len(frame):,} rows', flush=True)
    payload, sheets = build_workbook(frame, product, metadata, temp_root)
    with target.open('xb') as stream:
        try:
            stream.write(payload)
        except BaseException:
            stream.close()
            target.unlink(missing_ok=True)  # Only the file created by this call.
            raise
    times = pd.to_datetime(frame['sheet_start_time']) if 'sheet_start_time' in frame else pd.Series(dtype='datetime64[ns]')
    return {'product': product, 'row_count': len(frame), 'columns': list(frame.columns),
            'min_time': str(times.min()) if not times.empty else '',
            'max_time': str(times.max()) if not times.empty else '',
            'file': filename, 'size_bytes': target.stat().st_size, 'sheets': sheets,
            'plaintext_sha256': hashlib.sha256(payload).hexdigest(),
            'verification': 'CRC, row sequence/count, headers, first/last rows; before enterprise protection'}
