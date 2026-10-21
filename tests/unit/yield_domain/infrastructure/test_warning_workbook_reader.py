from pathlib import Path
from types import SimpleNamespace
from zipfile import BadZipFile

import pandas as pd
import pytest

from src.yield_domain.infrastructure import warning_workbook_reader as reader
from yield_domain.application.yield_service import YieldAnalysisService


def test_encrypted_warning_file_uses_readonly_values_with_original_header(monkeypatch, tmp_path):
    path = tmp_path/'limits.xlsx'
    path.write_bytes(b'\x00\x00\x00\x00encrypted')
    before = path.read_bytes()
    monkeypatch.setattr(reader.pd, 'read_excel', lambda *a, **k: (_ for _ in ()).throw(BadZipFile()))
    calls = []
    def read(file, sheet_name):
        calls.append((file,sheet_name))
        return pd.DataFrame({'Code':['D1','D2'], '预警线':['1.5%',.025]})
    monkeypatch.setattr(reader.excel_tools, '_read_encrypted_xlsx_via_com', read)
    config = SimpleNamespace(paths={'static_warning_lines':SimpleNamespace(file_name=str(path),sheet_name='P1')})
    YieldAnalysisService.load_static_warning_lines.clear()
    try:
        result = YieldAnalysisService.load_static_warning_lines(config,tmp_path/'P1')
        assert result['D1']['upper'] == .015
        assert result['D2']['upper'] == .025
        assert calls == [(path,'P1')]
        assert path.read_bytes() == before
    finally:
        YieldAnalysisService.load_static_warning_lines.clear()


def test_plain_matrix_and_ordinary_io_failure_do_not_start_office(monkeypatch, tmp_path):
    path = tmp_path/'plain.xlsx'
    pd.DataFrame({'Code':['D1'],'预警线':[.01]}).to_excel(path,index=False)
    monkeypatch.setattr(reader.excel_tools,'_read_encrypted_xlsx_via_com',
                        lambda *a, **k: pytest.fail('Office should not start'))
    assert reader.read_warning_matrix(path,'Sheet1').iloc[0,0] == 'Code'
    monkeypatch.setattr(reader.pd,'read_excel',lambda *a,**k: (_ for _ in ()).throw(PermissionError()))
    with pytest.raises(PermissionError):
        reader.read_warning_matrix(path,'Sheet1')
