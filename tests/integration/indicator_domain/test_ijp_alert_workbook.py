from datetime import date

import pandas as pd
import pytest

from src.indicator_domain.infrastructure.ijp.alert_workbook import IjpAlertWorkbook
from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS


def test_region_replacement_empty_success_and_failure_preservation(tmp_path, monkeypatch):
    path = tmp_path / 'alerts.xlsx'
    store = IjpAlertWorkbook(path)
    def rows(region):
        return pd.DataFrame([dict(region=region, productcode='M1', line='L1', printer='P1',
            rs_code='C3RA1' if region == '孔区' else 'C3DM1', ratio=.8, minimum=.9,
            period_start='2026-10-19', period_end='2026-10-25')], columns=ALERT_COLUMNS)
    store.save('AA区', rows('AA区'), date(2026, 10, 26))
    store.save('孔区', rows('孔区'), date(2026, 10, 26))
    store.save('孔区', pd.DataFrame(columns=ALERT_COLUMNS), date(2026, 10, 26))
    sheets = pd.read_excel(path, sheet_name=None)
    assert sheets['孔区'].empty and len(sheets['AA区']) == 1
    assert set(sheets['__meta__'].region) == {'AA区', '孔区'}
    original = path.read_bytes()
    def fail(*args):
        raise PermissionError('locked')
    monkeypatch.setattr('src.indicator_domain.infrastructure.ijp.alert_workbook.os.replace', fail)
    with pytest.raises(PermissionError):
        store.save('孔区', rows('孔区'), date(2026, 10, 26))
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]
