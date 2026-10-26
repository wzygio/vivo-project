from datetime import date

import pandas as pd
import pytest

from src.iqc_domain.infrastructure.eva_materials.alert_workbook import EvaporationAlertWorkbook
from src.iqc_domain.core.eva_materials.alerts import build_evaporation_alerts
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS


def test_latest_snapshot_empty_clear_and_atomic_failure(tmp_path, monkeypatch):
    path = tmp_path / 'alerts.xlsx'
    store = EvaporationAlertWorkbook(path)
    alerts = build_evaporation_alerts(pd.DataFrame({
        '物料号': ['=1+1'], 'IQC结果': ['NG'], 'COA结果': ['OK'], 'flag': [True],
    }))
    scope = dict(start=date(2026, 10, 19), end=date(2026, 10, 25), products=('M678', 'M626'))
    store.save(alerts, **scope)
    sheets = pd.read_excel(path, sheet_name=None)
    assert list(sheets['超规材料']) == list(REPORT_COLUMNS)
    assert sheets['超规材料']['物料号'].tolist() == ['=1+1']
    assert sheets['__meta__'].iloc[0].alert_count == 1
    assert sheets['__meta__'].iloc[0].products == 'M678、M626'
    with pd.ExcelWriter(path, engine='openpyxl', mode='a') as writer:
        pd.DataFrame({'note': ['keep']}).to_excel(writer, sheet_name='notes', index=False)
    store.save(build_evaporation_alerts(pd.DataFrame()), **scope)
    sheets = pd.read_excel(path, sheet_name=None)
    assert sheets['超规材料'].empty and sheets['__meta__'].iloc[0].alert_count == 0
    assert sheets['notes'].note.tolist() == ['keep']
    previous = path.read_bytes()
    def fail(*args):
        raise PermissionError('locked')
    monkeypatch.setattr('src.iqc_domain.infrastructure.eva_materials.alert_workbook.os.replace', fail)
    with pytest.raises(PermissionError):
        store.save(alerts, **scope)
    assert path.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [path]


def test_composition_respects_configured_filename(monkeypatch, tmp_path):
    from src.iqc_domain.composition import build_evaporation_alert_store
    from src.shared_kernel.config import ConfigLoader
    configured = tmp_path / 'external' / 'renamed.xlsx'
    def resource_path(domain, key, fallback):
        assert domain == 'iqc_domain' and key == 'evaporation_alerts'
        return configured
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', resource_path)
    assert build_evaporation_alert_store().path == configured
