import pandas as pd
import pytest

from src.equipment_domain.infrastructure.parts_alert_workbook import PartsAlertWorkbook
from src.equipment_domain.core.parts_alerts import build_parts_alerts, PARTS_PUBLIC_COLUMNS


def test_snapshot_overwrites_clears_and_preserves_on_write_failure(tmp_path, monkeypatch):
    path = tmp_path / 'alerts.xlsx'
    store = PartsAlertWorkbook(path)
    alerts = build_parts_alerts(pd.DataFrame({'备件类型': ['=1+1'], '使用进度': [110]}))
    store.save(alerts, as_of_date='2026-10-26', last_update='2026-10-25')
    sheets = pd.read_excel(path, sheet_name=None)
    assert sheets['超规备件']['备件类型'].tolist() == ['=1+1']
    assert sheets['超规备件']['使用进度'].item() == 110
    assert list(sheets['超规备件']) == list(PARTS_PUBLIC_COLUMNS)
    assert sheets['__meta__'].iloc[0].alert_count == 1
    # Owned sheets can be replaced while unrelated workbook notes are preserved.
    with pd.ExcelWriter(path, mode='a', engine='openpyxl') as writer:
        pd.DataFrame({'note': ['keep']}).to_excel(writer, sheet_name='notes', index=False)
    store.save(build_parts_alerts(pd.DataFrame()), as_of_date='2026-10-26', last_update='')
    sheets = pd.read_excel(path, sheet_name=None)
    assert sheets['超规备件'].empty and sheets['__meta__'].iloc[0].alert_count == 0
    assert sheets['notes'].note.tolist() == ['keep']
    original = path.read_bytes()
    def fail(*args):
        raise PermissionError('locked')
    monkeypatch.setattr('src.equipment_domain.infrastructure.parts_alert_workbook.os.replace', fail)
    with pytest.raises(PermissionError):
        store.save(alerts, as_of_date='2026-10-26', last_update='')
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


def test_composition_uses_configured_full_path(monkeypatch, tmp_path):
    from src.equipment_domain.composition import create_parts_alert_store
    from src.shared_kernel.config import ConfigLoader

    configured = tmp_path / 'external' / 'renamed.xlsx'
    def resource_path(domain, key, fallback):
        assert domain == 'equipment_domain' and key == 'parts_alerts'
        return configured
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', resource_path)
    store = create_parts_alert_store()
    assert store.path == configured
    store.save(build_parts_alerts(pd.DataFrame()), as_of_date='2026-10-26', last_update='')
    assert configured.exists()
