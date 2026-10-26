from datetime import date

import pandas as pd
import pytest

from src.iqc_domain.core.eva_materials.alerts import build_evaporation_alerts
from src.iqc_domain.core.eva_materials.alerts import previous_week_window
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.iqc_domain.application.eva_materials.alerts import publish_evaporation_alerts


def test_either_ng_selects_once_and_only_exports_public_fields():
    frame = pd.DataFrame({
        '序号': [1, 2, 3, 4, 5], '物料号': ['A', 'B', 'C', 'D', 'E'],
        'IQC结果': ['NG', 'OK', 'NG', 'OK', None],
        'COA结果': ['OK', 'NG', 'NG', 'OK', None],
        'flag': [True] * 5, 'batch_no': ['private'] * 5,
    })
    original = frame.copy(deep=True)
    alerts = build_evaporation_alerts(frame)
    assert alerts['物料号'].tolist() == ['A', 'B', 'C']
    assert list(alerts) == list(REPORT_COLUMNS)
    pd.testing.assert_frame_equal(frame, original)
    assert build_evaporation_alerts(pd.DataFrame()).empty
    # Keep distinct source facts, including duplicate-looking inspections.
    duplicated = pd.concat([frame.iloc[:1]] * 2, ignore_index=True)
    assert len(build_evaporation_alerts(duplicated)) == 2


def test_missing_result_columns_fail_instead_of_claiming_no_ng():
    with pytest.raises(ValueError, match='IQC_ALERT_SCHEMA_INVALID'):
        build_evaporation_alerts(pd.DataFrame({'IQC结果': ['OK']}))


def test_previous_natural_week_crosses_year():
    assert previous_week_window(date(2026, 1, 1)) == (date(2025, 12, 22), date(2025, 12, 28))


def test_publication_validates_scope_and_propagates_storage_failure():
    class Store:
        def save(self, alerts, *, start, end, products):
            self.alerts = alerts
            assert products == ('M678',)
            assert start <= end
    store = Store()
    alerts = publish_evaporation_alerts(pd.DataFrame({'IQC结果': ['NG'], 'COA结果': ['OK']}),
        store=store, start=date(2026, 10, 19), end=date(2026, 10, 25), products=('M678',))
    assert len(alerts) == 1
    pd.testing.assert_frame_equal(alerts, store.alerts)
    def fail(*args, **kwargs):
        raise PermissionError('private')
    store.save = fail
    with pytest.raises(PermissionError):
        publish_evaporation_alerts(pd.DataFrame(), store=store,
            start=date(2026, 10, 19), end=date(2026, 10, 25), products=('M678',))
