import pandas as pd
import pytest

from src.equipment_domain.core.parts_alerts import build_parts_alerts, PARTS_PUBLIC_COLUMNS
from src.equipment_domain.application.parts_alert_service import publish_parts_alerts


def test_strict_progress_limit_uses_current_report_and_public_columns():
    frame = pd.DataFrame({
        '厂别': ['Array'] * 7,
        '备件类型': ['A', 'B', 'C', 'D', 'E', 'F', 'G'],
        '使用进度': [100, 100.001, 0, None, 'invalid', float('inf'), '101'],
        '预警状态': ['预警', '正常', '已更换', '正常', '正常', '超规', '超规'],
        '是否超规': [True] * 7, '原始测量值': [999] * 7,
        '参数名称': ['internal'] * 7,
    })
    original = frame.copy(deep=True)
    result = build_parts_alerts(frame)
    assert result['备件类型'].tolist() == ['B', 'G']
    assert result['使用进度'].tolist() == [100.001, 101]
    assert result['预警状态'].eq('超规').all()
    assert result['厂别'].eq('ARRAY').all()
    assert list(result) == list(PARTS_PUBLIC_COLUMNS)
    pd.testing.assert_frame_equal(frame, original)
    assert build_parts_alerts(pd.DataFrame()).empty


def test_nonempty_report_without_progress_is_an_error():
    with pytest.raises(ValueError, match='使用进度'):
        build_parts_alerts(pd.DataFrame({'厂别': ['OLED']}))


def test_publication_uses_the_supplied_full_report_and_propagates_write_failure():
    class Store:
        def save(self, alerts, *, as_of_date, last_update):
            self.alerts = alerts
            assert as_of_date == '2026-10-26' and last_update == '2026-10-25'
    store = Store()
    result = publish_parts_alerts(pd.DataFrame({'使用进度': [101, 99]}),
        store=store, as_of_date='2026-10-26', last_update='2026-10-25')
    pd.testing.assert_frame_equal(result, store.alerts)
    assert len(result) == 1
    def fail(*args, **kwargs):
        raise PermissionError('locked')
    store.save = fail
    with pytest.raises(PermissionError):
        publish_parts_alerts(pd.DataFrame(), store=store, as_of_date='2026-10-26', last_update='')
