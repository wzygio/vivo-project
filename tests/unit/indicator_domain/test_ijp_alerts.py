from datetime import date

import pandas as pd
import pytest

from src.indicator_domain.core.ijp.alerts import build_weekly_alerts


@pytest.mark.parametrize('region,code,column', [
    ('AA区', 'C3DM1', 'display_ratio'), ('孔区', 'C3RA1', 'ratio'),
])
def test_previous_week_strict_threshold_and_public_projection(region, code, column):
    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer=printer, rs_code=code,
             period='周', period_start=start, period_end=end,
             has_data=has_data, **{column: ratio}, raw_ratio=0.01)
        for printer, start, end, ratio, has_data in [
            ('P1', '2026-10-19', '2026-10-25', .8999, True),
            ('P2', '2026-10-19', '2026-10-25', .9, True),
            ('P3', '2026-10-19', '2026-10-25', 0, False),
            ('P4', '2026-10-26', '2026-10-26', .2, True),
            ('P5', '2026-10-01', '2026-10-26', .2, True),
        ]
    ])
    result = build_weekly_alerts(frame, region, date(2026, 10, 26))
    assert result.printer.tolist() == ['P1']
    assert result.ratio.item() == .8999
    assert not {'raw_ratio', 'code_num', 'display_ratio'} & set(result)
    assert build_weekly_alerts(frame.iloc[:0], region, date(2026, 10, 26)).empty


def test_previous_week_across_year():
    frame = pd.DataFrame([dict(productcode='M1', line='L1', printer='P1',
        rs_code='C3RA1', period_start='2025-12-22', period_end='2025-12-28',
        has_data=True, ratio=.8)])
    assert len(build_weekly_alerts(frame, '孔区', date(2026, 1, 1))) == 1
