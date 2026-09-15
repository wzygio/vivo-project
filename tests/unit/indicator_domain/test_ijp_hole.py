import pandas as pd

from src.indicator_domain.core.ijp_hole.summary import summarize_holes


def test_denominator_includes_unselected_codes_and_weights_counts():
    frame = pd.DataFrame([
        ('M1', 'L1', 'P1', 'G1', '2026-09-01', 'C3RA1', 1, 1),
        ('M1', 'L1', 'P1', 'G1', '2026-09-01', 'C3RA2', 3, 1),
        ('M1', 'L1', 'P1', 'G2', '2026-09-02', 'C3RA1', 6, 0),
    ], columns=['productcode', 'line', 'printer', 'glass_id', 'day',
                'rs_code', 'code_num', 'in_window'])
    result = summarize_holes(frame, ('C3RA1',))
    assert result['glass'].ratio.tolist() == [0.25]
    assert result['total'].ratio.tolist() == [0.25]
    assert result['day'].ratio.tolist() == [0.25, 1.0]
    assert 'in_window' not in result['day']
    assert frame.code_num.sum() == 10


def test_missing_code_is_zero_and_empty_input_has_public_schema():
    frame = pd.DataFrame([dict(productcode='M1', line='L1', printer='P1',
        glass_id='G1', day='2026-09-01', rs_code='C3RA1', code_num=2, in_window=1)])
    result = summarize_holes(frame)
    assert result['total'].ratio.tolist() == [1.0, 0.0, 0.0]
    assert all(f.empty and 'ratio' in f for f in summarize_holes(frame.iloc[:0]).values())
