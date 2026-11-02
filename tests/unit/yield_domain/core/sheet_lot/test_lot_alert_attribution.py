"""Lot attribution uses the business group already present in calculated facts."""
import pandas as pd
import pytest

from src.yield_domain.core.sheet_lot.lot_alerts import compute_lot_oos_records


@pytest.mark.parametrize('group,factory', [
    ('Array_Line', 'ARRAY'), ('Array_Pixel', 'ARRAY'), ('Array_Mura', 'ARRAY'),
    ('OLED_Mura', 'OLED'), ('TP_Short NG', 'TP'), ('TP 容值NG', 'TP'),
])
def test_lot_oos_publishes_fact_group_and_its_factory(group, factory):
    details = pd.DataFrame([{
        'lot_id': '000001', 'defect_desc': 'D1', 'defect_group': group,
        'warehousing_time': '20261028', 'array_input_time': '20261001',
        'defect_rate': .02, 'defect_panel_count': 2,
    }])
    before = details.copy(deep=True)
    records, count = compute_lot_oos_records({'code_level_details': {group: details}}, {'D1': {'upper': .01}},
                                            factory_mapping={group: factory})
    assert count == 1 and len(records) == 1
    assert records[0]['超规 Lot ID'] == '000001'
    assert records[0]['异常 Code'] == 'D1'
    assert records[0]['defect_group'] == group
    assert records[0]['factory'] == factory
    assert records[0]['实际不良率'] == '2.00%'
    pd.testing.assert_frame_equal(details, before)


def test_unknown_group_keeps_oos_but_does_not_invent_factory():
    rows = pd.DataFrame([{'lot_id': 'L1', 'defect_desc': 'D1', 'defect_group': 'Unknown',
                          'warehousing_time': '20261028', 'defect_rate': .02}])
    records, count = compute_lot_oos_records({'code_level_details': {'Unknown': rows}}, {'D1': {'upper': .01}})
    assert count == 1 and records[0]['defect_group'] == 'Unknown'
    assert records[0]['factory'] is None


def test_older_fact_frame_uses_its_existing_group_key_without_mutating_input():
    rows = pd.DataFrame([{'lot_id': 'L1', 'defect_desc': 'D1',
                          'warehousing_time': '20261028', 'defect_rate': .02}])
    records, _ = compute_lot_oos_records({'code_level_details': {'OLED_Mura': rows}}, {'D1': {'upper': .01}},
                                        factory_mapping={'OLED_Mura': 'OLED'})
    assert records[0]['defect_group'] == 'OLED_Mura' and records[0]['factory'] == 'OLED'
    assert 'defect_group' not in rows
