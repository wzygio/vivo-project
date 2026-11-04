"""Public capability records must carry honest finite values and ISO periods."""
from datetime import date

from pydantic import ValidationError
import pytest

from api.schemas import CapabilityRecord, PUBLIC_RECORD_ADAPTER


def record(**changes):
    return dict(sub_domain='SPC', step_id='0100', factory='ARRAY', layer='PI2',
                parameter_name='THK', event_type='CPK', entity_type='capability',
                entity_id=['00002', '00001'], product='P1', occurred_at='2021-01-03',
                boundary='LOWER', metric_value=1.2, lower_bound=1.33,
                period_type='week', period_label='2020-W53',
                period_start_date=date(2020, 12, 28), period_end_date=date(2021, 1, 3)) | changes


def test_capability_cross_year_week_serializes_as_calendar_dates():
    parsed = PUBLIC_RECORD_ADAPTER.validate_python(record())
    assert isinstance(parsed, CapabilityRecord)
    public = parsed.model_dump(mode='json', by_alias=True)
    assert public['统计开始日期'] == '2020-12-28' and public['统计截止日期'] == '2021-01-03'
    assert public['发生时间'] == '2021-01-03' and public['统计周期'] == '2020-W53'
    assert public['对象编号'] == ['00001', '00002']
    assert public['命中边界'] == 'spec'


@pytest.mark.parametrize('field', ['metric_value', 'lower_bound'])
@pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan'), None])
def test_capability_contract_rejects_nonfinite_or_missing_values(field, value):
    with pytest.raises(ValidationError):
        PUBLIC_RECORD_ADAPTER.validate_python(record(**{field: value}))


@pytest.mark.parametrize('changes', [
    {'sub_domain': 'CTQ'}, {'entity_type': 'sheet'}, {'entity_id': 'FAKE_SHEET'},
    {'step_id': None}, {'layer': None}, {'factory': ''}, {'parameter_name': ''},
    {'period_type': 'month'}, {'period_label': '2021-W01'},
    {'period_end_date': date(2021, 1, 4)}, {'period_start_date': date(2020, 12, 29)},
    {'occurred_at': '2021-01-03T00:00:00+08:00'}, {'metric_value': 1.33},
    {'flag': False}, {'cpk_replacement': 99.0},
])
def test_capability_contract_rejects_false_identity_or_private_columns(changes):
    with pytest.raises(ValidationError):
        PUBLIC_RECORD_ADAPTER.validate_python(record(**changes))


@pytest.mark.parametrize('members', [None, [], [''], ['  '], [None], [1], ['x' * 129]])
def test_capability_contract_requires_complete_textual_sheet_membership(members):
    with pytest.raises(ValidationError):
        PUBLIC_RECORD_ADAPTER.validate_python(record(entity_id=members))


def test_capability_members_are_deduplicated_and_sorted_without_losing_zeroes():
    public = PUBLIC_RECORD_ADAPTER.validate_python(
        record(entity_id=['00002', ' 00001 ', '00002', '00000'])
    ).model_dump(mode='json', by_alias=True)
    assert public['对象编号'] == ['00000', '00001', '00002']
