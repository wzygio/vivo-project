import pandas as pd
import pytest

from src.iqc_domain.core.lifetime.alerts import (
    LIFETIME_ALERT_COLUMNS, LIFETIME_LEDGER_COLUMNS, LifetimeLedgerInvalid,
    merge_lifetime_upload, normalize_lifetime_ledger, reconcile_lifetime_ledger,
    released_lifetime_alerts,
)


def facts(*samples):
    return pd.DataFrame([
        ['M678', 'B1', 'W', sample, '效率衰减', 660, value, '预测', 0.95]
        for sample, value in samples
    ], columns=LIFETIME_ALERT_COLUMNS)


def test_legacy_snapshot_and_exact_threshold():
    old = facts((1, 0.92), (2, 0.95))
    assert normalize_lifetime_ledger(old)['Flag'].tolist() == [False, False]
    public = released_lifetime_alerts(old)
    assert list(public) == list(LIFETIME_ALERT_COLUMNS)
    assert public['样品编号'].tolist() == [1]


def test_reconcile_updates_current_measurement_and_preserves_manual_flag_and_history():
    old = normalize_lifetime_ledger(facts((1, 0.9), (2, 0.91), (3, 0.89)))
    old.loc[0, 'Flag'] = True
    current = facts((1, 0.91), (2, 0.95), (4, 0.93), (5, 0.98))
    current.loc[0, ['规格时间(H)', '取值方式']] = [680, '实测']
    ledger = reconcile_lifetime_ledger(old, current)
    assert tuple(ledger) == LIFETIME_LEDGER_COLUMNS
    assert ledger['样品编号'].tolist() == [1, 2, 3, 4]
    assert ledger['本期状态'].tolist() == ['超规', '达标', '未判定', '超规']
    assert ledger.iloc[0][['Flag', '规格时间(H)', '取值方式', '规格点数值']].tolist() == [True, 680, '实测', 0.91]
    assert ledger.iloc[2]['规格点数值'] == 0.89
    assert released_lifetime_alerts(ledger)['样品编号'].tolist() == [4]
    assert reconcile_lifetime_ledger(ledger, facts())['本期状态'].tolist() == ['未判定'] * 4
    assert reconcile_lifetime_ledger(ledger, facts((2, 0.94)))['本期状态'].tolist()[1] == '超规'


def test_upload_updates_decisions_while_preserving_latest_measurements_and_unlisted_rows():
    start = normalize_lifetime_ledger(facts((1, 0.85), (2, 0.93), (3, 0.88), (4, 0.92)))
    start.loc[0, '本期状态'] = '未判定'
    start.loc[1, '本期状态'] = '达标'
    uploaded = facts((1, 0.80), (2, 0.80), (3, 0.95), (5, 0.90), (6, 0.91))
    uploaded['Flag'] = ['False', 'FALSE', 'Delete', 'TRUE', 0]
    uploaded['本期状态'] = '超规'
    with pytest.raises(LifetimeLedgerInvalid):
        merge_lifetime_upload(start, uploaded, ('M678',))
    merged = merge_lifetime_upload(start, uploaded.iloc[:3], ('M678',))
    assert merged['本期状态'].tolist() == ['未判定', '达标', '超规', '超规']
    assert merged['规格点数值'].tolist() == [0.85, 0.93, 0.88, 0.92]
    assert merged['Flag'].tolist() == [False, False, 'Delete', False]
    assert released_lifetime_alerts(merged)['样品编号'].tolist() == [4]
    pd.testing.assert_frame_equal(merge_lifetime_upload(merged, facts(), ('M678',)), merged)


def test_rejects_ambiguous_flags_partial_schema_and_invalid_product():
    data = facts((1, 0.94))
    with pytest.raises(LifetimeLedgerInvalid):
        normalize_lifetime_ledger(data.assign(Flag='False'))
    with pytest.raises(LifetimeLedgerInvalid):
        normalize_lifetime_ledger(data.assign(本期状态='超规'))
    with pytest.raises(LifetimeLedgerInvalid):
        normalize_lifetime_ledger(data.assign(Flag='bogus', 本期状态='超规'))
    with pytest.raises(LifetimeLedgerInvalid):
        merge_lifetime_upload(facts(), data, ('M626',))


def test_legacy_upload_preserves_existing_decision_and_disabled_history():
    old = normalize_lifetime_ledger(facts((1, 0.91)))
    old['Flag'] = True
    legacy = facts((1, 0.92))
    restored = merge_lifetime_upload(old, legacy, ('M626',))
    assert restored['Flag'].tolist() == [True]
    assert restored['本期状态'].tolist() == ['超规']
    assert released_lifetime_alerts(restored).empty
    with pytest.raises(LifetimeLedgerInvalid):
        merge_lifetime_upload(old, facts((1, 0.92), (2, 0.9)), ('M626',))


def test_upgrade_legacy_upload_by_adding_only_the_manual_flag_column():
    data = facts((1, 0.93), (2, 0.94))
    edited = merge_lifetime_upload(data, data.assign(Flag=['True', 'False']), ('M678',))
    assert edited['本期状态'].tolist() == ['超规', '超规']
    assert released_lifetime_alerts(edited)['样品编号'].tolist() == [2]
