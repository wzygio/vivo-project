"""Opt-in read-only M3 parity check; no source identifiers in assertion output."""

import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from src.iqc_domain.composition import build_lifetime_service
from src.iqc_domain.infrastructure.lifetime.m3_database import get_m3_engine


@pytest.mark.skipif(os.getenv('IQC_LIFETIME_LIVE_DB') != '1', reason='Opt-in M3 read')
def test_live_lifetime_public_report_matches_source():
    with get_m3_engine().connect() as connection, connection.begin():
        connection.execute(text('SET TRANSACTION READ ONLY'))
        connection.execute(text("SET LOCAL statement_timeout = '60s'"))
        raw = pd.read_sql_query(text('''
            SELECT product_group, product_id, wo_name, date_key, panel_id,
                   test_time, bu, luminance, lumi_decay, cie_x, cie_xy,
                   iss, curr_decay, efficiency, eff_decay
            FROM m3dwd.dwd_panel_eff_dec_enter
        '''), connection)
    actual = build_lifetime_service().get_report()
    assert not actual.empty, 'Live acceptance requires actual measurements'
    assert len(actual) == len(raw.drop_duplicates())
    assert len(actual.columns) == 15
    public_text = actual.to_json()
    assert all(value not in public_text for value in raw.panel_id.unique()), 'Identity leak'
    for identity, source in raw.groupby(['product_group', 'date_key', 'bu']):
        product, batch, screen = identity
        group = actual.loc[actual['产品型号'].eq(product) & actual['批次号'].eq(batch)
                           & actual['测试画面'].eq(screen)]
        for number, panel in enumerate(sorted(source.panel_id.unique()), start=1):
            expected = source.loc[source.panel_id.eq(panel)].copy()
            expected['test_time'] = pd.to_numeric(expected.test_time)
            expected = expected.sort_values('test_time')
            points = group.loc[group['样品编号'].eq(number)].sort_values('测试时间')
            assert points['测试时间'].tolist() == expected.test_time.tolist()
            assert points['产品状态'].tolist() == expected.product_id.tolist()
            assert points['量产'].tolist() == expected.wo_name.tolist()
            for column, label in [('luminance','亮度'), ('lumi_decay','亮度衰减'),
                                  ('cie_x','CIEx'), ('cie_xy','CIEy'), ('iss','Iss'),
                                  ('curr_decay','电流衰减'), ('efficiency','效率'),
                                  ('eff_decay','效率衰减')]:
                np.testing.assert_allclose(points[label], pd.to_numeric(expected[column]),
                                           equal_nan=True, err_msg=label)
