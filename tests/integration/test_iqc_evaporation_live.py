"""Opt-in read-only comparison against the supplied FineReport SQL.

Run with IQC_LIVE_DB=1 and the existing project database configuration.
"""

from datetime import date, timedelta
import os
from pathlib import Path
import re

import pandas as pd
import pytest
from sqlalchemy import bindparam, text

from src.iqc_domain.composition import build_evaporation_service
from src.iqc_domain.core.eva_materials.evaporation import REPORT_FIELDS, project_report, measurement_results
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


@pytest.mark.skipif(os.environ.get('IQC_LIVE_DB') != '1', reason='Opt-in live database read')
def test_live_report_matches_supplied_finereport_sql():
    root = Path(__file__).resolve().parents[2]
    reference = (root / 'docs/project_files/iqc_domain/eva_materials/蒸镀材料体系报表-sql语句.txt').read_text(encoding='utf-8')
    # FineReport's empty multiselects; only the material-type clause has a default.
    lines = []
    for line in reference.splitlines():
        if '${IF(' in line:
            if 'cmcbMaterType' in line:
                lines.append("AND I.MATER_TYPE <> 'MASK'")
        else:
            lines.append(line)
    reference = '\n'.join(lines)
    reference = re.sub(r"to_timestamp\('\$\{dtStartDate\}','YYYY-MM-DD HH24:MI:SS'\)", ':start_time', reference)
    reference = re.sub(r"to_timestamp\('\$\{dtEndtDate\}','YYYY-MM-DD HH24:MI:SS'\)", ':end_time', reference)
    end = date.today()
    start = end - timedelta(days=30)
    policy = ConfigLoader.get_data_forward_policy()
    source_start, source_end = policy.to_source_window(pd.Timestamp(start), pd.Timestamp(end) + pd.Timedelta(days=1))
    engine = DatabaseManager().engine
    assert engine is not None
    with engine.connect() as connection, connection.begin():
        connection.execute(text('SET TRANSACTION READ ONLY'))
        connection.execute(text("SET LOCAL statement_timeout = '60s'"))
        connection.execute(text('SET LOCAL search_path TO mdw, public'))
        original = pd.read_sql_query(text(reference), connection, params={
            'start_time': source_start.to_pydatetime(), 'end_time': source_end.to_pydatetime(),
        })
        # Independent membership oracle: execute the complete supplied crucible
        # report, then associate its cycle/material output with request masters.
        supplied = (root / 'docs/project_files/iqc_domain/eva_materials/V3坩埚加料&回收报表-sql语句.md').read_text(encoding='utf-8')
        crucible_sql = re.findall(r'```\s*\n(.*?)```', supplied, re.DOTALL)[0]
        crucible_sql = '\n'.join(line for line in crucible_sql.splitlines() if '${IF(' not in line)
        membership_sql = text(f"""
            WITH supplied_report AS ({crucible_sql})
            SELECT DISTINCT c.MATERIALSPECNAME AS material_no, p.PRODUCTCODE AS prod_code, c.CYCLETIME AS batch_no
            FROM supplied_report c JOIN mdw.dwr_mes_productrequest r
              ON c.CYCLETIME = to_char(to_date(
                substring(r.DESCRIPTION, '^[0-9]{{4}}/[0-9]{{1,2}}/[0-9]{{1,2}}'),
                'YYYY/MM/DD'), 'YYYYMMDD')
            JOIN mdw.dwr_mes_productspec p ON p.PRODUCTSPECNAME=r.PRODUCTSPECNAME
            WHERE p.PRODUCTCODE IN :products AND r.SUBPRODUCTIONTYPE IN :work_orders
        """).bindparams(bindparam('products', expanding=True), bindparam('work_orders', expanding=True))
        memberships = pd.read_sql_query(membership_sql, connection, params={
            'products': tuple(ConfigLoader.get_enabled_products(strict=True)),
            'work_orders': tuple(ConfigLoader.get_work_order_types()),
        })
    original = original.rename(columns={'eattribute5': 'factory'})
    original = original.loc[original.factory.eq('V3') & original.mater_type.eq('有机')].copy()
    # Independent parity with the ten CASE expressions executed by PostgreSQL.
    case_columns = [f'{prefix}_{i}_result' for prefix in ('iqc', 'coa') for i in range(1, 6)]
    pd.testing.assert_frame_equal(
        measurement_results(original).reset_index(drop=True),
        original[case_columns].reset_index(drop=True),
    )
    original = original.loc[:, [field for field in REPORT_FIELDS if field != 'batch_no']]
    original = policy.shift_frame(original, ('requestdate', 'checkeddate'))
    original = ConfigLoader.get_report_cutoff_policy().filter_frame(original, 'checkeddate')

    def expected_report(products):
        selected_memberships = memberships.loc[memberships.prod_code.isin(products)]
        frame = original.loc[original.mitemname.isin(selected_memberships.material_no)].copy()
        product_labels, batch_labels = {}, {}
        for material, group in selected_memberships.groupby('material_no'):
            product_labels[material] = '、'.join(code for code in products if code in set(group.prod_code))
            batch_labels[material] = '、'.join(sorted(set(group.batch_no.astype(str))))
        frame['prod_code'] = frame.mitemname.map(product_labels)
        frame['batch_no'] = frame.mitemname.map(batch_labels)
        return project_report(frame)

    expected = expected_report(tuple(ConfigLoader.get_enabled_products(strict=True)))
    actual = build_evaporation_service().get_report(start, end)
    assert not actual.empty, 'Live acceptance requires records, not just a successful empty query'
    # Duplicate characteristics can tie the visible sort keys. Compare row multisets.
    expected_hashes = pd.util.hash_pandas_object(expected.drop(columns='序号'), index=False).sort_values().reset_index(drop=True)
    actual_hashes = pd.util.hash_pandas_object(actual.drop(columns='序号'), index=False).sort_values().reset_index(drop=True)
    pd.testing.assert_series_equal(actual_hashes, expected_hashes)

    # A selected product uses its material membership union, with no WMS
    # product-attribute filter and no duplication of source inspection rows.
    selected = (ConfigLoader.get_enabled_products(strict=True)[0],)
    expected_selected = expected_report(selected)
    actual_selected = build_evaporation_service().get_report(start, end, selected)
    pd.testing.assert_series_equal(
        pd.util.hash_pandas_object(actual_selected.drop(columns='序号'), index=False).sort_values().reset_index(drop=True),
        pd.util.hash_pandas_object(expected_selected.drop(columns='序号'), index=False).sort_values().reset_index(drop=True),
    )
