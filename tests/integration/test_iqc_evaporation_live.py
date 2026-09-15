"""Opt-in read-only comparison against the supplied FineReport SQL.

Run with IQC_LIVE_DB=1 and the existing project database configuration.
"""

from datetime import date, timedelta
import os
from pathlib import Path
import re

import pandas as pd
import pytest
from sqlalchemy import text

from src.iqc_domain.composition import build_evaporation_service
from src.iqc_domain.core.evaporation import REPORT_FIELDS, project_report
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager


@pytest.mark.skipif(os.environ.get('IQC_LIVE_DB') != '1', reason='Opt-in live database read')
def test_live_report_matches_supplied_finereport_sql():
    root = Path(__file__).resolve().parents[2]
    reference = (root / 'docs/project_files/iqc_domain/蒸镀材料体系报表-sql语句.txt').read_text(encoding='utf-8')
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
    original = original.rename(columns={'eattribute5': 'factory'})
    original = original.loc[:, list(REPORT_FIELDS)]
    original = policy.shift_frame(original, ('requestdate', 'checkeddate'))
    original = ConfigLoader.get_report_cutoff_policy().filter_frame(original, 'checkeddate')
    expected = project_report(original)
    actual = build_evaporation_service().get_report(start, end)
    assert not actual.empty, 'Live acceptance requires records, not just a successful empty query'
    # Duplicate characteristics can tie the visible sort keys. Compare row multisets.
    expected_hashes = pd.util.hash_pandas_object(expected.drop(columns='序号'), index=False).sort_values().reset_index(drop=True)
    actual_hashes = pd.util.hash_pandas_object(actual.drop(columns='序号'), index=False).sort_values().reset_index(drop=True)
    pd.testing.assert_series_equal(actual_hashes, expected_hashes)
