"""Complete lifetime facts in one read-only transaction."""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.iqc_domain.application.lifetime.lifetime import LifetimeSourceUnavailable

logger = logging.getLogger(__name__)
REPORT_SQL = """
SELECT product_group, product_id, wo_name, date_key, panel_id, test_time,
       bu, luminance, lumi_decay, cie_x, cie_xy, iss, curr_decay, efficiency, eff_decay
FROM m3dwd.dwd_panel_eff_dec_enter
"""


class LifetimeRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def read_report(self) -> pd.DataFrame:
        try:
            with self._engine.connect() as connection, connection.begin():
                connection.execute(text('SET TRANSACTION READ ONLY'))
                connection.execute(text("SET LOCAL statement_timeout = '60s'"))
                return pd.read_sql_query(text(REPORT_SQL), connection)
        except Exception as exc:
            logger.error('LIFETIME_SOURCE_UNAVAILABLE exception_type=%s', type(exc).__name__)
            raise LifetimeSourceUnavailable('LIFETIME_SOURCE_UNAVAILABLE') from None
