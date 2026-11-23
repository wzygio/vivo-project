"""Complete lifetime facts in one read-only transaction."""

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.iqc_domain.application.lifetime.lifetime import LifetimeSourceUnavailable

logger = logging.getLogger(__name__)
REPORT_SQL = """
SELECT product_group, product_id, wo_name, date_key, panel_id, test_time,
       bu, luminance, lumi_decay, cie_x, cie_xy, iss, curr_decay, efficiency, eff_decay, jncd
FROM m3dwd.dwd_panel_eff_dec_enter
WHERE ext_time <= LOCALTIMESTAMP OR ext_time IS NULL
LIMIT 100001
"""

# Full sample histories are needed for stable group-local sample numbering.
# Inspect catalog estimates before reading and reject a partial result if they lag.
SOURCE_SIZE_SQL = """
SELECT relkind, relhassubclass, reltuples, pg_total_relation_size(oid)
FROM pg_class
WHERE oid = to_regclass('m3dwd.dwd_panel_eff_dec_enter')
  AND EXISTS (
      SELECT 1 FROM pg_attribute
      WHERE attrelid = pg_class.oid AND attname = 'ext_time' AND NOT attisdropped
        AND atttypid = 'timestamp'::regtype
  )
"""


class LifetimeRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def read_report(self) -> pd.DataFrame:
        try:
            with self._engine.connect() as connection, connection.begin():
                connection.execute(text('SET TRANSACTION READ ONLY'))
                connection.execute(text("SET LOCAL statement_timeout = '60s'"))
                kind, children, estimated_rows, size = connection.execute(text(SOURCE_SIZE_SQL)).one()
                if kind != 'r' or children or estimated_rows > 100_000 or size > 64 * 1024**2:
                    raise LifetimeSourceUnavailable('LIFETIME_SOURCE_SIZE')
                chunks = pd.read_sql_query(text(REPORT_SQL), connection, chunksize=5_000)
                frame = pd.concat(chunks, ignore_index=True)
                if len(frame) > 100_000:
                    raise LifetimeSourceUnavailable('LIFETIME_SOURCE_SIZE')
                return frame
        except Exception as exc:
            logger.error('LIFETIME_SOURCE_UNAVAILABLE exception_type=%s', type(exc).__name__)
            raise LifetimeSourceUnavailable('LIFETIME_SOURCE_UNAVAILABLE') from None
