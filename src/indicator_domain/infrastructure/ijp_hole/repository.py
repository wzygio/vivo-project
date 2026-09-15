"""Filter large holes and deduplicate FineReport projections in the database."""

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.core.ijp_hole.summary import COUNT_COLUMNS, HOLE_CODES, HOLE_PRINTERS
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.report_cutoff_config import load_report_cutoff_policy

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)
SAFE_ERROR = 'IJP 孔区数据暂时无法读取，请稍后重试。'

# split_part runs in PostgreSQL before any data leaves the database. SQLite
# contract fixtures register an equivalent SQL function, never a production fallback.
_FACTS = """
WITH defects AS (
    SELECT UPDATE_TIME, GLASS_ID, CODE,
           SUBSTR(IMG_DIR, 4) || '/' || IMAGE_NAME AS image_data
    FROM EDA.OLED_IJP_DEFECT_T
    WHERE STEP_ID = '21200' AND CODE IN :hole_codes
      AND UPDATE_TIME >= :start_time AND UPDATE_TIME <= :end_time
      AND SUBSTR(split_part(SUBSTR(IMG_DIR, 4) || '/' || IMAGE_NAME, '/', 10), 17, 1)
          IN ('0','1','2','3','4')
), facts AS (
    SELECT D.UPDATE_TIME, D.GLASS_ID, D.CODE, D.image_data,
           P.PRODUCTCODE, H.EQUIP_ID, H.SUB_EQUIP_ID,
           MAX(CASE WHEN H.CUT_START_TIME >= :start_time THEN 1 ELSE 0 END) AS in_window
    FROM defects D
    JOIN EDA.OLED_CHAMBER_HST_T H ON D.GLASS_ID = H.CUT_ID
      AND H.CUT_START_TIME >= :history_start AND H.CUT_START_TIME <= :end_time
    JOIN DWR_MES_PRODUCTSPEC P ON P.PRODUCTSPECNAME = H.PRODUCT_ID
    WHERE H.SUB_EQUIP_ID IN :printers AND P.PRODUCTCODE IN :products
    {filters}
    GROUP BY D.UPDATE_TIME, D.GLASS_ID, D.CODE, D.image_data,
             P.PRODUCTCODE, H.EQUIP_ID, H.SUB_EQUIP_ID
)
SELECT productcode, SUBSTR(sub_equip_id, 1, 6) AS line,
       sub_equip_id AS printer, glass_id,
       SUBSTR(CAST(update_time AS TEXT), 1, 10) AS day,
       code AS rs_code, in_window, COUNT(*) AS code_num
FROM facts
GROUP BY productcode, sub_equip_id, glass_id,
         SUBSTR(CAST(update_time AS TEXT), 1, 10), code, in_window
ORDER BY productcode, printer, day, glass_id, rs_code
"""


class IjpHoleRepository:
    def __init__(self, db_manager: 'DatabaseManager') -> None:
        self._engine = db_manager.engine
        self._time_policy = ConfigLoader.get_data_forward_policy()

    def fetch_counts(self, query: IjpQuery) -> pd.DataFrame:
        if not query.product_codes:
            return pd.DataFrame(columns=COUNT_COLUMNS)
        params = self._window(query)
        params.update(hole_codes=HOLE_CODES, printers=HOLE_PRINTERS, products=query.product_codes)
        expanding = ['hole_codes', 'printers', 'products']
        filters = []
        if query.lines:
            filters.append('AND SUBSTR(H.SUB_EQUIP_ID, 1, 6) IN :lines')
            params['lines'] = query.lines
            expanding.append('lines')
        if query.work_order_types:
            filters.append('AND EXISTS (SELECT 1 FROM DWR_MES_PRODUCTREQUEST_V V '
                           'WHERE V.SUB_PROD_ID = H.ITEM5 AND V.SUB_PROD_TYPE IN :orders)')
            params['orders'] = query.work_order_types
            expanding.append('orders')
        if query.picis:
            filters.append('AND EXISTS (SELECT 1 FROM EDA.DWD_GLASS_OLED_CYCLE_V3 T '
                           'WHERE T.GLASS_ID = D.GLASS_ID AND T.PICI IN :picis)')
            params['picis'] = query.picis
            expanding.append('picis')
        frame = self._read(_FACTS.format(filters='\n'.join(filters)), params, expanding)
        frame = frame.reindex(columns=COUNT_COLUMNS)
        if not frame.empty:
            frame['day'] = (pd.to_datetime(frame.day) + pd.Timedelta(
                days=self._time_policy.effective_days,
            )).dt.strftime('%Y-%m-%d')
        return frame

    def list_picis(self, query: IjpQuery) -> tuple[str, ...]:
        if not query.product_codes:
            return ()
        # Match the existing report's batch option source and source-time window.
        event = ('CAST(NULLIF(EVENT_TIME, \'NaT\') AS TIMESTAMP)'
                 if self._engine is not None and self._engine.dialect.name == 'postgresql'
                 else 'EVENT_TIME')
        sql = (f'SELECT DISTINCT PICI FROM EDA.DWD_GLASS_OLED_CYCLE_V3 WHERE '
               f'{event} >= :start_time AND {event} <= :end_time '
               'AND PROD_CODE IN :products ORDER BY PICI')
        frame = self._read(sql, {**self._window(query), 'products': query.product_codes}, ['products'])
        return tuple(frame.pici.dropna().astype(str)) if not frame.empty else ()

    def _window(self, query: IjpQuery) -> dict[str, object]:
        start, end = self._time_policy.to_source_window(
            pd.Timestamp(query.start_time), load_report_cutoff_policy().cap_end(query.end_time),
        )
        return {'start_time': start.isoformat(sep=' '), 'end_time': end.isoformat(sep=' '),
                'history_start': (start - timedelta(days=7)).isoformat(sep=' ')}

    def _read(self, sql: str, params: dict[str, object], expanding: list[str]) -> pd.DataFrame:
        statement = text(sql).bindparams(*(bindparam(key, expanding=True) for key in expanding))
        try:
            if self._engine is None:
                raise RuntimeError('database unavailable')
            frame = pd.read_sql(statement, self._engine, params=params)
            frame.columns = frame.columns.str.lower()
            return frame
        except Exception as exc:
            logger.error('IJP hole query failed: %s', type(exc).__name__)
            raise IjpDataAccessError(SAFE_ERROR) from exc
