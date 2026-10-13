"""Bounded database adapter for O091 OUT-to-next-chamber intervals."""

import logging
from datetime import date

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.application.qtime.chamber_service import validate_chamber_dates
from src.indicator_domain.core.qtime.chamber import CHAMBERS
from src.indicator_domain.infrastructure.shared.date_exclusion import apply_indicator_date_exclusion
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy
from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)
SAFE_ERROR = '蒸镀单腔停留时间数据读取失败，请稍后重试。'
MAX_INTERVALS = 500_000
LINE_IDS = {'3CEE01': '3CEE001', '3CEE02': '3CEE002'}
UNIT_PATHS = ('BL0-PT', 'BL1-C1', 'BL1-C2', 'BL3-C3', 'BL3-C4',
              'BL5-C5', 'BL5-C6', 'BL7-C7', 'BL7-C8', 'BLA-MC', 'BLB-CC', 'BLD-CD')


def chamber_interval_sql(dialect: str) -> TextClause:
    """Only fixed adapter-owned expressions are interpolated; inputs are bound."""
    unit_filter = ' OR '.join(f"sub_unit_id LIKE '3CEE0%-{unit}%'" for unit in UNIT_PATHS)
    cases = '\n'.join(
        f"WHEN sub_unit_id LIKE '3CEE0%-{left}%' AND next_unit LIKE '3CEE0%-{right}%' THEN '{chamber}'"
        for left, right, chamber in zip(UNIT_PATHS, UNIT_PATHS[1:], CHAMBERS)
    )
    # SQLite's date arithmetic has millisecond precision; remove Julian-day
    # cancellation noise so exact thresholds do not turn into false exceedances.
    duration = ("ROUND((julianday(next_time) - julianday(event_time)) * 86400.0, 3)"
                if dialect == 'sqlite' else "EXTRACT(EPOCH FROM (next_time - event_time))")
    return text(f'''
        WITH events AS (
            SELECT DISTINCT glass_id, eqp_id, unit_id, sub_unit_id, inout_type, prod_id,
                event_timekey, oper_in_time,
                CASE WHEN inout_type = 'IN' THEN oper_in_time ELSE oper_out_time END AS event_time
            FROM mdw.dwt_inout_sub_unit_gls
            WHERE date_timekey >= :scan_start AND date_timekey < :scan_end
              AND factory LIKE 'OLED%' AND prod_type IN ('P', 'E')
              AND eqp_id IN ('3CEE01', '3CEE02') AND glass_id IS NOT NULL
              AND ({unit_filter} OR sub_unit_id IN ('3CEE01-BL0-LL1', '3CEE02-BL0-LL1'))
        ), passages AS (
            SELECT *, MAX(CASE WHEN sub_unit_id IN ('3CEE01-BL0-LL1','3CEE02-BL0-LL1')
                                   AND inout_type = 'IN' THEN oper_in_time END)
                OVER (PARTITION BY eqp_id, glass_id ORDER BY event_timekey, sub_unit_id, inout_type
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS entry_time
            FROM events
        ), identified AS (
            SELECT *, FIRST_VALUE(prod_id) OVER (
                PARTITION BY eqp_id, glass_id, entry_time
                ORDER BY event_timekey, sub_unit_id, inout_type
            ) AS entry_prod_id
            FROM passages WHERE entry_time >= :start_time AND entry_time < :end_time
        ), paired AS (
            SELECT *,
                LEAD(sub_unit_id) OVER w AS next_unit,
                LEAD(unit_id) OVER w AS next_parent,
                LEAD(event_time) OVER w AS next_time
            FROM identified
            WHERE sub_unit_id NOT IN ('3CEE01-BL0-LL1','3CEE02-BL0-LL1')
              AND entry_time >= :start_time AND entry_time < :end_time
            WINDOW w AS (PARTITION BY eqp_id, glass_id, entry_time
                         ORDER BY event_timekey, sub_unit_id, inout_type)
        ), intervals AS (
            SELECT glass_id, eqp_id, entry_time, entry_prod_id, event_timekey,
                CASE {cases} END AS chamber, {duration} AS duration_seconds
            FROM paired WHERE inout_type = 'OUT' AND next_parent IS NOT NULL
        )
        SELECT glass_id, eqp_id, entry_time, entry_prod_id, chamber, duration_seconds
        FROM intervals WHERE chamber IS NOT NULL
        ORDER BY entry_time, eqp_id, glass_id, event_timekey
        LIMIT :row_limit
    ''')


class DatabaseChamberQTimeRepository:
    def __init__(
        self, db_manager: DatabaseManager, *, target_seconds: float,
        data_policy: DataForwardPolicy | None = None,
        cutoff_policy: ReportCutoffPolicy | None = None,
    ) -> None:
        self._engine = db_manager.engine
        self._target_seconds = target_seconds
        self._data_policy = data_policy if data_policy is not None else ConfigLoader.get_data_forward_policy()
        self._cutoff_policy = cutoff_policy if cutoff_policy is not None else ConfigLoader.get_report_cutoff_policy()

    def cache_signature(self) -> tuple:
        return ('chamber-database-v1', self._data_policy.signature,
                self._cutoff_policy.latest_day_time, ConfigLoader.get_indicator_data_exclusion(),
                self._target_seconds, id(self._engine))

    def read(self, *, as_of: date, start_date: date | None = None, end_date: date | None = None) -> dict:
        start_date = start_date or as_of.replace(day=1)
        end_date = end_date or as_of
        validate_chamber_dates(as_of, start_date, end_date)
        try:
            return self._read(as_of, start_date, end_date)
        except Exception as exc:
            logger.error('Chamber database read failed: %s', type(exc).__name__, exc_info=True)
            raise QTimeDataAccessError(SAFE_ERROR) from exc

    def _read(self, as_of: date, start_date: date, end_date: date) -> dict:
        if self._engine is None:
            raise RuntimeError('Database engine unavailable')
        start, end = self._data_policy.to_source_window(
            pd.Timestamp(start_date), pd.Timestamp(end_date) + pd.Timedelta(days=1),
        )
        params = {'start_time': start.isoformat(sep=' '), 'end_time': end.isoformat(sep=' '),
                  'scan_start': (start - pd.Timedelta(days=1)).strftime('%Y%m%d'),
                  'scan_end': (end + pd.Timedelta(days=1)).strftime('%Y%m%d'),
                  'row_limit': MAX_INTERVALS + 1}
        with self._engine.connect() as connection:
            if connection.dialect.name == 'postgresql':
                connection.execute(text('SET TRANSACTION READ ONLY'))
                connection.execute(text("SET LOCAL statement_timeout = '60s'"))
            measurements = pd.read_sql(chamber_interval_sql(connection.dialect.name), connection, params=params)
            if len(measurements) > MAX_INTERVALS:
                raise ValueError('Chamber interval result limit exceeded; shorten the date range')
            measurements['entry_time'] = pd.to_datetime(measurements['entry_time'])
            measurements['line'] = measurements['eqp_id'].map(LINE_IDS)
            displayed = self._data_policy.shift_frame(measurements, ['entry_time'])
            displayed = self._cutoff_policy.filter_frame(displayed, 'entry_time', now=as_of)
            displayed = apply_indicator_date_exclusion(displayed, time_column='entry_time')
            products = self._load_products(connection, measurements.loc[displayed.index])
        targets = pd.DataFrame([
            {'line': line, 'chamber': chamber, 'target_seconds': self._target_seconds}
            for line in LINE_IDS.values() for chamber in CHAMBERS
        ])
        return {'measurements': displayed.drop(columns='eqp_id'), 'products': products,
                'targets': targets, 'invalid_rows': 0}

    @staticmethod
    def _load_products(connection: Connection, measurements: pd.DataFrame) -> pd.DataFrame:
        if measurements.empty:
            return pd.DataFrame(columns=['glass_id', 'prod_code'])
        # LL1.prod_id is the MES specification name, verified against real facts.
        # Using the passage identity avoids losing glass absent from SPC sampling.
        identities = measurements[['glass_id', 'entry_prod_id']].drop_duplicates()
        specs = identities['entry_prod_id'].dropna().unique().tolist()
        if not specs:
            return pd.DataFrame(columns=['glass_id', 'prod_code'])
        spec_filter = ('productspecname IN :specs' if connection.dialect.name == 'sqlite'
                       else 'productspecname = ANY(:specs)')
        sql = text(f'''SELECT DISTINCT productspecname AS entry_prod_id, productcode AS prod_code
                       FROM mdw.dwr_mes_productspec WHERE {spec_filter}''')
        if connection.dialect.name == 'sqlite':
            sql = sql.bindparams(bindparam('specs', expanding=True))
        master = pd.read_sql(sql, connection, params={'specs': specs})
        return identities.merge(master, on='entry_prod_id', how='inner')[['glass_id', 'prod_code']].drop_duplicates()
