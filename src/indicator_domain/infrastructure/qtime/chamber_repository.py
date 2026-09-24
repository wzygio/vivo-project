"""Workbook residence facts joined to source-month OLED glass product identities."""

import logging
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.infrastructure.qtime.chamber_workbook import read_chamber_workbook
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy

logger = logging.getLogger(__name__)
SAFE_ERROR = '蒸镀单腔停留时间数据读取失败，请稍后重试。'
PRODUCT_SQL = text('''
SELECT DISTINCT sto.glass_id, dmp.productcode AS prod_code
FROM eda.spc_tzbjx_oled sto
JOIN mdw.dwr_mes_productspec dmp ON dmp.productspecname = sto.product_spec
WHERE sto.glass_start_time >= :start_time AND sto.glass_start_time < :end_time
''')


class ChamberQTimeRepository:
    def __init__(
        self, db_manager, sources: dict[str, Path], *,
        data_policy: DataForwardPolicy | None = None,
        cutoff_policy: ReportCutoffPolicy | None = None,
    ) -> None:
        self._engine = db_manager.engine
        self._sources = {line: Path(path) for line, path in sources.items()}
        self._data_policy = data_policy if data_policy is not None else ConfigLoader.get_data_forward_policy()
        self._cutoff_policy = cutoff_policy if cutoff_policy is not None else ConfigLoader.get_report_cutoff_policy()

    def cache_signature(self) -> tuple:
        files = []
        for line, path in self._sources.items():
            try:
                stat = path.stat()
                files.append((line, str(path.resolve()), stat.st_mtime_ns, stat.st_size))
            except OSError:
                files.append((line, str(path.resolve()), None, None))
        return (tuple(files), self._data_policy.signature, self._cutoff_policy.latest_day_time, id(self._engine))

    def read(self, *, as_of: date) -> dict:
        try:
            return self._read(as_of)
        except Exception as exc:
            # The browser receives a stable business message, never SQL, paths or IDs.
            logger.error('Chamber Q-Time source failed: %s', type(exc).__name__, exc_info=True)
            raise QTimeDataAccessError(SAFE_ERROR) from exc

    def _read(self, as_of: date) -> dict:
        if not self._sources:
            raise ValueError('No chamber sources configured')
        sources = [read_chamber_workbook(
            path, line=line, temporary_root=ConfigLoader.get_project_root() / 'output' / 'decrypted_files',
        ) for line, path in self._sources.items()]
        measurements = pd.concat([source['measurements'] for source in sources], ignore_index=True)
        displayed = self._data_policy.shift_frame(measurements, ['entry_time'])
        displayed = self._cutoff_policy.filter_frame(displayed, 'entry_time', now=as_of)
        # Match visible measurements using their unshifted source months.
        eligible = measurements.loc[measurements.index.isin(displayed.index)]
        products = self._load_products(eligible)
        return {
            'measurements': displayed,
            'products': products,
            'targets': pd.concat([source['targets'] for source in sources], ignore_index=True),
            'invalid_rows': sum(source['invalid_rows'] for source in sources),
        }

    def _load_products(self, measurements: pd.DataFrame) -> pd.DataFrame:
        if measurements.empty:
            return pd.DataFrame(columns=['glass_id', 'prod_code'])
        if self._engine is None:
            raise RuntimeError('Database engine unavailable')
        times = pd.to_datetime(measurements['entry_time'])
        start = times.min().to_period('M').start_time
        end = (times.max().to_period('M') + 1).start_time
        with self._engine.connect() as connection:
            if connection.dialect.name == 'postgresql':
                connection.execute(text('SET TRANSACTION READ ONLY'))
                connection.execute(text("SET LOCAL statement_timeout = '60s'"))
            # One bounded DISTINCT scan avoids rescanning the monthly SPC facts
            # for every workbook ID batch. Only the identity pairs leave SQL.
            result = pd.read_sql(PRODUCT_SQL, connection, params={
                'start_time': start.isoformat(sep=' '), 'end_time': end.isoformat(sep=' '),
            })
        result.columns = result.columns.str.lower()
        return result.loc[result['glass_id'].isin(measurements['glass_id'])].reset_index(drop=True)
