"""Read-only bindings around the existing SPC preparation and decoration pipeline."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
import re
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import event, text

from src.shared_kernel.config import ConfigLoader
from . import ExportError

# Current source ownership is verified against database catalogs before fact reads.
DIMENSIONS = {
    'eda.imp_spc_tzbjx': ('update_time', 'productspecname, step_id, parmtername, data_type'),
    'eda.imp_spc_tzbjx_mqc': ('update_time', 'ppid, prodcode'),
    'mdw.dwr_mes_productspec': ('interface_time', 'productspecname, productcode'),
    'mdw.dwd_imp_dv_param_spec': ('interface_time',
        'prod_code, step_id, param_name, param_type, usl, lsl, ucl, lcl, target, main_step_id, main_eqp_type'),
    'mdw.dwr_mes_machinespec': ('interface_time', 'machinename, detailmachinetype, factoryname'),
    'mdw.dwr_mes_tpfopolicy': ('interface_time', 'processoperationname, conditionid, factoryname, productspecname'),
    'mdw.dwr_mes_posmachine_bk': ('interface_time', 'conditionid, machinename'),
    'mdw.dwr_mes_processoperationspec': ('interface_time', 'processoperationname, description'),
}
HISTORY = {'mdw.' + name for name in (
    'dwt_inout_sht', 'dwt_inout_sub_unit_sht', 'dwt_inout_unit_sht',
    'dwt_inout_gls', 'dwt_inout_sub_unit_gls')}
ROUTE_VIEW = 'mdw.dwd_mes_oled_oper_layer_v'
MEASUREMENTS = {'eda.spc_tzbjx_array': 'sheet_start_time',
                'eda.spc_tzbjx_oled': 'glass_start_time',
                'eda.spc_tzbjx_tsp': 'glass_start_time'}
SCHEMA_SQL = """
WITH RECURSIVE sources AS (
  SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname || '.' || c.relname = ANY(CAST(:names AS text[]))
  UNION
  SELECT d.refobjid FROM sources s JOIN pg_rewrite r ON r.ev_class=s.oid
  JOIN pg_depend d ON d.objid=r.oid AND d.refclassid='pg_class'::regclass
  JOIN pg_class c ON c.oid=d.refobjid
  WHERE d.refobjid<>s.oid AND c.relkind IN ('r','p','v','m','f')
)
SELECT n.nspname || '.' || c.relname AS source, c.relkind, c.reltuples,
       a.attname AS column_name, format_type(a.atttypid,a.atttypmod) AS type
FROM sources s JOIN pg_class c ON c.oid=s.oid
JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=s.oid AND a.attnum>0 AND NOT a.attisdropped
"""


def audit_schema(connection) -> str:
    """Inspect columns, view dependencies and low-cost row estimates without scans."""
    expected = set(DIMENSIONS) | HISTORY | set(MEASUREMENTS) | {ROUTE_VIEW}
    rows = connection.execute(text(SCHEMA_SQL), {'names': sorted(expected)}).mappings().all()
    columns: dict[str, dict[str, str]] = {}
    for row in rows:
        columns.setdefault(row['source'], {})[row['column_name']] = row['type']
    if set(columns) != expected:
        raise ExportError('Database source dependencies changed; review the export adapter before running')
    for source, (timestamp, fields) in DIMENSIONS.items():
        required = {value.strip() for value in fields.split(',')} | {timestamp}
        if not required <= columns[source].keys() or 'timestamp' not in columns[source][timestamp]:
            raise ExportError('Reference source schema changed; review temporal filters before running')
    if any('date_timekey' not in columns[source] for source in HISTORY):
        raise ExportError('History source has no verified time filter')
    if any('timestamp' not in columns[source].get(column, '') for source, column in MEASUREMENTS.items()):
        raise ExportError('Measurement source has no verified timestamp filter')
    return connection.execute(text(
        "SELECT pg_get_viewdef('mdw.dwd_mes_oled_oper_layer_v'::regclass, true)"
    )).scalar_one().strip().rstrip(';')


class TemporalReadGuard:
    """Time-bound each reference source, including every underlying view source."""

    def __init__(self, view: str, asof: datetime) -> None:
        self.asof = asof.strftime('%Y-%m-%d %H:%M:%S')
        self.view = self.bound_dimensions(view)

    def bound_dimensions(self, sql: str) -> str:
        for table, (timestamp, fields) in DIMENSIONS.items():
            name = table.split('.')[1]
            pattern = rf'(?i)(?<![\w.])(?:mdw\.|eda\.)?{re.escape(name)}\b'
            # Identifiers are fixed above; timestamp comes only from datetime.strftime.
            replacement = (f'(SELECT {fields} FROM {table} WHERE '
                           f"({timestamp} <= TIMESTAMP '{self.asof}' OR {timestamp} IS NULL))")

            def replace_source(match: re.Match) -> str:
                following = re.match(r'\s*(\w+)', sql[match.end():])
                alias = following is None or following.group(1).upper() in {
                    'WHERE', 'GROUP', 'ORDER', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'LIMIT', 'UNION'}
                return replacement + (f' AS {name}' if alias else '')

            sql = re.sub(pattern, replace_source, sql)
        return sql

    def __call__(self, conn, cursor, statement, parameters, context, executemany):
        if not re.match(r'^\s*(SELECT|WITH)\b', statement, re.I):
            raise ExportError('Export permits read queries only')
        if 'dwt_inout_' in statement.lower() and not re.search(r'date_timekey\s+BETWEEN', statement, re.I):
            raise ExportError('History query is missing its bounded time window')
        for source, column in MEASUREMENTS.items():
            if source in statement.lower() and not (
                    re.search(rf'T\.{column}\s*>=', statement, re.I)
                    and re.search(rf'T\.{column}\s*<=', statement, re.I)):
                raise ExportError('Measurement query is missing its bounded time window')
        bounded = self.bound_dimensions(statement)
        # The inserted PostgreSQL view is already bounded. Escape literal '%' for DBAPI.
        bounded = re.sub(r'(?i)mdw\.dwd_mes_oled_oper_layer_v\b',
                         lambda _: '(' + self.view.replace('%', '%%') + ')', bounded)
        return bounded, parameters


@contextmanager
def readonly_source():
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

    engine = DatabaseManager().engine
    if engine is None:
        raise ExportError('Database unavailable; check the project connection environment')
    try:
        with engine.connect().execution_options(postgresql_readonly=True) as connection:
            connection.execute(text("SET LOCAL statement_timeout = '180s'"))
            view = audit_schema(connection)
            rules = read_outlier_rules()
            guard = TemporalReadGuard(view, datetime.now(ZoneInfo('Asia/Shanghai')).replace(tzinfo=None))
            event.listen(connection, 'before_cursor_execute', guard, retval=True)
            try:
                yield SimpleNamespace(engine=connection, outlier_rules=rules)
            finally:
                event.remove(connection, 'before_cursor_execute', guard)
    finally:
        engine.dispose()


def read_outlier_rules() -> pd.DataFrame:
    """Use the project's read-only Excel reader rather than decrypt/SaveAs copies."""
    from fr_file_decryption import is_encrypted_file
    from src.shared_kernel.utils.excel_tools import read_workbook_sheets
    from src.inline_domain.infrastructure.shared.outlier_filter_rules import REQUIRED_RULE_COLUMNS

    path = ConfigLoader.get_domain_resource_path('inline_domain', 'spc_outlier_filters')
    if not path.is_file():
        raise ExportError('Required outlier-filter workbook is missing')
    if is_encrypted_file(path):
        import pythoncom
        import win32com.client

        pythoncom.CoInitialize()
        try:
            try:
                active = win32com.client.GetActiveObject('Excel.Application')
            except pythoncom.com_error:
                active = None
            if active is not None:
                try:
                    _ = active.Workbooks.Count  # No changes to the user's existing Excel.
                except Exception as exc:
                    raise ExportError('Excel is busy: finish cell editing or close its dialog, then rerun') from exc
                finally:
                    active = None
        finally:
            pythoncom.CoUninitialize()
    sheets = read_workbook_sheets(path)
    if not sheets:
        raise ExportError('Required outlier-filter workbook has no readable sheet')
    rules = next(iter(sheets.values())).rename(columns=lambda value: str(value).strip())
    if not REQUIRED_RULE_COLUMNS <= set(rules.columns):
        raise ExportError('Outlier-filter workbook is missing required columns')
    return rules.fillna('').astype(str)


class MeasurementSource:
    """Reuse fresh snapshots or query only the selected source window in memory."""

    def __init__(self, adapter, start: date, end: date) -> None:
        self.adapter, self.start, self.end = adapter, start, end

    def get_measurements(self, product: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
        from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import measurement_snapshot_coverage
        from src.inline_domain.infrastructure.shared.rolling_snapshot import is_fresh, read_metadata

        path = self.adapter.snapshot_dir / f'inline_measurements_{product}.parquet'
        metadata = read_metadata(path, self.adapter.SNAPSHOT_POLICY_VERSION, policy_file=True)
        coverage = measurement_snapshot_coverage(path, self.adapter.data_forward_policy)
        fresh = (is_fresh(metadata, self.end, self.adapter.SNAPSHOT_TTL_HOURS)
                 and coverage is not None and coverage['start_date'] <= self.start.isoformat()
                 and coverage['end_date'] >= self.end.isoformat())
        if fresh:
            source = pd.read_parquet(path)
            if read_metadata(path, self.adapter.SNAPSHOT_POLICY_VERSION, policy_file=True) != metadata:
                raise ExportError(f'{product}: source snapshot changed during the read; retry the export')
        else:
            display_end = ConfigLoader.get_report_cutoff_policy().cap_end(
                pd.Timestamp(self.end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            source_start, source_end = self.adapter.data_forward_policy.to_source_window(
                pd.Timestamp(self.start), display_end)
            print(f'SOURCE {product}: read-only query {source_start.date()} to {source_end.date()}', flush=True)
            source = self.adapter.measurement_loader(self.adapter.db_manager,
                source_start.strftime('%Y-%m-%d'), source_end.strftime('%Y-%m-%d'), product)
            if self.adapter.measurement_corrector is not None and not source.empty:
                source = self.adapter.measurement_corrector(source)
        # Reuse the repository's sole source-time/display-time output boundary.
        displayed = self.adapter._report_projection(source)
        if fresh:
            displayed.attrs['spc_source_coverage'] = coverage
        displayed.attrs['export_source'] = 'fresh-snapshot' if fresh else 'read-only-query'
        return displayed


def load_product(manager, product: str, start: date, end: date) -> pd.DataFrame:
    from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    from src.inline_domain.composition import build_raw_measurement_repository
    from src.inline_domain.core.shared.sheet_oos_alerts import previous_iso_week_range
    from src.inline_domain.infrastructure.shared.main_process_history_repository import InlineMainProcessHistoryRepository
    from src.inline_domain.infrastructure.shared.measurement_metadata_loader import InlineMeasurementMetadataRepository
    from src.inline_domain.infrastructure.shared.measurement_preparation import InlineMeasurementPreparationRepository
    from src.inline_domain.infrastructure.shared.outlier_filter_rules import apply_outlier_filter_rules
    from src.inline_domain.infrastructure.spc.spc_repository import SpcRepository

    class ReadOnlyPreparation(InlineMeasurementPreparationRepository):
        def _apply_outlier_filters(self, frame: pd.DataFrame, prod_code: str) -> pd.DataFrame:
            return apply_outlier_filter_rules(frame, prod_code, manager.outlier_rules)

    repository = SpcRepository(ReadOnlyPreparation(
        raw_measurements=MeasurementSource(build_raw_measurement_repository(manager, product), start, end),
        metadata=InlineMeasurementMetadataRepository(manager),
        main_process_history=InlineMainProcessHistoryRepository(manager),
    ))
    points = repository.get_spc_measurements(SpcQueryConfig(
        prod_code=product, start_date=start.isoformat(), end_date=end.isoformat(), data_type_filter='SPC'))
    specs = repository.get_spc_spec_limits(product)
    if points.empty:
        return points
    if specs.empty:
        raise ExportError(f'{product}: specifications missing; no partially decorated export was written')
    points = ConfigLoader.get_report_cutoff_policy().filter_frame(points, 'sheet_start_time')
    previous_start, _ = previous_iso_week_range(pd.Timestamp(end))
    return prepare_decorated_data(
        points, specs, product, 'spc', persist=False,
        sheet_features_start_date=previous_start.strftime('%Y-%m-%d'),
        spc_point_policy=ConfigLoader.get_spc_point_decoration_policy(),
    ).raw_measurements_df
