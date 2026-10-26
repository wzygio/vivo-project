"""Bounded monthly source queries; maintained rates never query Defect facts."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from functools import cached_property
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.shared_kernel.utils.excel_tools import read_workbook_sheets
from src.yield_domain.core.mwd_trend.modifier_table import resolve_monthly_targets
from src.yield_domain.core.mwd_trend.period_trend import RATE_COLUMNS, rates_from_counts


BASE = """
FROM dwt_warehousing_pnl dwp
JOIN dwr_mes_productspec dmp ON dwp.prod_id=dmp.productspecname
{joins}
WHERE dwp.last_flag='Y' AND dmp.productcode=:product
AND dwp.sub_prod_type IN :orders
AND dwp.first_ship_date >= :start AND dwp.first_ship_date <= :end
AND (dwp.first_ship_date < :today OR
     (dwp.first_ship_date = :today AND
      dwp.first_ship_time ~ '^[0-9]{{14}}([0-9]{{1,6}})?$' AND
      rpad(dwp.first_ship_time,20,'0') <= :cutoff))
{extra}
"""


class PeriodTrendDatabaseSource:
    def __init__(self, modifier_path: Path, *, db_manager=None):
        self.path = modifier_path
        self._db = db_manager
        self._probed = False

    @cached_property
    def signature(self) -> str:
        policy = [(p, ConfigLoader.load_config(p).data_source.work_order_types,
                   ConfigLoader.load_config(p).data_source.target_defect_groups,
                   ConfigLoader.load_config(p).processing.get('defect_multipliers'))
                  for p in ConfigLoader.get_enabled_products()]
        identity = (policy, ConfigLoader.get_data_forward_policy().signature,
                    ConfigLoader.get_report_cutoff_policy().latest_day_time, 'period-monthly-v1')
        return hashlib.sha256(repr(identity).encode()).hexdigest()

    @property
    def maintenance_signature(self) -> str:
        return str(self.path.stat().st_mtime_ns) + ':direct-v2'

    @property
    def frames(self) -> dict[str, pd.DataFrame]:
        signature = self.maintenance_signature
        if getattr(self, '_frames_signature', None) != signature:
            frames = read_workbook_sheets(self.path)
            if self.maintenance_signature != signature:
                raise RuntimeError('Maintained workbook changed during read; retry')
            self._frames_signature, self._frames = signature, frames
        return self._frames

    def _read(self, sql: str, params: dict, *, orders=False, groups=False) -> pd.DataFrame:
        if self._db is None:
            self._db = DatabaseManager()
        if self._db.engine is None:
            raise RuntimeError('Yield source unavailable')
        query = text(sql)
        for key, enabled in [('orders', orders), ('groups', groups)]:
            if enabled:
                query = query.bindparams(bindparam(key, expanding=True))
        with self._db.engine.connect() as conn:
            conn.execute(text("SET statement_timeout='60s'"))
            if not self._probed:
                # Low-cost metadata, never COUNT(*) as a size probe.
                metadata = pd.read_sql(text("""SELECT c.relname,c.reltuples,c.relpages
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='mdw' AND c.relname IN
                    ('dwt_warehousing_pnl','dws_dft_warehousing_d','dwr_mes_productspec','imp_ct_dft_group')
                    LIMIT 8"""), conn)
                if len(metadata) < 4:
                    raise RuntimeError('Yield source metadata incomplete')
                # EXPLAIN does not execute the bounded query.
                conn.execute(text('EXPLAIN ' + sql).bindparams(
                    *[bindparam(k, expanding=True) for k, v in [('orders', orders), ('groups', groups)] if v]), params).fetchmany(100)
                self._probed = True
            result = pd.read_sql(query, conn, params=params)
        if len(result) >= 5001:
            raise ValueError('Yield source result exceeds bounded catalog size')
        return result

    @cached_property
    def catalog(self) -> pd.DataFrame:
        groups = sorted({g for p in ConfigLoader.get_enabled_products()
                         for g in ConfigLoader.load_config(p).data_source.target_defect_groups})
        frame = self._read('SELECT DISTINCT defect_group,defect_desc,defect_code FROM imp_ct_dft_group WHERE defect_group IN :groups LIMIT 5001', {'groups': groups}, groups=True)
        # Some descriptions have historical code aliases. Keep one rate and show
        # every actual code explicitly instead of duplicating the maintained rate.
        return frame.groupby(['defect_group', 'defect_desc'], as_index=False).defect_code.agg(lambda s: ' / '.join(sorted(set(s.dropna().astype(str)))))

    def _params(self, product: str, month: str, now: datetime) -> dict:
        if product not in ConfigLoader.get_enabled_products():
            raise ValueError('Unknown product')
        period = pd.Period(month, freq='M')
        if str(period) != month:
            raise ValueError('Invalid month')
        orders = ConfigLoader.load_config(product).data_source.work_order_types
        if not orders:
            raise ValueError('Empty work order scope')
        policy = ConfigLoader.get_data_forward_policy()
        start, end = policy.to_source_window(period.start_time, min(period.end_time, pd.Timestamp(now).normalize()))
        boundary = ConfigLoader.get_report_cutoff_policy().boundary(now) - pd.Timedelta(days=policy.effective_days)
        return dict(product=product, orders=orders, start=start.strftime('%Y%m%d'), end=end.strftime('%Y%m%d'),
                    today=boundary.strftime('%Y%m%d'), cutoff=boundary.strftime('%Y%m%d%H%M%S') + '000000')

    def throughput(self, product: str, month: str, now: datetime) -> int:
        sql = 'SELECT COUNT(DISTINCT dwp.panel_id) AS total_panels ' + BASE.format(joins='', extra='')
        return int(self._read(sql, self._params(product, month, now), orders=True).total_panels.iloc[0])

    def raw_rates(self, product: str, month: str, total: int, now: datetime) -> pd.DataFrame:
        params = self._params(product, month, now)
        params['groups'] = ConfigLoader.load_config(product).data_source.target_defect_groups
        joins = 'JOIN dws_dft_warehousing_d dd ON dwp.panel_id=dd.panel_id JOIN imp_ct_dft_group g ON dd.defect_code=g.defect_code'
        sql = 'SELECT g.defect_group,g.defect_desc,COUNT(DISTINCT dwp.panel_id) AS defect_panel_count ' + BASE.format(joins=joins, extra='AND g.defect_group IN :groups') + ' GROUP BY g.defect_group,g.defect_desc LIMIT 5001'
        counts = self._read(sql, params, orders=True, groups=True) if total else pd.DataFrame(columns=['defect_group', 'defect_desc', 'defect_panel_count'])
        catalog = self.catalog.loc[self.catalog.defect_group.isin(params['groups'])]
        counts = catalog.merge(counts, on=['defect_group', 'defect_desc'], how='left')
        counts['defect_panel_count'] = pd.to_numeric(counts.defect_panel_count, errors='raise').fillna(0)
        from src.yield_domain.core.mwd_trend.mwd_trend_processor import MWDTrendProcessor
        return MWDTrendProcessor.create_period_monthly_rates(counts, total, month,
            ConfigLoader.load_config(product).processing.get('defect_multipliers'))

    def maintained_rates(self, product: str, month: str) -> pd.DataFrame:
        group = self.frames.get(f'{product}_Group级', pd.DataFrame())
        code = self.frames.get(f'{product}_Code级', pd.DataFrame())
        rows = []
        for level, frame in [('Group', group), ('Code', code)]:
            if frame.empty:
                raise ValueError('Maintained product sheet missing')
            from src.yield_domain.core.mwd_trend.modifier_table import _validate_rate_values
            _validate_rate_values(frame, product_code=product, sheet_name=level)
            monthly = frame.loc[frame['周期类型'].eq('月度')].copy()
            monthly['_month'] = monthly['时间标签'].astype(str).str.extract(r'(\d{4}-\d{2})', expand=False)
            exists = monthly['_month'].eq(month).any()
            if monthly.duplicated(['不良类型', '_month']).any():
                raise ValueError('Duplicate maintained monthly rate')
            if level == 'Code':
                targets = resolve_monthly_targets(monthly, [month])
                for desc in monthly['不良类型'].dropna().unique():
                    matches = self.catalog.loc[self.catalog.defect_desc.eq(desc)]
                    if len(matches) != 1:
                        resolved = targets.get(desc, {})
                        if len(matches) == 0 and resolved and all(v == 0 for v in resolved.values()):
                            logging.getLogger(__name__).warning('Ignoring zero-only orphan maintained Code: %s', desc)
                            continue
                        raise ValueError(f'Maintained Code mapping is not unique: {desc}')
                    item = matches.iloc[0]
                    value = targets.get(desc, {}).get(month, np.nan) if exists else np.nan
                    rows.append([month, level, item.defect_group, item.defect_code, desc, value])
            else:
                from src.yield_domain.core.mwd_trend.modifier_table import parse_rate_value
                for name in monthly['不良类型'].dropna().unique():
                    selected = monthly.loc[monthly['_month'].eq(month) & monthly['不良类型'].eq(name)]
                    value = np.nan
                    if not selected.empty:
                        item = selected.iloc[0]
                        specified = parse_rate_value(item['指定良损'])
                        value = specified if specified is not None else parse_rate_value(item['当月良损'])
                    rows.append([month, level, name, '', '', value])
        result = pd.DataFrame(rows, columns=RATE_COLUMNS)
        values = result.defect_rate.dropna()
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError('Invalid maintained rate')
        return result
