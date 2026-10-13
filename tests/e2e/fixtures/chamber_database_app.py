"""Real database adapter over isolated SQL facts; query failures are injectable."""

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parents[3]
for path in (root, root / 'src'):
    sys.path.insert(0, str(path))

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from app.sections.indicator_domain.qtime.chamber_dashboard import render_chamber_dashboard, refresh_chamber_data
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.infrastructure.qtime.chamber_database import DatabaseChamberQTimeRepository
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy


@st.cache_resource
def fixture_engine():
    from sqlalchemy.pool import StaticPool
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    with engine.begin() as c:
        c.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        c.execute(text('''CREATE TABLE mdw.dwt_inout_sub_unit_gls (glass_id TEXT, eqp_id TEXT,
            unit_id TEXT, sub_unit_id TEXT, factory TEXT, prod_type TEXT, inout_type TEXT,
            date_timekey TEXT, event_timekey TEXT, oper_in_time TEXT, oper_out_time TEXT, prod_id TEXT)'''))
        c.execute(text('CREATE TABLE mdw.dwr_mes_productspec (productspecname TEXT, productcode TEXT)'))
        c.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('S1','M626'),('S2','M678'),('OFF','DISABLED_PRODUCT')"))
        for glass, spec, eqp, seconds, day in (
            ('DB_A','S1','3CEE01',1000,1), ('DB_B','S2','3CEE02',120,1),
            ('DB_OUTLIER','S1','3CEE01',1001,1), ('DISABLED_GLASS','OFF','3CEE01',10,1),
            ('DB_NEXT_DAY','S1','3CEE01',40,2),
        ):
            start = pd.Timestamp(f'2026-09-{day:02} 08:00:00')
            for unit, kind, offset in [('BL0-LL1','IN',0), ('BL0-PT1','IN',5),
                                       ('BL0-PT1','OUT',10), ('BL1-C11','IN',10+seconds)]:
                when = start + pd.Timedelta(seconds=offset)
                c.execute(text('''INSERT INTO mdw.dwt_inout_sub_unit_gls VALUES
                    (:glass,:eqp,'UNIT',:unit,'OLED','P',:kind,:day,:key,:when,:when,:spec)'''),
                    {'glass': glass, 'eqp': eqp, 'unit': eqp+'-'+unit, 'kind': kind,
                     'day': when.strftime('%Y%m%d'), 'key': when.strftime('%Y%m%d%H%M%S%f'), 'when': str(when), 'spec': spec})
    return engine


st.set_page_config(layout='wide')
scenario = st.query_params.get('scenario', 'normal')
if scenario == 'live':
    from src.indicator_domain.composition import build_chamber_qtime_service
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    service = build_chamber_qtime_service(DatabaseManager())
    default_day = date(2026, 9, 5)
else:
    source = DatabaseChamberQTimeRepository(
        SimpleNamespace(engine=None if scenario == 'failure' else fixture_engine()),
        target_seconds=1200, data_policy=DataForwardPolicy(), cutoff_policy=ReportCutoffPolicy(),
    )
    service = ChamberQTimeService(source, enabled_products=('M626', 'M678'),
                                  cache_namespace='chamber-database-e2e')
    default_day = date(2026, 9, 3) if scenario == 'empty' else date(2026, 9, 1)
st.session_state.setdefault('chamber_start_date', default_day)
st.session_state.setdefault('chamber_end_date', default_day)
if st.button('刷新单腔缓存', key='fixture_refresh'):
    refresh_chamber_data()
render_chamber_dashboard(service)
