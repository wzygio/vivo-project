"""Latest-per-key source SQL is equivalent to the established pandas projection."""
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine, event, text

from src.equipment_domain.infrastructure import data_loader


def test_query_returns_only_latest_valid_measurements_without_shortening_history(monkeypatch):
    engine = create_engine('sqlite://')
    @event.listens_for(engine, 'connect')
    def dates(connection, _):
        connection.create_function('TO_DATE', 2, lambda value, format: value)
    now = datetime.now().replace(microsecond=0)
    at = lambda days: (now - timedelta(days=days)).isoformat(sep=' ')
    rows = [
        ('A', 'X', 'LIFE', 1.0, at(30)),
        ('A', 'X', 'LIFE', 2.0, at(10)),
        ('A', 'X', 'LIFE', None, at(1)),  # An invalid newer value cannot hide the valid old value.
        ('A', 'X', 'COUNT', 0.0, at(1)),  # Zero is valid.
        ('A', 'Y', 'LIFE', 7.0, at(20)),  # Preserve existing independent IN scopes.
        ('B', 'Y', 'LIFE', -3.0, at(89)),
        ('B', 'Y', 'LIFE', 9.0, at(91)),
        ('B', 'Y', 'OTHER', 100.0, at(1)),
        ('OTHER', 'Y', 'LIFE', 100.0, at(1)),
        # SQLite's flexible REAL affinity represents PostgreSQL NUMERIC NaN as
        # this literal. It must be removed BEFORE choosing this key's newest row.
        ('A', 'X', 'LIFE', 'NaN', at(0)),
    ]
    monkeypatch.setattr(data_loader, 'get_equipment_runtime_config', lambda: SimpleNamespace(
        query_source_table='source', query_lookback_days=90))
    spec = pd.DataFrame({'站点': ['A', 'B'], '机台号-腔室': ['X', 'Y'], '参数名称': ['LIFE%', 'COUNT%']})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql('CREATE TABLE source (step_id TEXT, sub_equip_id TEXT, '
                                      'param_name TEXT, value REAL, glass_start_time TEXT)')
            connection.exec_driver_sql('INSERT INTO source VALUES (?,?,?,?,?)', rows)
        result = data_loader._query_part_life_from_db(SimpleNamespace(engine=engine), spec)
        expected = pd.DataFrame([rows[1], rows[3], rows[4], rows[5]], columns=result.columns)
        expected['glass_start_time'] = pd.to_datetime(expected['glass_start_time'])
        expected = data_loader.latest_part_measurements(expected)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
        assert result.attrs['parts_source_read']['status'] == 'success'
    finally:
        engine.dispose()
