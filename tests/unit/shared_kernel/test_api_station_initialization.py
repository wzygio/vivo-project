"""Full dictionary initialization uses plans and capped reads, never HTTP."""
from contextlib import contextmanager
import json
from pathlib import Path

import pandas as pd
import pytest

from src.shared_kernel.infrastructure.api_alert_metadata import initialize, initialize_from_database


class Result:
    def __init__(self, rows):
        self.rows = rows

    def scalar_one(self):
        return self.rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class DictionaryEngine:
    def __init__(self, rows=None, *, fail_view=False, fail_all=False, estimate=2, scan_rows=2,
                 physical_rows=2, physical_bytes=16384):
        self.rows = rows if rows is not None else [
            {'step_id': '00100', 'description': 'PI2_CURE_THK'},
            {'step_id': '00200', 'description': 'METAL'},
        ]
        self.fail_view, self.fail_all = fail_view, fail_all
        self.estimate, self.scan_rows = estimate, scan_rows
        self.physical_rows, self.physical_bytes = physical_rows, physical_bytes
        self.calls = []
        self.transaction = 0

    @contextmanager
    def begin(self):
        self.transaction += 1
        yield self

    def execute(self, statement, parameters=None):
        from sqlalchemy.exc import SQLAlchemyError

        sql = str(statement)
        self.calls.append((self.transaction, sql, parameters))
        if sql.startswith('SET ') or 'set_config' in sql:
            return Result([])
        if self.fail_all or (self.fail_view and 'processoperationspec_v' in sql):
            raise SQLAlchemyError('offline missing dictionary relation')
        if sql.startswith('EXPLAIN'):
            return Result([{'Plan': {'Plan Rows': self.estimate, 'Total Cost': 10,
                                    'Plans': [{'Schema': 'mdw', 'Relation Name': 'dictionary', 'Plan Rows': self.scan_rows,
                                               'Plan Width': 32}]}}])
        if 'FROM pg_class' in sql:
            return Result([{'estimated_rows': self.physical_rows, 'estimated_bytes': self.physical_bytes}])
        return Result(self.rows)


def test_cli_initialize_fetches_all_stations_and_materializes_layers(tmp_path: Path, capsys):
    from tools.sync_api_alert_metadata import main

    path = tmp_path / 'metadata.xlsx'
    engine = DictionaryEngine()
    assert main(['--initialize'], metadata_path=path, engine=engine) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['stations_updated'] == payload['stations_total'] == 2
    assert payload['dictionary_state'] == 'populated'
    rows = pd.read_excel(path, sheet_name='站点字典', dtype=str)
    assert rows.to_dict('records') == [
        {'step_id': '00100', 'description': 'PI2_CURE_THK', 'layer': 'PI2'},
        {'step_id': '00200', 'description': 'METAL', 'layer': 'METAL'},
    ]
    plan_index = next(i for i, c in enumerate(engine.calls) if c[1].startswith('EXPLAIN'))
    read_index = next(i for i, c in enumerate(engine.calls) if 'LIMIT :row_limit' in c[1])
    assert plan_index < read_index
    assert engine.calls[read_index][2] == {'row_limit': 10001}
    assert all('ANALYZE' not in c[1] and 'COUNT(' not in c[1] for c in engine.calls)


def test_full_initialization_preserves_history_and_unrelated_sheet(tmp_path: Path):
    path = tmp_path / 'metadata.xlsx'
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame([{'step_id': 'old', 'description': 'OLD_KEEP'}]).to_excel(writer, sheet_name='站点字典', index=False)
        pd.DataFrame({'notes': ['keep']}).to_excel(writer, sheet_name='Yield归属', index=False)
    created, count = initialize_from_database(path, DictionaryEngine())
    assert created is False and count == 2
    assert len(pd.read_excel(path, sheet_name='站点字典')) == 3
    assert pd.read_excel(path, sheet_name='Yield归属').iloc[0, 0] == 'keep'
    assert path.with_suffix('.xlsx.bak').exists()


def test_full_initialization_falls_back_with_fresh_transaction(tmp_path: Path):
    engine = DictionaryEngine(fail_view=True)
    assert initialize_from_database(tmp_path / 'metadata.xlsx', engine) == (True, 2)
    assert engine.transaction == 2


@pytest.mark.parametrize('engine', [
    DictionaryEngine(fail_all=True), DictionaryEngine(estimate=10001),
    DictionaryEngine(scan_rows=100001), DictionaryEngine(rows=[]),
    DictionaryEngine(rows=[{'step_id': '1', 'description': 'A_X'}, {'step_id': '1', 'description': 'B_X'}]),
    DictionaryEngine(rows=[{'step_id': '1', 'description': '=BAD'}]),
    DictionaryEngine(rows=[{'step_id': str(i), 'description': 'A_X'} for i in range(11)], estimate=1),
])
def test_failed_full_read_never_replaces_existing_or_creates_empty_target(tmp_path: Path, engine):
    for existed in (False, True):
        path = tmp_path / f'{existed}.xlsx'
        if existed:
            initialize(path)
        before = path.read_bytes() if existed else None
        with pytest.raises((ValueError, OSError)):
            initialize_from_database(path, engine, max_rows=10)
        assert (path.read_bytes() if path.exists() else None) == before
    if engine.estimate > 10 or engine.scan_rows > 100000:
        assert not any('LIMIT :row_limit' in c[1] for c in engine.calls)


def test_full_initialization_keeps_source_layer_gaps_but_api_rejects_affected_record(tmp_path: Path, capsys):
    from tools.sync_api_alert_metadata import main
    from src.shared_kernel.infrastructure.api_alert_metadata import ApiAlertMetadata

    path = tmp_path / 'metadata.xlsx'
    engine = DictionaryEngine(rows=[{'step_id': '1A455', 'description': '_DI_CDOL'}], estimate=1)
    assert main(['--initialize'], metadata_path=path, engine=engine) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['stations_total'] == 1
    assert payload['layers_complete'] is False
    assert payload['stations_without_layer'] == ['1A455']
    row = pd.read_excel(path, sheet_name='站点字典', dtype=str, keep_default_na=False).iloc[0]
    assert row['description'] == '_DI_CDOL' and row['layer'] == ''
    with pytest.raises(ValueError, match='ALERT_LAYER_UNAVAILABLE'):
        ApiAlertMetadata(path).enrich('SPC', 'P1', 'S1', 'param', 'ARRAY', '1A455')


@pytest.mark.parametrize('physical_rows,physical_bytes', [
    (500000, 16384), (-1, 16384), (2, 64 * 1024 * 1024 + 1),
])
def test_full_initialization_checks_underlying_table_even_when_view_output_is_small(
    tmp_path: Path, physical_rows, physical_bytes,
):
    engine = DictionaryEngine(estimate=2, scan_rows=2,
                              physical_rows=physical_rows, physical_bytes=physical_bytes)
    path = tmp_path / 'metadata.xlsx'
    with pytest.raises(ValueError, match='SCALE_'):
        initialize_from_database(path, engine)
    assert not path.exists()
    assert not any('LIMIT :row_limit' in c[1] for c in engine.calls)
