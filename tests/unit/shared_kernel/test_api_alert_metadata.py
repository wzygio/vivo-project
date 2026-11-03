"""Public metadata behavior using isolated, editable workbooks."""

from pathlib import Path
from contextlib import contextmanager

import pandas as pd
import pytest

from src.shared_kernel.infrastructure.api_alert_metadata import ApiAlertMetadata, initialize


def test_new_metadata_template_only_requires_station_dictionary(tmp_path: Path) -> None:
    path = tmp_path / 'metadata.xlsx'
    assert initialize(path)
    assert pd.ExcelFile(path).sheet_names == ['站点字典']


def test_reading_metadata_preserves_identity_and_does_not_modify_workbook(tmp_path: Path) -> None:
    path = tmp_path / "metadata.xlsx"
    assert initialize(path) is True
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        pd.DataFrame([{"step_id": "00100", "description": "PI2_CURE_THK"}]).to_excel(
            writer, sheet_name="站点字典", index=False,
        )
    before = path.read_bytes()
    metadata = ApiAlertMetadata(path)
    assert metadata.enrich("SPC", "P1", "00001", "THK", "ARRAY", "00100") == {
        "factory": "ARRAY", "step_id": "00100", "layer": "PI2",
    }
    assert initialize(path) is False
    assert path.read_bytes() == before


def _write(path: Path, stations: list[dict], attributions: list[dict] | None = None) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import STATION_COLUMNS

    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(stations, columns=STATION_COLUMNS).to_excel(writer, sheet_name="站点字典", index=False)
        if attributions:
            pd.DataFrame(attributions).to_excel(writer, sheet_name="Yield归属", index=False)


def test_legacy_yield_attribution_does_not_override_station_facts(tmp_path: Path) -> None:
    path = tmp_path / "metadata.xlsx"
    _write(path, [{'step_id': '001', 'description': 'PI2_CURE'}], [
        dict(product="P1", lot_id="", parameter_name="CODE", factory="ARRAY", step_id="", description="BASE_default"),
        dict(product="P1", lot_id="0001", parameter_name="CODE", factory="CELL", step_id="", description="CELL_review"),
    ])
    metadata = ApiAlertMetadata(path)
    assert metadata.enrich("SPC", "P1", "0001", "CODE", 'OLED', '001') == {
        "factory": "OLED", "step_id": '001', "layer": "PI2",
    }


@pytest.mark.parametrize("description", ['', '_CURE', '   '])
def test_station_sources_still_require_nonempty_layer(
    tmp_path: Path, description: str,
) -> None:
    path = tmp_path / "metadata.xlsx"
    _write(path, [{'step_id': '001', 'description': description}])
    with pytest.raises(ValueError):
        ApiAlertMetadata(path).enrich("CTQ", "P1", "L1", "CODE", 'ARRAY', '001')


def test_station_dictionary_is_authoritative_and_conflicts_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "metadata.xlsx"
    _write(path, [{"step_id": "001", "description": "FIRST_CURE"}], [
        dict(product="P1", lot_id="L1", parameter_name="CODE", factory="ARRAY", step_id="001", description="WRONG_value"),
    ])
    assert ApiAlertMetadata(path).enrich("SPC", "P1", "L1", "CODE", 'ARRAY', '001')["layer"] == "FIRST"
    _write(path, [{"step_id": "001", "description": "FIRST_CURE"}, {"step_id": "001", "description": "SECOND_CURE"}])
    with pytest.raises(ValueError, match="AMBIGUOUS"):
        ApiAlertMetadata(path).enrich("SPC", "P1", "S1", "THK", "ARRAY", "001")


@pytest.mark.parametrize("factory,step", [("", "001"), ("ARRAY", ""), ("ARRAY", "missing")])
def test_inline_requires_factory_and_known_station(tmp_path: Path, factory: str, step: str) -> None:
    path = tmp_path / "metadata.xlsx"
    _write(path, [{"step_id": "001", "description": "PI2_CURE"}])
    with pytest.raises(ValueError):
        ApiAlertMetadata(path).enrich("SPC", "P1", "S1", "THK", factory, step)


class _Rows:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def all(self) -> list[dict]:
        return self.rows


class FakeEngine:
    def __init__(self, rows: list[dict], *, fail_view: bool = False) -> None:
        self.rows = rows
        self.fail_view = fail_view
        self.calls: list[tuple] = []
        self.transactions = 0

    @contextmanager
    def begin(self):
        self.transactions += 1
        yield self

    def execute(self, statement, parameters):
        from sqlalchemy.exc import SQLAlchemyError

        sql = str(statement)
        self.calls.append((self.transactions, sql, parameters))
        if "set_config" in sql:
            return _Rows([])
        if self.fail_view and "processoperationspec_v" in sql:
            raise SQLAlchemyError("fixture unavailable view")
        return _Rows(self.rows)


def test_station_sync_ignores_and_preserves_unrecognised_legacy_yield_sheet(tmp_path: Path) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata
    path = tmp_path / 'metadata.xlsx'
    initialize(path)
    legacy = pd.DataFrame({'old operator notes': ['keep this maintained information']})
    with pd.ExcelWriter(path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        legacy.to_excel(writer, sheet_name='Yield归属', index=False)
    assert sync_metadata(path, ['001'], FakeEngine([{'step_id': '001', 'description': 'PI2_CURE'}])) == 1
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name='Yield归属'), legacy)


def test_bounded_sync_falls_back_in_fresh_transaction_and_preserves_manual_data(tmp_path: Path) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata

    path = tmp_path / "metadata.xlsx"
    attributions = [dict(product="P1", lot_id="L1", parameter_name="CODE", factory="CELL", step_id="", description="CELL_REVIEW")]
    _write(path, [{"step_id": "old", "description": "OLD_keep"}], attributions)
    before_manual = pd.read_excel(path, sheet_name="Yield归属", dtype=str).fillna("")
    engine = FakeEngine([{"step_id": "00100", "description": "PI2_CURE"}], fail_view=True)
    assert sync_metadata(path, ["00100"], engine, max_rows=10, timeout_ms=2500) == 1
    assert engine.transactions == 2
    queries = [call for call in engine.calls if "set_config" not in call[1]]
    assert len(queries) == 2 and queries[0][0] != queries[1][0]
    for _, sql, parameters in queries:
        assert "WHERE oper_code IN" in sql and "LIMIT :row_limit" in sql
        assert parameters == {"step_ids": ["00100"], "row_limit": 11}
        assert "00100" not in sql
    assert all(call[2] == {"timeout": "2500ms"} for call in engine.calls if "set_config" in call[1])
    pd.testing.assert_frame_equal(before_manual, pd.read_excel(path, sheet_name="Yield归属", dtype=str).fillna(""))
    saved = pd.read_excel(path, sheet_name="站点字典", dtype=str)
    assert set(saved["step_id"]) == {"old", "00100"}
    assert ApiAlertMetadata(path).enrich("SPC", "P1", "S1", "THK", "ARRAY", "00100")["layer"] == "PI2"


@pytest.mark.parametrize("steps,rows,cap", [
    ([], [], 10),
    (["1"] * 1001, [], 10),
    (["1"], [], 10),
    (["1"], [{"step_id": "2", "description": "OTHER_desc"}], 10),
    (["1"], [{"step_id": "1", "description": "A_desc"}, {"step_id": "1", "description": "B_desc"}], 10),
    (["1"], [{"step_id": "1", "description": "A_desc"}] * 2, 1),
])
def test_invalid_or_incomplete_sync_preserves_previous_bytes(tmp_path: Path, steps, rows, cap) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata

    path = tmp_path / "metadata.xlsx"
    _write(path, [{"step_id": "old", "description": "OLD_keep"}])
    previous = path.read_bytes()
    with pytest.raises(ValueError):
        sync_metadata(path, steps, FakeEngine(rows), max_rows=cap)
    assert path.read_bytes() == previous


def test_identical_station_rows_are_deduplicated_and_plain_description_is_layer(tmp_path: Path) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata

    path = tmp_path / "metadata.xlsx"
    initialize(path)
    engine = FakeEngine([{"step_id": "001", "description": "PI2"}] * 2)
    assert sync_metadata(path, ["001"], engine) == 1
    assert ApiAlertMetadata(path).enrich("CTQ", "P1", "S1", "THK", "ARRAY", "001")["layer"] == "PI2"


def test_ambiguous_obsolete_yield_rows_do_not_invalidate_stations(tmp_path: Path) -> None:
    path = tmp_path / "metadata.xlsx"
    row = dict(product="P1", lot_id="", parameter_name="CODE", factory="ARRAY", step_id="", description="PI2_CURE")
    _write(path, [{'step_id': '001', 'description': 'REAL_LAYER'}], [row, row])
    assert ApiAlertMetadata(path).enrich("SPC", "P1", "L1", "CODE", 'ARRAY', '001')['layer'] == 'REAL'


def test_read_missing_file_never_creates_it_and_injected_reader_is_used(tmp_path: Path) -> None:
    path = tmp_path / "metadata.xlsx"
    with pytest.raises(FileNotFoundError):
        ApiAlertMetadata(path).enrich("SPC", "P1", "S1", "THK", "ARRAY", "001")
    assert not path.exists()
    path.write_bytes(b"fixture contents are handled by the injected reader")
    calls = []
    def reader(source, mtime, size):
        calls.append((source, mtime, size))
        return {"站点字典": pd.DataFrame([{"step_id": "001", "description": "PI2_CURE"}])}
    assert ApiAlertMetadata(path, workbook_reader=reader).enrich("SPC", "P1", "S1", "THK", "ARRAY", "001")["step_id"] == "001"
    assert calls == [(str(path.resolve()), path.stat().st_mtime_ns, path.stat().st_size)]


def test_sync_publication_failure_keeps_previous_bytes(tmp_path: Path, monkeypatch) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata
    from src.shared_kernel.utils import excel_tools

    path = tmp_path / "metadata.xlsx"
    initialize(path)
    previous = path.read_bytes()
    monkeypatch.setattr(excel_tools, "replace_workbook_sheets", lambda *args: type("Failure", (), {"written": False})())
    with pytest.raises(OSError, match="PUBLICATION_FAILED"):
        sync_metadata(path, ["001"], FakeEngine([{"step_id": "001", "description": "PI2_CURE"}]))
    assert path.read_bytes() == previous


def test_source_failures_are_safe_and_do_not_change_metadata(tmp_path: Path) -> None:
    from sqlalchemy.exc import SQLAlchemyError
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata

    class BrokenEngine(FakeEngine):
        def execute(self, statement, parameters):
            if "set_config" not in str(statement):
                raise SQLAlchemyError("fixture internal connection details")
            return super().execute(statement, parameters)

    path = tmp_path / "metadata.xlsx"
    initialize(path)
    before = path.read_bytes()
    engine = BrokenEngine([])
    with pytest.raises(OSError, match="STATION_DICTIONARY_UNAVAILABLE"):
        sync_metadata(path, ["001"], engine)
    assert engine.transactions == 2
    assert path.read_bytes() == before


def test_formula_like_source_values_are_not_published(tmp_path: Path) -> None:
    from src.shared_kernel.infrastructure.api_alert_metadata import sync_metadata

    path = tmp_path / "metadata.xlsx"
    initialize(path)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="FORMULA_INVALID"):
        sync_metadata(path, ["001"], FakeEngine([{"step_id": "001", "description": "=HYPERLINK(\"fixture\")"}]))
    assert path.read_bytes() == before


def test_cli_offline_template_never_constructs_database_and_preserves_existing(tmp_path: Path, monkeypatch, capsys) -> None:
    import json
    from src.shared_kernel.infrastructure import db_handler
    from tools.sync_api_alert_metadata import main

    def forbidden():
        raise AssertionError("Database must not be created for initialization")
    monkeypatch.setattr(db_handler, "DatabaseManager", forbidden)
    path = tmp_path / "metadata.xlsx"
    assert main(["--template-only"], metadata_path=path) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["created"] is True
    assert first["initialized"] is True
    assert first["stations_total"] == 0
    assert first["dictionary_state"] == "empty"
    assert first["message"]
    before = path.read_bytes()
    assert main(["--template-only"], metadata_path=path) == 0
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["created"] is False
    assert repeated["initialized"] is True
    assert repeated["stations_total"] == 0
    assert repeated["dictionary_state"] == "empty"
    assert repeated["message"] != first["message"]
    assert path.read_bytes() == before


def test_cli_initialization_reports_populated_dictionary_without_rewriting(tmp_path: Path, capsys) -> None:
    import json
    from tools.sync_api_alert_metadata import main

    path = tmp_path / 'metadata.xlsx'
    _write(path, [{'step_id': '00100', 'description': 'PI2_CURE'}], [
        dict(product='P1', lot_id='L1', parameter_name='CODE', factory='ARRAY', step_id='', description='old'),
    ])
    before = path.read_bytes()
    assert main(['--template-only'], metadata_path=path) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['initialized'] is True
    assert payload['created'] is False
    assert payload['stations_total'] == 1
    assert payload['dictionary_state'] == 'populated'
    assert path.read_bytes() == before


@pytest.mark.parametrize('stations', [
    [{'wrong_column': '00100'}],
    [{'step_id': '00100', 'description': ''}],
    [{'step_id': '00100', 'description': 'PI2_CURE'}, {'step_id': '00100', 'description': 'PI3_CURE'}],
])
def test_cli_initialization_rejects_invalid_existing_dictionary_without_database_or_writes(
    tmp_path: Path, monkeypatch, capsys, stations,
) -> None:
    import json
    from src.shared_kernel.infrastructure import db_handler
    from tools.sync_api_alert_metadata import main

    def forbidden():
        raise AssertionError('Initialization must not access database')

    monkeypatch.setattr(db_handler, 'DatabaseManager', forbidden)
    path = tmp_path / 'metadata.xlsx'
    pd.DataFrame(stations).to_excel(path, sheet_name='站点字典', index=False)
    before = path.read_bytes()
    assert main(['--template-only'], metadata_path=path) == 2
    assert json.loads(capsys.readouterr().out)['status'] == 'error'
    assert path.read_bytes() == before


def test_cli_rejects_combining_full_initialization_with_partial_selection(tmp_path: Path) -> None:
    from tools.sync_api_alert_metadata import main

    path = tmp_path / 'metadata.xlsx'
    with pytest.raises(SystemExit) as error:
        main(['--initialize', '--step', '00100'], metadata_path=path,
             engine=FakeEngine([{'step_id': '00100', 'description': 'PI2_CURE'}]))
    assert error.value.code == 2
    assert not path.exists()


def test_cli_entry_outputs_utf8_json_even_with_legacy_pipe_encoding(tmp_path: Path) -> None:
    import json
    import os
    import subprocess
    import sys

    path = tmp_path / 'metadata.xlsx'
    code = '''import runpy, sys
from pathlib import Path
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler
metadata_path = Path(sys.argv[1])
ConfigLoader.get_domain_resource_path = staticmethod(lambda *_: metadata_path)
def forbidden():
    raise AssertionError("Initialization must not construct database")
db_handler.DatabaseManager = forbidden
sys.argv = ["sync_api_alert_metadata", "--template-only"]
runpy.run_module("tools.sync_api_alert_metadata", run_name="__main__")
'''
    result = subprocess.run([sys.executable, '-c', code, str(path)],
                            cwd=Path(__file__).resolve().parents[3],
                            env=dict(os.environ, PYTHONIOENCODING='ascii'),
                            capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr.decode('utf-8', errors='replace')
    payload = json.loads(result.stdout.decode('utf-8'))
    assert payload['initialized'] is True
    assert payload['message'].startswith('站点字典已新建')
    assert path.exists()


def test_cli_resolves_registry_and_uses_explicit_engine(tmp_path: Path, monkeypatch, capsys) -> None:
    import json
    from src.shared_kernel.config import ConfigLoader
    from tools.sync_api_alert_metadata import main

    path = tmp_path / "metadata.xlsx"
    initialize(path)
    calls = []
    def registered(domain, name):
        calls.append((domain, name))
        return path
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", registered)
    assert main(["--step", "001"], engine=FakeEngine([{"step_id": "001", "description": "PI2_CURE"}])) == 0
    assert calls == [("shared_kernel", "api_alert_metadata")]
    assert json.loads(capsys.readouterr().out) == {"status": "success", "created": False, "stations_updated": 1}


@pytest.mark.parametrize("arguments", [[], ["--step", ""], ["--step", "001", "--max-rows", "0"], ["--step", "001", "--timeout-ms", "60001"]])
def test_cli_rejects_unbounded_or_invalid_request(tmp_path: Path, arguments) -> None:
    from tools.sync_api_alert_metadata import main

    path = tmp_path / "metadata.xlsx"
    with pytest.raises(SystemExit) as result:
        main(arguments, metadata_path=path)
    assert result.value.code == 2
    assert not path.exists()


def test_cli_does_not_query_database_if_template_is_missing(tmp_path: Path, monkeypatch, capsys) -> None:
    import json
    from src.shared_kernel.infrastructure import db_handler
    from tools.sync_api_alert_metadata import main

    def forbidden():
        raise AssertionError("Database must not be created without an initialized target")
    monkeypatch.setattr(db_handler, "DatabaseManager", forbidden)
    assert main(["--step", "001"], metadata_path=tmp_path / "missing.xlsx") == 2
    assert json.loads(capsys.readouterr().out) == {"status": "error", "code": "API_METADATA_MAINTENANCE_FAILED"}
