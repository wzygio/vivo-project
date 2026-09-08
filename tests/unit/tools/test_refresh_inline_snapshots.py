from pathlib import Path

import pytest

from tools.refresh_inline_snapshots import deletion_plan


def test_plan_only_recognizes_selected_inline_raw_files(tmp_path: Path):
    raw = tmp_path / "inline_domain/shared/inline_measurements_M626.parquet"
    raw.parent.mkdir(parents=True)
    raw.touch()
    other = tmp_path / "equipment_domain/parts/part_life.parquet"
    other.parent.mkdir(parents=True)
    other.touch()
    unknown = raw.parent / "user.xlsx"
    unknown.touch()
    assert deletion_plan(tmp_path, ["M626"]) == [raw]
    assert deletion_plan(tmp_path, ["M678"]) == []


def test_plan_rejects_product_path_traversal(tmp_path: Path):
    with pytest.raises(ValueError):
        deletion_plan(tmp_path, ["../../equipment_domain"])


def test_plan_includes_only_recognized_inline_legacy_families(tmp_path: Path):
    directory = tmp_path / "inline_domain/shared"
    directory.mkdir(parents=True)
    known = directory / "legacy_inline_measurements_M626_abcdef123456.parquet"
    known.touch()
    unknown = directory / "legacy_user_abcdef123456.parquet"
    unknown.touch()
    assert deletion_plan(tmp_path, ["M626"]) == [known]


def test_database_initialization_error_is_sanitized(monkeypatch, capsys):
    from tools.refresh_inline_snapshots import rebuild
    import src.shared_kernel.infrastructure.db_handler as db_handler

    def fail():
        raise RuntimeError("sensitive-placeholder")
    monkeypatch.setattr(db_handler, "DatabaseManager", fail)
    results = rebuild(["M626"], "2026-09-08")
    assert len(results) == 2 and not any(item["success"] for item in results)
    assert "sensitive-placeholder" not in repr(results)
    assert "sensitive-placeholder" not in capsys.readouterr().err
