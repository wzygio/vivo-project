from pathlib import Path

import yaml

from src.shared_kernel.config import ConfigLoader


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_application_cache_ttl_is_the_only_cache_ttl_source(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "global.yaml").write_text(
        yaml.safe_dump(
            {
                "application": {"cache_ttl_hours": 7},
                "service_cache": {"ttl_hours": {"inline_ctq_report_payload": 1}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", staticmethod(lambda: tmp_path))

    assert ConfigLoader.get_cache_ttl_seconds() == 7 * 60 * 60
    assert ConfigLoader.get_snapshot_ttl_hours() == 7
    assert (
        ConfigLoader.get_service_cache_ttl_seconds(
            "inline_ctq_report_payload",
            default_hours=1,
        )
        == 7 * 60 * 60
    )


def test_global_yaml_has_no_service_specific_cache_ttl_map() -> None:
    config = yaml.safe_load((PROJECT_ROOT / "config" / "global.yaml").read_text(encoding="utf-8"))
    assert "service_cache" not in config
    assert "data_snapshot" not in config
