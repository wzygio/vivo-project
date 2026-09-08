from __future__ import annotations

from src.inline_domain.infrastructure.shared.resource_paths import scope_decoration_path
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.shared import resource_paths


def test_all_alarm_workbooks_resolve_to_module_subdirectories() -> None:
    root = ConfigLoader.get_project_root()
    for scope in ("spc", "ctq", "aoi_tt", "aoi_rs"):
        for alarm_type in ("oos", "ooc"):
            path = scope_decoration_path(scope, alarm_type)
            assert path.parent == root / "resources" / "inline_domain" / scope
            assert path.name == f"{scope}_sheet_{alarm_type}_decoration.xlsx"


def test_non_alarm_inline_resources_resolve_to_owned_subdirectories() -> None:
    expected = {
        "spc_cpk_cpm_decoration": "spc",
        "spc_outlier_filters": "spc",
        "aoi_tt_particle_size_ratio_spec": "aoi_tt",
        "compliance_config": "monitor",
        "scrap_sheets": "monitor",
    }
    for key, folder in expected.items():
        path = ConfigLoader.get_domain_resource_path("inline_domain", key)
        assert path.parent.name == folder


def test_custom_resources_configuration_is_not_forced_back_to_flat_layout(
    monkeypatch, tmp_path
) -> None:
    configured = tmp_path / "custom" / "spc" / "spc_sheet_oos_decoration.xlsx"
    monkeypatch.setattr(
        resource_paths.ConfigLoader,
        "get_domain_resource_path",
        classmethod(lambda _cls, _domain, _key, default_name=None: configured),
    )

    assert scope_decoration_path("spc", "oos") == configured
