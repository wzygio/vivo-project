import os
from types import SimpleNamespace

import yaml

from app.manager import session_manager
from app.manager.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.yield_domain.application.dtos import YieldDataPolicy


def test_yield_data_policy_is_defined_once_in_global_config() -> None:
    config_dir = ConfigLoader.get_project_root() / "config"
    global_config = yaml.safe_load(
        (config_dir / "global.yaml").read_text(encoding="utf-8")
    )

    expected_groups = ["Array_Line", "Array_Pixel", "Array_Mura", "OLED_Mura"]
    expected_work_orders = ["ESLC", "P", "LCFG", "SLCFG", "SYZLC"]
    assert global_config["data_source"]["target_defect_groups"] == expected_groups
    assert global_config["data_source"]["work_order_types"] == expected_work_orders

    product_paths = sorted((config_dir / "products").glob("*.yaml"))
    assert product_paths
    for product_path in product_paths:
        product_config = yaml.safe_load(product_path.read_text(encoding="utf-8"))
        product_data_source = product_config["data_source"]
        assert "target_defect_groups" not in product_data_source
        assert "work_order_types" not in product_data_source

        loaded = ConfigLoader.load_config(product_path.stem)
        assert loaded.data_source.target_defect_groups == expected_groups
        assert loaded.data_source.work_order_types == expected_work_orders


def test_active_config_reloads_when_global_config_changes(
    tmp_path, monkeypatch
) -> None:
    config_dir = tmp_path / "config"
    product_dir = config_dir / "products"
    product_dir.mkdir(parents=True)
    global_path = config_dir / "global.yaml"
    product_path = product_dir / "M678.yaml"

    def write_global(groups: list[str]) -> None:
        global_path.write_text(
            yaml.safe_dump(
                {
                    "application": {"cache_ttl_hours": 4},
                    "data_source": {
                        "target_defect_groups": groups,
                        "work_order_types": ["ESLC", "P"],
                    },
                    "ui": {"icons": {}},
                },
                allow_unicode=True,
            ),
            encoding="utf-8",
        )

    write_global(["Array_Line"])
    product_path.write_text(
        yaml.safe_dump({"data_source": {"product_code": "M678"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(ConfigLoader, "get_project_root", lambda: tmp_path)
    fake_streamlit = SimpleNamespace(
        session_state={SessionManager.KEY_PRODUCT: "M678"}
    )
    monkeypatch.setattr(session_manager, "st", fake_streamlit)

    first = SessionManager.get_active_config()
    assert first.data_source.target_defect_groups == ["Array_Line"]

    previous_version = fake_streamlit.session_state[SessionManager.KEY_CONFIG_VERSION]
    write_global(["OLED_Mura"])
    newer_mtime = previous_version[0] + 10_000_000_000
    os.utime(global_path, ns=(newer_mtime, newer_mtime))

    reloaded = SessionManager.get_active_config()
    assert reloaded.data_source.target_defect_groups == ["OLED_Mura"]


def test_yield_data_policy_is_built_once_from_validated_app_config() -> None:
    config = ConfigLoader.load_config("M678")

    policy = YieldDataPolicy.from_app_config(config)

    assert policy.work_order_types == ("ESLC", "P", "LCFG", "SLCFG", "SYZLC")
    assert policy.target_defect_groups == (
        "Array_Line",
        "Array_Pixel",
        "Array_Mura",
        "OLED_Mura",
    )
