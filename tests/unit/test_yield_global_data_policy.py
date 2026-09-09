import os
from types import SimpleNamespace

import yaml

from app.manager import session_manager
from app.manager.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader


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
