from pathlib import Path

import pandas as pd

from src.shared_kernel.config_model import AppConfig
from src.yield_domain.application.excel_service import ExcelService


def _write_mapping_config(path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "启用": True,
                "产品型号": "M626",
                "Defect Code": "彩斑Mura",
                "蒸镀批次": "26/04/29",
                "修饰模式": "multiplicative",
                "热点倍率": 0.3,
                "普通倍率": 1.5,
                "规则": "row",
                "膜位": "2E,1A",
            },
            {
                "启用": True,
                "产品型号": "M678",
                "Defect Code": "S向单暗线",
                "蒸镀批次": "26/05/25",
                "修饰模式": "additive",
                "热点加值": 2,
                "加值模式普通倍率": 0,
                "规则": "position",
                "膜位": "1A:J0,1B:K0",
            },
            {
                "启用": True,
                "产品型号": "M678",
                "Defect Code": "S向单暗线",
                "蒸镀批次": "26/05/25",
                "修饰模式": "additive",
                "热点加值": 2,
                "加值模式普通倍率": 0,
                "规则": "col",
                "膜位": "A0,B0,S0",
            },
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Mapping修饰")


def test_parse_mapping_config_excel_supports_product_code_batch_and_custom_rules(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mapping_config.xlsx"
    _write_mapping_config(config_path)

    scripts = ExcelService.parse_mapping_config_excel(config_path, product_code="M626")

    assert scripts == [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "multiplicative",
            "hotspot_multiplier": 0.3,
            "normal_multiplier": 1.5,
            "hotspot_rules": [{"type": "row", "value": ["2E", "1A"]}],
        }
    ]


def test_inject_mapping_config_replaces_yaml_scripts_for_active_product(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mapping_config.xlsx"
    _write_mapping_config(config_path)
    config = AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "M678"},
            "processing": {"mapping_hotspot_script": [{"target_code": "旧配置"}]},
        }
    )

    ExcelService.inject_mapping_config_to_config(config, config_path)

    assert config.processing["mapping_hotspot_script"] == [
        {
            "enable": True,
            "target_product": "M678",
            "target_code": "S向单暗线",
            "target_batch": "2026/05/25",
            "mode": "additive",
            "hotspot_adder": 2,
            "normal_multiplier_in_add_mode": 0,
            "hotspot_rules": [
                {"type": "position", "value": [["1A", "J0"], ["1B", "K0"]]},
                {"type": "col", "value": ["A0", "B0", "S0"]},
            ],
        }
    ]


def test_parse_mapping_config_ignores_legacy_batch_position_field(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mapping_config.xlsx"
    df = pd.DataFrame(
        [
            {
                "启用": True,
                "产品型号": "M626",
                "Defect Code": "白画面黑斑Mura",
                "蒸镀批次": "26/05/14",
                "批次位置": 0,
                "修饰模式": "multiplicative",
                "热点倍率": 0.3,
                "普通倍率": 1,
                "规则": "position",
                "膜位": "1A:A0,1B:B0",
            }
        ]
    )
    with pd.ExcelWriter(config_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Mapping修饰")

    scripts = ExcelService.parse_mapping_config_excel(config_path, product_code="M626")

    assert scripts == [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "白画面黑斑Mura",
            "target_batch": "2026/05/14",
            "mode": "multiplicative",
            "hotspot_multiplier": 0.3,
            "normal_multiplier": 1,
            "hotspot_rules": [{"type": "position", "value": [["1A", "A0"], ["1B", "B0"]]}],
        }
    ]


def test_parse_mapping_config_falls_back_to_com_when_openpyxl_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "mapping_config.xlsx"
    config_path.write_bytes(b"encrypted-placeholder")

    def fail_read_excel(*args, **kwargs):
        raise ValueError("not a zip file")

    fallback_df = pd.DataFrame(
        [
            {
                "启用": True,
                "产品型号": "M626",
                "Defect Code": "彩斑Mura",
                "蒸镀批次": "26/04/29",
                "修饰模式": "random",
                "随机方法": "poisson",
                "随机波动": 0.2,
                "随机种子": 2026,
            }
        ]
    )

    monkeypatch.setattr(pd, "read_excel", fail_read_excel)
    monkeypatch.setattr(
        ExcelService,
        "_read_mapping_config_via_com",
        staticmethod(lambda excel_path: fallback_df),
    )

    scripts = ExcelService.parse_mapping_config_excel(config_path, product_code="M626")

    assert scripts == [
        {
            "enable": True,
            "target_product": "M626",
            "target_code": "彩斑Mura",
            "target_batch": "2026/04/29",
            "mode": "random",
            "random_seed": 2026,
            "random_variation": 0.2,
            "random_method": "poisson",
        }
    ]
