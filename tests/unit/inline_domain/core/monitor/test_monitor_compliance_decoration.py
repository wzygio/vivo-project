import pandas as pd

from src.inline_domain.core.monitor.monitor_calculator import sanitize_to_compliant
from src.shared_kernel.config import ConfigLoader


def test_sanitize_to_compliant_matches_factory_product_type_and_month(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigLoader,
        "get_compliance_config",
        staticmethod(
            lambda: {
                "rules": [
                    {
                        "factory": "ALL",
                        "prod_code": "Z571",
                        "data_type": "SPC",
                        "month": "M04",
                    },
                    {
                        "factory": "OLED",
                        "prod_code": "ALL",
                        "data_type": "ALL",
                        "month": "ALL",
                    },
                ]
            }
        ),
    )
    status_df = pd.DataFrame(
        [
            {
                "sheet_id": "S1",
                "factory": "ARRAY",
                "prod_code": "Z571",
                "data_type": "SPC",
                "sheet_start_time": "2026-04-15",
                "spc_status": "OOC",
                "is_ooc": 1,
            },
            {
                "sheet_id": "S2",
                "factory": "ARRAY",
                "prod_code": "Z571",
                "data_type": "SPC",
                "sheet_start_time": "2026-05-15",
                "spc_status": "OOC",
                "is_ooc": 1,
            },
            {
                "sheet_id": "S3",
                "factory": "OLED",
                "prod_code": "M673",
                "data_type": "CTQ",
                "sheet_start_time": "2026-12-15",
                "spc_status": "OOS",
                "is_ooc": 0,
                "is_oos": 1,
            },
        ]
    )

    result = sanitize_to_compliant(status_df, add_tag=True)

    assert result["spc_status"].tolist() == ["OK", "OOC", "OK"]
    assert result["is_compliant_modified"].tolist() == [True, False, True]


def test_sanitize_to_compliant_has_no_legacy_default_or_priority(monkeypatch) -> None:
    monkeypatch.setattr(
        ConfigLoader,
        "get_compliance_config",
        staticmethod(lambda: {"rules": []}),
    )
    status_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "Z571",
                "data_type": "SPC",
                "sheet_start_time": "2026-04-15",
                "spc_status": "OOC",
                "is_ooc": 1,
            }
        ]
    )

    result = sanitize_to_compliant(status_df, add_tag=True)

    assert result["spc_status"].tolist() == ["OOC"]
    assert "is_compliant_modified" not in result.columns

