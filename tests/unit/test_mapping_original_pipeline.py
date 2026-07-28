import pandas as pd

from src.shared_kernel.config_model import AppConfig
from src.yield_domain.application.yield_service import YieldAnalysisService
from src.yield_domain.core.mapping import mapping_processor as mapping_module


def test_mapping_service_forwards_original_config_before_position_shift(
    monkeypatch,
) -> None:
    original_panel_id = "SHEET0000011AA0"
    panel_df = pd.DataFrame(
        [
            {
                "batch_no": "2026/06/29",
                "panel_id": original_panel_id,
                "defect_desc": "CodeOriginal",
            }
        ]
    )
    config = AppConfig.model_validate(
        {
            "application": {"cache_ttl_hours": 4},
            "data_source": {"product_code": "Z571"},
            "processing": {
                "mapping_hotspot_script": [
                    {
                        "enable": True,
                        "target_product": "Z571",
                        "target_code": "CodeOriginal",
                        "target_batch": "2026/06/29",
                        "mode": "original",
                    }
                ]
            },
        }
    )
    monkeypatch.setattr(
        YieldAnalysisService,
        "get_modified_panel_details",
        staticmethod(lambda *args, **kwargs: panel_df),
    )
    monkeypatch.setattr(
        mapping_module,
        "_get_deterministically_modified_panel_id",
        lambda panel_id, batch_no: "SHEET0000012ES0",
    )

    YieldAnalysisService.get_mapping_data.clear()
    try:
        result = YieldAnalysisService.get_mapping_data(
            config,
            scaling_factor=1,
            snapshot_signature="original-mode-regression",
        )
    finally:
        YieldAnalysisService.get_mapping_data.clear()

    assert result["panel_id"].tolist() == [original_panel_id]
