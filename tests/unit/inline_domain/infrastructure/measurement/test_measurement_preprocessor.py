from __future__ import annotations

import pandas as pd

from src.inline_domain.infrastructure.shared.measurement_preprocessor import (
    filter_excluded_param_names,
)


def test_preprocessor_excludes_loss_but_keeps_mt_ch_parameters() -> None:
    measurements = pd.DataFrame(
        {
            "param_name": ["PPA_B_X", "TOTAL_LOSS_RATE", "MT_CH_PRESS_A"],
            "param_value": [1.0, 2.0, 3.0],
        }
    )

    result = filter_excluded_param_names(measurements)

    assert result["param_name"].tolist() == ["PPA_B_X", "MT_CH_PRESS_A"]
