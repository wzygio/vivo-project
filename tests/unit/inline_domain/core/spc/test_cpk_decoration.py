from pathlib import Path

import pandas as pd

from src.inline_domain.core.spc.cpk_decoration import (
    apply_cpk_decoration,
    prepare_cpk_decoration,
)


def _capability_frame(cpk: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "period_type": "day",
                "period_label": "2026-07-20",
                "period_sort": 320,
                "period_start": "2026-07-20",
                "period_end": "2026-07-20",
                "cpk": cpk,
            }
        ]
    )


def test_cpk_decoration_defaults_to_real_cpk_until_an_admin_enables_a_row(tmp_path: Path) -> None:
    real_df = _capability_frame(0.82)
    corrected_df = _capability_frame(1.46)

    result = prepare_cpk_decoration(
        real_period_capability_df=real_df,
        corrected_period_capability_df=corrected_df,
        product_dir=tmp_path,
        persist_files=False,
    )

    assert result.decoration_df["flag"].tolist() == [False]
    assert result.period_capability_df["cpk"].tolist() == [0.82]
    assert result.period_capability_df["cpk_decorated"].tolist() == [False]

    enabled_df = result.decoration_df.assign(flag=True)
    decorated_df = apply_cpk_decoration(real_df, corrected_df, enabled_df)

    assert decorated_df["cpk"].tolist() == [1.46]
    assert decorated_df["cpk_decorated"].tolist() == [True]
